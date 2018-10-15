// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// This file contains regression tests for consistency bugs, perhaps
// reduced from jepsen tests.

func registerConsistencySplit(r *registry) {
	r.Add(testSpec{
		Name:   `consistency/split`,
		Nodes:  nodes(6),
		Stable: false, // TODO(bdarnell): stabilize
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")

			c.Start(ctx, c.All())
			db := c.Conn(ctx, 1)
			defer db.Close()

			const key = 10000000
			const numWrites = 10000

			if _, err := db.Exec("CREATE DATABASE d"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE d.t (k INT PRIMARY KEY, v INT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO d.t VALUES ($1, 0)", key); err != nil {
				t.Fatal(err)
			}

			const (
				writeNotStarted = iota
				writePending
				writeSuccess
				writeFailed
				writeAmbiguous
			)
			writeStatus := make([]int, numWrites)
			var mu syncutil.Mutex

			taskCtx, taskCancel := context.WithCancel(ctx)
			defer func() {
				if taskCancel != nil {
					taskCancel()
				}
			}()

			// Splitter goroutine
			numSplits := 0
			splitErrCh := make(chan error, 1)
			go func() {
				for i := 0; i < key; i++ {
					_, err := db.ExecContext(taskCtx, "ALTER TABLE d.t SPLIT AT VALUES ($1)", i)
					if err != nil {
						splitErrCh <- err
						return
					}
					numSplits++

					if taskCtx.Err() != nil {
						break
					}
				}
				splitErrCh <- nil
			}()

			// Reader goroutine
			numReads := 0
			readErrCh := make(chan error, 1)
			go func() {
				lastRead := 0
				for {
					row := db.QueryRowContext(taskCtx, "SELECT v FROM d.t WHERE k = $1", key)
					var i int
					err := row.Scan(&i)
					if err != nil {
						readErrCh <- err
						return
					}

					if i < lastRead {
						// readErrCh <- errors.Errorf("read value regressed from %d to %d", lastRead, i)
						// return
					}

					mu.Lock()
					switch writeStatus[i] {
					case writeNotStarted:
						readErrCh <- errors.Errorf("read value %d which was never written", i)
						return
					case writeFailed:
						readErrCh <- errors.Errorf("read value %d which failed", i)
						return
					case writePending, writeAmbiguous:
						// We now know that this write succeeded, so mark it as such.
						// TODO can we do something useful with this?
						writeStatus[i] = writeSuccess
					case writeSuccess:
					// The normal successful case
					default:
						panic("unknown status")
					}
					mu.Unlock()

					numReads++
					lastRead = i

					if taskCtx.Err() != nil {
						break
					}
				}
				readErrCh <- nil
			}()

			const numWriters = 2
			writeQueue := make(chan int, numWriters)
			writeErrCh := make(chan error, numWriters)
			writeAttempts := 0
			for i := 0; i < numWriters; i++ {
				go func() {
					for i := range writeQueue {
						mu.Lock()
						writeStatus[i] = writePending
						mu.Unlock()

						// err := crdb.ExecuteTx(taskCtx, db, nil, func(tx *sql.Tx) error {
						// 	writeAttempts++
						// 	rows, err := tx.QueryContext(taskCtx, "SELECT * FROM d.t WHERE k = $1", key)
						// 	if err != nil {
						// 		return err
						// 	}
						// 	err = rows.Close()
						// 	if err != nil {
						// 		return err
						// 	}
						// 	_, err = tx.ExecContext(taskCtx, "UPDATE d.t SET v = $1 WHERE k = $2", i, key)
						// 	return err
						// })

						_, err := db.ExecContext(taskCtx, "UPDATE d.t SET v = $1 WHERE k = $2", i, key)

						// tx, err := db.BeginTx(taskCtx, nil)
						// if err == nil {
						// 	_, err := tx.ExecContext(taskCtx, "update d.t set v =$1 where k=$2", i, key)
						// 	if err == nil {
						// 		err = tx.Commit()
						// 	} else {
						// 		_ = tx.Rollback()
						// 	}
						// }

						mu.Lock()
						if err == nil {
							writeStatus[i] = writeSuccess
						} else {
							if writeStatus[i] == writeSuccess {
								writeErrCh <- errors.Errorf("got unambigous failure writing %d after value was read", i)
								mu.Unlock()
								return
							}
							c.l.Printf("write failed: %s\n", err)
							writeStatus[i] = writeFailed
						}
						mu.Unlock()

						if taskCtx.Err() != nil {
							break
						}
					}
					writeErrCh <- nil
				}()
			}

			for i := 0; i < numWrites; i++ {
				if i%100 == 0 {
					t.Status("writing ", i)
				}
				select {
				case writeQueue <- i:
				case err := <-writeErrCh:
					writeErrCh <- err
					break
				}
			}
			close(writeQueue)

			var errMsg string
			t.Status("waiting for write goroutines")
			for i := 0; i < numWriters; i++ {
				if err := <-writeErrCh; err != nil {
					errMsg += fmt.Sprintf("write failed: %s\n", err)
				}
			}

			taskCancel()
			taskCancel = nil

			t.Status("waiting for split goroutine")
			if err := <-splitErrCh; err != nil {
				errMsg += fmt.Sprintf("split failed: %s\n", err)
			}
			t.Status("waiting for read goroutine")
			if err := <-readErrCh; err != nil {
				errMsg += fmt.Sprintf("read failed: %s\n", err)
			}

			c.l.Printf("performed %d writes, %d reads, and %d splits\n", numWrites, numReads, numSplits)

			counts := map[int]int{}
			for _, v := range writeStatus {
				counts[v]++
			}
			c.l.Printf("write status: %d success, %d failure, %d ambiguous, %d pending, %d not started, %d attempts\n",
				counts[writeSuccess], counts[writeFailed], counts[writeAmbiguous], counts[writePending],
				counts[writeNotStarted], writeAttempts)
			t.Status("shutting down")

			if errMsg != "" {
				t.Fatal(errMsg)
			}
		},
	})
}
