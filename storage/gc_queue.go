// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// gcQueueMaxSize is the max size of the gc queue.
	gcQueueMaxSize = 100
	// gcQueueTimerDuration is the duration between GCs of queued replicas.
	gcQueueTimerDuration = 1 * time.Second
	// gcByteCountNormalization is the count of GC'able bytes which
	// amount to a score of "1" added to total replica priority.
	gcByteCountNormalization = 1 << 20 // 1 MB
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total replica priority.
	intentAgeNormalization = 24 * time.Hour // 1 day
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 2 * time.Hour // 2 hour
	// txnCleanupThreshold is the threshold after which a transaction is
	// considered abandoned and fit for removal, as measured by the maximum
	// of its last heartbeat and timestamp.
	// TODO(tschottdorf): need to enforce at all times that this is much
	// larger than the heartbeat interval used by the coordinator.
	txnCleanupThreshold = time.Hour
)

// gcQueue manages a queue of replicas slated to be scanned in their
// entirety using the MVCC versions iterator. The gc queue manages the
// following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents and determine oldest non-resolvable
//    intent.
//  - GC of old transaction entries. This should include most committed
//    entries almost immediately and, after a threshold on inactivity,
//    all others.
//
// The shouldQueue function combines the need for both tasks into a
// single priority. If any task is overdue, shouldQueue returns true.
type gcQueue struct {
	*baseQueue
}

// newGCQueue returns a new instance of gcQueue.
func newGCQueue() *gcQueue {
	gcq := &gcQueue{}
	gcq.baseQueue = newBaseQueue("gc", gcq, gcQueueMaxSize)
	return gcq
}

func (gcq *gcQueue) needsLeaderLease() bool {
	return true
}

// shouldQueue determines whether a replica should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (gcq *gcQueue) shouldQueue(now proto.Timestamp, repl *Replica) (shouldQ bool, priority float64) {
	// Lookup GC policy for this replica.
	policy, err := gcq.lookupGCPolicy(repl)
	if err != nil {
		log.Errorf("GC policy: %s", err)
		return
	}

	// GC score is the total GC'able bytes age normalized by 1 MB * the replica's TTL in seconds.
	gcScore := float64(repl.stats.GetGCBytesAge(now.WallTime)) / float64(policy.TTLSeconds) / float64(gcByteCountNormalization)

	// Intent score. This computes the average age of outstanding intents
	// and normalizes.
	intentScore := repl.stats.GetAvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

	// Compute priority.
	if gcScore > 1 {
		priority += gcScore
	}
	if intentScore > 1 {
		priority += intentScore
	}
	shouldQ = priority > 0
	return
}

// process iterates through all keys in a replica's range, calling the garbage
// collector for each key and associated set of values. GC'd keys are batched
// into GC calls. Extant intents are resolved if intents are older than
// intentAgeThreshold.
func (gcq *gcQueue) process(now proto.Timestamp, repl *Replica) error {
	snap := repl.rm.Engine().NewSnapshot()
	iter := newRangeDataIterator(repl.Desc(), snap)
	defer iter.Close()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	policy, err := gcq.lookupGCPolicy(repl)
	if err != nil {
		return err
	}

	gcMeta := proto.NewGCMetadata(now.WallTime)
	gc := engine.NewGarbageCollector(now, policy)

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()
	txnExp := now
	txnExp.WallTime -= txnCleanupThreshold.Nanoseconds()

	gcArgs := &proto.GCRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp: now,
			RangeID:   repl.Desc().RangeID,
		},
	}

	var mu sync.Mutex
	var oldestIntentNanos int64 = math.MaxInt64
	var expBaseKey proto.Key
	var keys []proto.EncodedKey
	var vals [][]byte

	// Maps from txn ID to txn and intent key slice.
	txnMap := map[string]*proto.Transaction{}
	intentMap := map[string][]proto.Intent{}

	// updateOldestIntent atomically updates the oldest intent.
	updateOldestIntent := func(intentNanos int64) {
		mu.Lock()
		defer mu.Unlock()
		if intentNanos < oldestIntentNanos {
			oldestIntentNanos = intentNanos
		}
	}

	// processKeysAndValues is invoked with each key and its set of
	// values. Intents older than the intent age threshold are sent for
	// resolution and values after the MVCC metadata, and possible
	// intent, are sent for garbage collection.
	processKeysAndValues := func() {
		// If there's more than a single value for the key, possibly send for GC.
		if len(keys) > 1 {
			meta := &engine.MVCCMetadata{}
			if err := gogoproto.Unmarshal(vals[0], meta); err != nil {
				log.Errorf("unable to unmarshal MVCC metadata for key %q: %s", keys[0], err)
			} else {
				// In the event that there's an active intent, send for
				// intent resolution if older than the threshold.
				startIdx := 1
				if meta.Txn != nil {
					// Keep track of intent to resolve if older than the intent
					// expiration threshold.
					if meta.Timestamp.Less(intentExp) {
						id := string(meta.Txn.ID)
						txnMap[id] = meta.Txn
						intentMap[id] = append(intentMap[id], proto.Intent{Key: expBaseKey})
					} else {
						updateOldestIntent(meta.Txn.OrigTimestamp.WallTime)
					}
					// With an active intent, GC ignores MVCC metadata & intent value.
					startIdx = 2
				}
				// See if any values may be GC'd.
				if gcTS := gc.Filter(keys[startIdx:], vals[startIdx:]); !gcTS.Equal(proto.ZeroTimestamp) {
					// TODO(spencer): need to split the requests up into
					// multiple requests in the event that more than X keys
					// are added to the request.
					gcArgs.Keys = append(gcArgs.Keys, proto.GCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
				}
			}
		}
	}

	// Iterate through the keys and values of this replica's range.
	for ; iter.Valid(); iter.Next() {
		baseKey, ts, isValue := engine.MVCCDecodeKey(iter.Key())
		if !isValue {
			// Moving to the next key (& values).
			processKeysAndValues()
			expBaseKey = baseKey
			keys = []proto.EncodedKey{iter.Key()}
			vals = [][]byte{iter.Value()}
		} else {
			if !baseKey.Equal(expBaseKey) {
				log.Errorf("unexpectedly found a value for %q with ts=%s; expected key %q", baseKey, ts, expBaseKey)
				continue
			}
			keys = append(keys, iter.Key())
			vals = append(vals, iter.Value())
		}
	}
	if iter.Error() != nil {
		return iter.Error()
	}
	// Handle last collected set of keys/vals.
	processKeysAndValues()

	// Set start and end keys.
	if len(gcArgs.Keys) == 0 {
		return nil
	}
	gcArgs.Key = gcArgs.Keys[0].Key
	gcArgs.EndKey = gcArgs.Keys[len(gcArgs.Keys)-1].Key.Next()

	txnKeys, err := processTransactionTable(repl, txnMap, intentMap, txnExp)
	if err != nil {
		return err
	}

	// Append the keys only now, after having set the key range for gcArgs.
	// Those keys are all range-local.
	// TODO(tschottdorf): Probably need two requests because we need to have
	// the full key range in the header, but we can't span from range-local
	// to range-data.
	gcArgs.Keys = append(gcArgs.Keys, txnKeys...)

	// Process push transactions in parallel.
	var wg sync.WaitGroup
	for _, txn := range txnMap {
		if txn.Status != proto.PENDING {
			continue
		}
		wg.Add(1)
		go pushTxn(repl, now, txn, updateOldestIntent, &wg)
	}
	wg.Wait()

	// Resolve all intents.
	var intents []proto.Intent
	for id, txn := range txnMap {
		if txn.Status != proto.PENDING {
			for _, intent := range intentMap[id] {
				intent.Txn = *txn
				intents = append(intents, intent)
			}
		}
	}

	if len(intents) > 0 {
		repl.resolveIntents(repl.context(), intents)
	}

	// Send GC request through range.
	gcMeta.OldestIntentNanos = gogoproto.Int64(oldestIntentNanos)
	gcArgs.GCMeta = *gcMeta
	if _, err := repl.AddCmd(repl.context(), gcArgs); err != nil {
		return err
	}

	// Store current timestamp as last verification for this replica, as
	// we've just successfully scanned.
	if err := repl.SetLastVerificationTimestamp(now); err != nil {
		log.Errorf("failed to set last verification timestamp for replica %s: %s", repl, err)
	}

	return nil
}

// processTransactionTable scans the transaction table and updates txnMap with
// those transactions which are old and either PENDING or with intents
// registered. In the first case we want to push the transaction so that it is
// aborted, and in the second case we care about checking the intents and
// updating the record accordingly so that the next gc cycle can dispose of
// them. The transaction records which can be gc'ed are returned separately and
// not added to txnMap nor intentMap.
func processTransactionTable(r *Replica, txnMap map[string]*proto.Transaction, intentMap map[string][]proto.Intent, cutoff proto.Timestamp) ([]proto.GCRequest_GCKey, error) {
	snap := r.rm.Engine().NewSnapshot()
	defer snap.Close()

	var gcKeys []proto.GCRequest_GCKey
	handleOne := func(kv proto.KeyValue) error {
		var txn proto.Transaction
		if err := gogoproto.Unmarshal(kv.Value.Bytes, &txn); err != nil {
			return err
		}
		ts := txn.Timestamp
		if heartbeatTS := txn.LastHeartbeat; heartbeatTS != nil {
			ts.Forward(*heartbeatTS)
		}
		if !ts.Less(cutoff) {
			return nil
		}

		id := string(txn.ID)
		// Always collect all intents referred to by stale transactions. In
		// practice, there should barely be any intents here, and (currently)
		// they only occur on committed transactions.
		if len(txn.Intents) > 0 {
			intentMap[id] = append(intentMap[id], txn.Intents...)
			txnMap[id] = &txn
		}

		// The transaction record should be considered for removal.
		switch txn.Status {
		case proto.PENDING:
			// Marked as running, so we need to push it to abort it.
			txnMap[id] = &txn
			return nil
		case proto.COMMITTED:
			// It's committed, so it doesn't need a push but it might be
			// waiting for some intents to be cleaned up.
			if len(txn.Intents) > 0 {
				// Can't GC until the intents are gone. We've already
				// collected them above, so nothing else to do.
				return nil
			}
		case proto.ABORTED:
			// It's aborted and old, so once removed a push to it will
			// still succeed (by design). So this is safe to remove.
		default:
			panic(fmt.Sprintf("invalid transaction state: %s", txn))
		}
		gcKeys = append(gcKeys, proto.GCRequest_GCKey{Key: kv.Key}) // zero timestamp
		return nil
	}

	startKey := keys.TransactionKey(proto.KeyMin, nil)
	endKey := keys.TransactionKey(proto.KeyMax, nil)

	_, err := engine.MVCCIterate(snap, startKey, endKey, proto.ZeroTimestamp, true /* consistent */, nil /* txn */, false /* !reverse */, func(kv proto.KeyValue) (bool, error) {
		return false, handleOne(kv)
	})
	return gcKeys, err
}

// timer returns a constant duration to space out GC processing
// for successive queued replicas.
func (gcq *gcQueue) timer() time.Duration {
	return gcQueueTimerDuration
}

// pushTxn attempts to abort the txn via push. If the transaction
// cannot be aborted, the oldestIntentNanos value is atomically
// updated to the min of oldestIntentNanos and the intent's
// timestamp. The wait group is signaled on completion.
func pushTxn(repl *Replica, now proto.Timestamp, txn *proto.Transaction, updateOldestIntent func(int64), wg *sync.WaitGroup) {
	defer wg.Done() // signal wait group always on completion
	if log.V(1) {
		log.Infof("pushing txn %s ts=%s", txn, txn.OrigTimestamp)
	}

	// Attempt to push the transaction which created the intent.
	pushArgs := &proto.PushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp:    now,
			Key:          txn.Key,
			User:         security.RootUser,
			UserPriority: gogoproto.Int32(proto.MaxPriority),
			Txn:          nil,
		},
		Now:       now,
		PusheeTxn: *txn,
		PushType:  proto.ABORT_TXN,
	}
	pushReply := &proto.PushTxnResponse{}
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: pushArgs, Reply: pushReply})
	if err := repl.rm.DB().Run(b); err != nil {
		log.Warningf("push of txn %s failed: %s", txn, err)
		updateOldestIntent(txn.OrigTimestamp.WallTime)
		return
	}
	// Update the supplied txn on successful push.
	*txn = *pushReply.PusheeTxn
}

// lookupGCPolicy queries the gossip prefix config map based on the
// supplied replica's start key. It queries all matching config prefixes
// and then iterates from most specific to least, returning the first
// non-nil GC policy.
func (gcq *gcQueue) lookupGCPolicy(repl *Replica) (config.GCPolicy, error) {
	info, err := repl.rm.Gossip().GetInfo(gossip.KeyConfigZone)
	if err != nil {
		return config.GCPolicy{}, util.Errorf("unable to fetch zone config from gossip: %s", err)
	}
	configMap, ok := info.(config.PrefixConfigMap)
	if !ok {
		return config.GCPolicy{}, util.Errorf("gossiped info is not a prefix configuration map: %+v", info)
	}

	// Verify that the replica's range doesn't cross over the zone config
	// prefix.  This could be the case if the zone config is new and the range
	// hasn't been split yet along the new boundary.
	var gc *config.GCPolicy
	if err = configMap.VisitPrefixesHierarchically(repl.Desc().StartKey, func(start, end proto.Key, cfg gogoproto.Message) (bool, error) {
		zone := cfg.(*config.ZoneConfig)
		if zone.GC != nil {
			repl.RLock()
			isCovered := !end.Less(repl.Desc().EndKey)
			repl.RUnlock()
			if !isCovered {
				return false, util.Errorf("replica is only partially covered by zone %s (%q-%q); must wait for range split", cfg, start, end)
			}
			gc = zone.GC
			return true, nil
		}
		if log.V(1) {
			log.Infof("skipping zone config %+v, because no GC policy is set", zone)
		}
		return false, nil
	}); err != nil {
		return config.GCPolicy{}, err
	}

	// We should always match _at least_ the default GC.
	if gc == nil {
		return config.GCPolicy{}, util.Errorf("no zone for range with start key %q", repl.Desc().StartKey)
	}
	return *gc, nil
}
