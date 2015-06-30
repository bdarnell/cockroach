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

package rpc

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// OrderingPolicy is an enum for ordering strategies when there
// are multiple endpoints available.
type OrderingPolicy int

const (
	// OrderStable uses endpoints in the order provided.
	OrderStable = iota
	// OrderRandom randomly orders available endpoints.
	OrderRandom
)

// An Options structure describes the algorithm for sending RPCs to
// one or more replicas, depending on error conditions and how many
// successful responses are required.
type Options struct {
	// N is the number of successful responses required.
	N int
	// Ordering indicates how the available endpoints are ordered when
	// deciding which to send to (if there are more than one).
	Ordering OrderingPolicy
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration
	// Timeout is the maximum duration of an RPC before failure.
	// 0 for no timeout.
	Timeout time.Duration
	// If not nil, information about the request is added to this trace.
	Trace *tracer.Trace
}

// An rpcError indicates a failure to send the RPC. rpcErrors are
// retryable.
type rpcError struct {
	errMsg string
}

// Error implements the error interface.
func (r rpcError) Error() string { return r.errMsg }

// CanRetry implements the Retryable interface.
// TODO(tschottdorf): the way this is used by rpc/send suggests that it
// may be better if these weren't retriable - they are returned when the
// connection fails, i.e. for example when a node is down or the network
// fails. Retrying on such errors keeps the caller waiting for a long time
// and without a positive outlook.
func (r rpcError) CanRetry() bool { return true }

// sendOneFn is overwritten in tests to mock sendOne.
var sendOneFn = sendOne

// A SendError indicates that too many RPCs to the replica
// set failed to achieve requested number of successful responses.
// canRetry is set depending on the types of errors encountered.
type SendError struct {
	errMsg   string
	canRetry bool
}

// Error implements the error interface.
func (s SendError) Error() string {
	return "failed to send RPC: " + s.errMsg
}

// CanRetry implements the Retryable interface.
func (s SendError) CanRetry() bool { return s.canRetry }

// Send sends one or more method RPCs to clients specified by the
// slice of endpoint addrs. Arguments for methods are obtained using
// the supplied getArgs function. The number of required replies is
// given by opts.N. Reply structs are obtained through the getReply()
// function. On success, Send returns a slice of replies of length
// opts.N. Otherwise, Send returns an error if and as soon as the
// number of failed RPCs exceeds the available endpoints less the
// number of required replies.
func Send(opts Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) interface{},
	getReply func() interface{}, context *Context) ([]interface{}, error) {
	trace := opts.Trace // not thread safe!

	if opts.N <= 0 {
		return nil, SendError{
			errMsg:   fmt.Sprintf("opts.N must be positive: %d", opts.N),
			canRetry: false,
		}
	}

	if len(addrs) < opts.N {
		return nil, SendError{
			errMsg:   fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d", len(addrs), opts.N),
			canRetry: false,
		}
	}

	var clients []*Client
	switch opts.Ordering {
	case OrderStable:
		for _, addr := range addrs {
			clients = append(clients, NewClient(addr, nil, context))
		}
	case OrderRandom:
		// Randomly permute order, but keep known-unhealthy clients last.
		var healthy, unhealthy []*Client
		for _, addr := range addrs {
			client := NewClient(addr, nil, context)
			if client.IsHealthy() {
				healthy = append(healthy, client)
			} else {
				unhealthy = append(unhealthy, client)
			}
		}
		for _, idx := range rand.Perm(len(healthy)) {
			clients = append(clients, healthy[idx])
		}
		for _, idx := range rand.Perm(len(unhealthy)) {
			clients = append(clients, unhealthy[idx])
		}
	}
	// TODO(spencer): going to need to also sort by affinity; closest
	// ping time should win. Makes sense to have the rpc client/server
	// heartbeat measure ping times. With a bit of seasoning, each
	// node will be able to order the healthy replicas based on latency.

	replies := []interface{}(nil)
	helperChan := make(chan interface{}, len(clients))
	N := opts.N
	errors := 0
	retryableErrors := 0
	successes := 0
	index := 0

	// Send RPCs to replicas as necessary to achieve opts.N successes.
	for {
		// Start clients up to N.
		for ; index < N; index++ {
			args := getArgs(clients[index].Addr())
			if args == nil {
				helperChan <- util.Errorf("nil arguments returned for client %s", clients[index].Addr())
				continue
			}
			reply := getReply()
			if log.V(2) {
				log.Infof("%s: sending request to %s: %+v", method, clients[index].Addr(), args)
			}

			trace.Event(fmt.Sprintf("sending to %s", clients[index].Addr()))
			go sendOneFn(clients[index], opts.Timeout, method, args, reply, helperChan)
		}
		// Wait for completions.
		select {
		case r := <-helperChan:
			switch t := r.(type) {
			case error:
				errors++
				if retryErr, ok := t.(util.Retryable); ok && retryErr.CanRetry() {
					retryableErrors++
				}
				if log.V(1) {
					log.Warningf("%s: error reply: %+v", method, t)
				}
				remainingNonErrorRPCs := len(clients) - errors
				if remainingNonErrorRPCs < opts.N {
					return nil, SendError{
						errMsg:   fmt.Sprintf("too many errors encountered (%d of %d total): %v", errors, len(clients), t),
						canRetry: remainingNonErrorRPCs+retryableErrors >= opts.N,
					}
				}
				// Send to additional replicas if available.
				if N < len(clients) {
					trace.Event("error, trying next peer")
					N++
				}
			default:
				successes++
				if log.V(2) {
					log.Infof("%s: successful reply: %+v", method, t)
				}
				replies = append(replies, t)
				if successes == opts.N {
					return replies, nil
				}
			}
		case <-time.After(opts.SendNextTimeout):
			// On successive RPC timeouts, send to additional replicas if available.
			if N < len(clients) {
				trace.Event("timeout, trying next peer")
				N++
			}
		}
	}
}

// sendOne invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func sendOne(client *Client, timeout time.Duration, method string, args, reply interface{}, c chan interface{}) {
	if timeout == 0 {
		// Wait forever.
		timeout = math.MaxInt64
	}
	select {
	case <-client.Ready:
	case <-client.Closed:
		c <- rpcError{fmt.Sprintf("rpc to %s failed as client connection was closed", method)}
		return
	case <-time.After(timeout):
		c <- rpcError{fmt.Sprintf("rpc to %s: client not ready after %s", method, timeout)}
		return
	}
	call := client.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			// Handle cases which are retryable.
			switch call.Error {
			case rpc.ErrShutdown: // client connection fails: rpc/client.go
				fallthrough
			case io.ErrUnexpectedEOF: // server connection fails: rpc/client.go
				c <- rpcError{call.Error.Error()}
			default:
				// Otherwise, not retryable; just return error.
				c <- call.Error
			}
		} else {
			// Verify response data integrity if this is a proto response.
			if resp, respOk := reply.(proto.Response); respOk {
				if req, reqOk := args.(proto.Request); reqOk {
					if err := resp.Verify(req); err != nil {
						c <- err
						return
					}
				}
			}
			c <- reply
		}
	case <-client.Closed:
		c <- rpcError{fmt.Sprintf("rpc to %s failed as client connection was closed", method)}
	case <-time.After(timeout):
		c <- rpcError{fmt.Sprintf("rpc to %s timed out after %s", method, timeout)}
	}
}
