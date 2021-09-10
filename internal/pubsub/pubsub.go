// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package pubsub

import (
	"sync"
	"sync/atomic"
)

// Sub - subscriber entity.
type Sub struct {
	ch     chan interface{}
	filter func(entry interface{}) bool
}

// PubSub holds publishers and subscribers
type PubSub struct {
	subs           []*Sub
	numSubscribers int32
	sync.RWMutex
}

// Publish message to the subscribers.
// Note that publish is always nob-blocking send so that we don't block on slow receivers.
// Hence receivers should use buffered channel so as not to miss the published events.
func (ps *PubSub) Publish(item interface{}) {
	ps.RLock()
	defer ps.RUnlock()

	for _, sub := range ps.subs {
		if sub.filter == nil || sub.filter(item) {
			select {
			case sub.ch <- item:
			default:
			}
		}
	}
}

// Subscribe - Adds a subscriber to pubsub system
func (ps *PubSub) Subscribe(subCh chan interface{}, doneCh <-chan struct{}, filter func(entry interface{}) bool) {
	ps.Lock()
	defer ps.Unlock()

	sub := &Sub{subCh, filter}
	ps.subs = append(ps.subs, sub)
	atomic.AddInt32(&ps.numSubscribers, 1)

	go func() {
		<-doneCh

		ps.Lock()
		defer ps.Unlock()

		for i, s := range ps.subs {
			if s == sub {
				ps.subs = append(ps.subs[:i], ps.subs[i+1:]...)
			}
		}
		atomic.AddInt32(&ps.numSubscribers, -1)
	}()
}

// NumSubscribers returns the number of current subscribers
func (ps *PubSub) NumSubscribers() int32 {
	return atomic.LoadInt32(&ps.numSubscribers)
}

// New inits a PubSub system
func New() *PubSub {
	return &PubSub{}
}
