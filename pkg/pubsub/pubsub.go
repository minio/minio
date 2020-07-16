/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pubsub

import (
	"sync"
)

// Sub - subscriber entity.
type Sub struct {
	ch     chan interface{}
	filter func(entry interface{}) bool
}

// PubSub holds publishers and subscribers
type PubSub struct {
	subs []*Sub
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

	go func() {
		<-doneCh

		ps.Lock()
		defer ps.Unlock()

		for i, s := range ps.subs {
			if s == sub {
				ps.subs = append(ps.subs[:i], ps.subs[i+1:]...)
			}
		}
	}()
}

// HasSubscribers returns true if pubsub system has subscribers
func (ps *PubSub) HasSubscribers() bool {
	ps.RLock()
	defer ps.RUnlock()
	return len(ps.subs) > 0
}

// New inits a PubSub system
func New() *PubSub {
	return &PubSub{}
}
