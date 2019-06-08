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

// PubSub holds publishers and subscribers
type PubSub struct {
	subs  []chan interface{}
	pub   chan interface{}
	mutex sync.Mutex
}

// process item to subscribers.
func (ps *PubSub) process() {
	for item := range ps.pub {
		ps.mutex.Lock()
		for _, sub := range ps.subs {
			go func(s chan interface{}) {
				s <- item
			}(sub)
		}
		ps.mutex.Unlock()
	}
}

// Publish message to pubsub system
func (ps *PubSub) Publish(item interface{}) {
	ps.pub <- item
}

// Subscribe - Adds a subscriber to pubsub system
func (ps *PubSub) Subscribe() chan interface{} {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ch := make(chan interface{})
	ps.subs = append(ps.subs, ch)
	return ch
}

// Unsubscribe removes current subscriber
func (ps *PubSub) Unsubscribe(ch chan interface{}) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	for i, sub := range ps.subs {
		if sub == ch {
			close(ch)
			ps.subs = append(ps.subs[:i], ps.subs[i+1:]...)
		}
	}
}

// HasSubscribers returns true if pubsub system has subscribers
func (ps *PubSub) HasSubscribers() bool {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return len(ps.subs) > 0
}

// New inits a PubSub system
func New() *PubSub {
	ps := &PubSub{}
	ps.pub = make(chan interface{})
	go ps.process()
	return ps
}
