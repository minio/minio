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
	"fmt"
	"sync"
	"sync/atomic"
)

// Sub - subscriber entity.
type Sub struct {
	ch     chan Maskable
	types  Mask
	filter func(entry Maskable) bool
}

// PubSub holds publishers and subscribers
type PubSub struct {
	// atomics, keep at top:
	types          uint64
	numSubscribers int32
	maxSubscribers int32

	// not atomics:
	subs []*Sub
	sync.RWMutex
}

// Publish message to the subscribers.
// Note that publish is always nob-blocking send so that we don't block on slow receivers.
// Hence receivers should use buffered channel so as not to miss the published events.
func (ps *PubSub) Publish(item Maskable) {
	ps.RLock()
	defer ps.RUnlock()
	for _, sub := range ps.subs {
		if sub.types.Contains(Mask(item.Mask())) && (sub.filter == nil || sub.filter(item)) {
			select {
			case sub.ch <- item:
			default:
			}
		}
	}
}

// Subscribe - Adds a subscriber to pubsub system
func (ps *PubSub) Subscribe(mask Mask, subCh chan Maskable, doneCh <-chan struct{}, filter func(entry Maskable) bool) error {
	totalSubs := atomic.AddInt32(&ps.numSubscribers, 1)
	if ps.maxSubscribers > 0 && totalSubs > ps.maxSubscribers {
		atomic.AddInt32(&ps.numSubscribers, -1)
		return fmt.Errorf("the limit of `%d` subscribers is reached", ps.maxSubscribers)
	}
	ps.Lock()
	defer ps.Unlock()

	sub := &Sub{ch: subCh, types: mask, filter: filter}
	ps.subs = append(ps.subs, sub)

	// We hold a lock, so we are safe to update
	combined := Mask(atomic.LoadUint64(&ps.types))
	combined.Merge(mask)
	atomic.StoreUint64(&ps.types, uint64(combined))

	go func() {
		<-doneCh

		ps.Lock()
		defer ps.Unlock()
		var remainTypes Mask
		for i, s := range ps.subs {
			if s == sub {
				ps.subs = append(ps.subs[:i], ps.subs[i+1:]...)
			} else {
				remainTypes.Merge(s.types)
			}
		}
		atomic.StoreUint64(&ps.types, uint64(remainTypes))
		atomic.AddInt32(&ps.numSubscribers, -1)
	}()

	return nil
}

// NumSubscribers returns the number of current subscribers,
// If t is non-nil, the type is checked against the active subscribed types,
// and 0 will be returned if nobody is subscribed for the type,
// otherwise the *total* number of subscribers is returned.
func (ps *PubSub) NumSubscribers(m Maskable) int32 {
	if m != nil {
		types := Mask(atomic.LoadUint64(&ps.types))
		if !types.Overlaps(Mask(m.Mask())) {
			return 0
		}
	}
	return atomic.LoadInt32(&ps.numSubscribers)
}

// New inits a PubSub system with a limit of maximum
// subscribers unless zero is specified
func New(maxSubscribers int32) *PubSub {
	return &PubSub{maxSubscribers: maxSubscribers}
}
