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

package event

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/store"
)

const (
	// The maximum allowed number of concurrent Send() calls to all configured notifications targets
	maxConcurrentTargetSendCalls = 20000
)

// Target - event target interface
type Target interface {
	ID() TargetID
	IsActive() (bool, error)
	Save(Event) error
	SendFromStore(store.Key) error
	Close() error
	Store() TargetStore
}

// TargetStore is a shallow version of a target.Store
type TargetStore interface {
	Len() int
}

// TargetStats is a collection of stats for multiple targets.
type TargetStats struct {
	// CurrentSendCalls is the number of concurrent async Send calls to all targets
	CurrentSendCalls int64
	TotalEvents      int64
	EventsSkipped    int64

	TargetStats map[string]TargetStat
}

// TargetStat is the stats of a single target.
type TargetStat struct {
	ID           TargetID
	CurrentQueue int // Populated if target has a store.
}

// TargetList - holds list of targets indexed by target ID.
type TargetList struct {
	// The number of concurrent async Send calls to all targets
	currentSendCalls int64
	totalEvents      int64
	eventsSkipped    int64

	sync.RWMutex
	targets map[TargetID]Target
}

// Add - adds unique target to target list.
func (list *TargetList) Add(targets ...Target) error {
	list.Lock()
	defer list.Unlock()

	for _, target := range targets {
		if _, ok := list.targets[target.ID()]; ok {
			return fmt.Errorf("target %v already exists", target.ID())
		}
		list.targets[target.ID()] = target
	}

	return nil
}

// Exists - checks whether target by target ID exists or not.
func (list *TargetList) Exists(id TargetID) bool {
	list.RLock()
	defer list.RUnlock()

	_, found := list.targets[id]
	return found
}

// TargetIDResult returns result of Remove/Send operation, sets err if
// any for the associated TargetID
type TargetIDResult struct {
	// ID where the remove or send were initiated.
	ID TargetID
	// Stores any error while removing a target or while sending an event.
	Err error
}

// Remove - closes and removes targets by given target IDs.
func (list *TargetList) Remove(targetIDSet TargetIDSet) {
	list.Lock()
	defer list.Unlock()

	for id := range targetIDSet {
		target, ok := list.targets[id]
		if ok {
			target.Close()
			delete(list.targets, id)
		}
	}
}

// Targets - list all targets
func (list *TargetList) Targets() []Target {
	if list == nil {
		return []Target{}
	}

	list.RLock()
	defer list.RUnlock()

	targets := []Target{}
	for _, tgt := range list.targets {
		targets = append(targets, tgt)
	}

	return targets
}

// List - returns available target IDs.
func (list *TargetList) List() []TargetID {
	list.RLock()
	defer list.RUnlock()

	keys := []TargetID{}
	for k := range list.targets {
		keys = append(keys, k)
	}

	return keys
}

// TargetMap - returns available targets.
func (list *TargetList) TargetMap() map[TargetID]Target {
	list.RLock()
	defer list.RUnlock()

	ntargets := make(map[TargetID]Target, len(list.targets))
	for k, v := range list.targets {
		ntargets[k] = v
	}
	return ntargets
}

// Send - sends events to targets identified by target IDs.
func (list *TargetList) Send(event Event, targetIDset TargetIDSet, resCh chan<- TargetIDResult, synchronous bool) {
	if atomic.LoadInt64(&list.currentSendCalls) > maxConcurrentTargetSendCalls {
		atomic.AddInt64(&list.eventsSkipped, 1)
		err := fmt.Errorf("concurrent target notifications exceeded %d", maxConcurrentTargetSendCalls)
		for id := range targetIDset {
			resCh <- TargetIDResult{ID: id, Err: err}
		}
		return
	}
	if synchronous {
		list.send(event, targetIDset, resCh)
		return
	}
	go list.send(event, targetIDset, resCh)
}

func (list *TargetList) send(event Event, targetIDset TargetIDSet, resCh chan<- TargetIDResult) {
	var wg sync.WaitGroup
	for id := range targetIDset {
		list.RLock()
		target, ok := list.targets[id]
		list.RUnlock()
		if ok {
			wg.Add(1)
			go func(id TargetID, target Target) {
				atomic.AddInt64(&list.currentSendCalls, 1)
				defer atomic.AddInt64(&list.currentSendCalls, -1)
				defer wg.Done()
				tgtRes := TargetIDResult{ID: id}
				if err := target.Save(event); err != nil {
					tgtRes.Err = err
				}
				resCh <- tgtRes
			}(id, target)
		} else {
			resCh <- TargetIDResult{ID: id}
		}
	}
	wg.Wait()
	atomic.AddInt64(&list.totalEvents, 1)
}

// Stats returns stats for targets.
func (list *TargetList) Stats() TargetStats {
	t := TargetStats{}
	if list == nil {
		return t
	}
	t.CurrentSendCalls = atomic.LoadInt64(&list.currentSendCalls)
	t.EventsSkipped = atomic.LoadInt64(&list.eventsSkipped)
	t.TotalEvents = atomic.LoadInt64(&list.totalEvents)

	list.RLock()
	defer list.RUnlock()
	t.TargetStats = make(map[string]TargetStat, len(list.targets))
	for id, target := range list.targets {
		ts := TargetStat{ID: id}
		if st := target.Store(); st != nil {
			ts.CurrentQueue = st.Len()
		}
		t.TargetStats[strings.ReplaceAll(id.String(), ":", "_")] = ts
	}
	return t
}

// NewTargetList - creates TargetList.
func NewTargetList() *TargetList {
	return &TargetList{targets: make(map[TargetID]Target)}
}
