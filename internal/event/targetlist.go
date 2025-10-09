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
	"context"
	"fmt"
	"maps"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/store"
	"github.com/minio/pkg/v3/workers"
)

const (
	logSubsys = "notify"

	// The maximum allowed number of concurrent Send() calls to all configured notifications targets
	maxConcurrentAsyncSend = 50000
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

// Stats is a collection of stats for multiple targets.
type Stats struct {
	TotalEvents        int64 // Deprecated
	EventsSkipped      int64
	CurrentQueuedCalls int64 // Deprecated
	EventsErrorsTotal  int64 // Deprecated
	CurrentSendCalls   int64 // Deprecated

	TargetStats map[TargetID]TargetStat
}

// TargetStat is the stats of a single target.
type TargetStat struct {
	CurrentSendCalls int64 // CurrentSendCalls is the number of concurrent async Send calls to all targets
	CurrentQueue     int   // Populated if target has a store.
	TotalEvents      int64
	FailedEvents     int64 // Number of failed events per target
}

// TargetList - holds list of targets indexed by target ID.
type TargetList struct {
	// The number of concurrent async Send calls to all targets
	currentSendCalls  atomic.Int64
	totalEvents       atomic.Int64
	eventsSkipped     atomic.Int64
	eventsErrorsTotal atomic.Int64

	sync.RWMutex
	targets map[TargetID]Target
	queue   chan asyncEvent
	ctx     context.Context

	statLock    sync.RWMutex
	targetStats map[TargetID]targetStat
}

type targetStat struct {
	// The number of concurrent async Send calls per targets
	currentSendCalls int64
	// The number of total events per target
	totalEvents int64
	// The number of failed events per target
	failedEvents int64
}

func (list *TargetList) getStatsByTargetID(id TargetID) (stat targetStat) {
	list.statLock.RLock()
	defer list.statLock.RUnlock()

	return list.targetStats[id]
}

func (list *TargetList) incCurrentSendCalls(id TargetID) {
	list.statLock.Lock()
	defer list.statLock.Unlock()

	stats, ok := list.targetStats[id]
	if !ok {
		stats = targetStat{}
	}

	stats.currentSendCalls++
	list.targetStats[id] = stats
}

func (list *TargetList) decCurrentSendCalls(id TargetID) {
	list.statLock.Lock()
	defer list.statLock.Unlock()

	stats, ok := list.targetStats[id]
	if !ok {
		// should not happen
		return
	}

	stats.currentSendCalls--
	list.targetStats[id] = stats
}

func (list *TargetList) incFailedEvents(id TargetID) {
	list.statLock.Lock()
	defer list.statLock.Unlock()

	stats, ok := list.targetStats[id]
	if !ok {
		stats = targetStat{}
	}

	stats.failedEvents++
	list.targetStats[id] = stats
}

func (list *TargetList) incTotalEvents(id TargetID) {
	list.statLock.Lock()
	defer list.statLock.Unlock()

	stats, ok := list.targetStats[id]
	if !ok {
		stats = targetStat{}
	}

	stats.totalEvents++
	list.targetStats[id] = stats
}

type asyncEvent struct {
	ev        Event
	targetSet TargetIDSet
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

func (list *TargetList) get(id TargetID) (Target, bool) {
	list.RLock()
	defer list.RUnlock()

	target, ok := list.targets[id]
	return target, ok
}

// TargetMap - returns available targets.
func (list *TargetList) TargetMap() map[TargetID]Target {
	list.RLock()
	defer list.RUnlock()

	ntargets := make(map[TargetID]Target, len(list.targets))
	maps.Copy(ntargets, list.targets)
	return ntargets
}

// Send - sends events to targets identified by target IDs.
func (list *TargetList) Send(event Event, targetIDset TargetIDSet, sync bool) {
	if sync {
		list.sendSync(event, targetIDset)
	} else {
		list.sendAsync(event, targetIDset)
	}
}

func (list *TargetList) sendSync(event Event, targetIDset TargetIDSet) {
	var wg sync.WaitGroup
	for id := range targetIDset {
		target, ok := list.get(id)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(id TargetID, target Target) {
			list.currentSendCalls.Add(1)
			list.incCurrentSendCalls(id)
			list.incTotalEvents(id)
			defer list.decCurrentSendCalls(id)
			defer list.currentSendCalls.Add(-1)
			defer wg.Done()

			if err := target.Save(event); err != nil {
				list.eventsErrorsTotal.Add(1)
				list.incFailedEvents(id)
				reqInfo := &logger.ReqInfo{}
				reqInfo.AppendTags("targetID", id.String())
				logger.LogOnceIf(logger.SetReqInfo(context.Background(), reqInfo), logSubsys, err, id.String())
			}
		}(id, target)
	}
	wg.Wait()
	list.totalEvents.Add(1)
}

func (list *TargetList) sendAsync(event Event, targetIDset TargetIDSet) {
	select {
	case list.queue <- asyncEvent{
		ev:        event,
		targetSet: targetIDset.Clone(),
	}:
	case <-list.ctx.Done():
		list.eventsSkipped.Add(int64(len(list.queue)))
		return
	default:
		list.eventsSkipped.Add(1)
		err := fmt.Errorf("concurrent target notifications exceeded %d, configured notification target is too slow to accept events for the incoming request rate", maxConcurrentAsyncSend)
		for id := range targetIDset {
			reqInfo := &logger.ReqInfo{}
			reqInfo.AppendTags("targetID", id.String())
			logger.LogOnceIf(logger.SetReqInfo(context.Background(), reqInfo), logSubsys, err, id.String())
		}
		return
	}
}

// Stats returns stats for targets.
func (list *TargetList) Stats() Stats {
	t := Stats{}
	if list == nil {
		return t
	}
	t.CurrentSendCalls = list.currentSendCalls.Load()
	t.EventsSkipped = list.eventsSkipped.Load()
	t.TotalEvents = list.totalEvents.Load()
	t.CurrentQueuedCalls = int64(len(list.queue))
	t.EventsErrorsTotal = list.eventsErrorsTotal.Load()

	list.RLock()
	defer list.RUnlock()
	t.TargetStats = make(map[TargetID]TargetStat, len(list.targets))
	for id, target := range list.targets {
		var currentQueue int
		if st := target.Store(); st != nil {
			currentQueue = st.Len()
		}
		stats := list.getStatsByTargetID(id)
		t.TargetStats[id] = TargetStat{
			CurrentSendCalls: stats.currentSendCalls,
			CurrentQueue:     currentQueue,
			FailedEvents:     stats.failedEvents,
			TotalEvents:      stats.totalEvents,
		}
	}

	return t
}

func (list *TargetList) startSendWorkers(workerCount int) {
	if workerCount == 0 {
		workerCount = runtime.GOMAXPROCS(0)
	}
	wk, err := workers.New(workerCount)
	if err != nil {
		panic(err)
	}
	for range workerCount {
		wk.Take()
		go func() {
			defer wk.Give()

			for {
				select {
				case av := <-list.queue:
					list.sendSync(av.ev, av.targetSet)
				case <-list.ctx.Done():
					return
				}
			}
		}()
	}
	wk.Wait()
}

var startOnce sync.Once

// Init initialize target send workers.
func (list *TargetList) Init(workers int) *TargetList {
	startOnce.Do(func() {
		go list.startSendWorkers(workers)
	})
	return list
}

// NewTargetList - creates TargetList.
func NewTargetList(ctx context.Context) *TargetList {
	list := &TargetList{
		targets:     make(map[TargetID]Target),
		queue:       make(chan asyncEvent, maxConcurrentAsyncSend),
		targetStats: make(map[TargetID]targetStat),
		ctx:         ctx,
	}
	return list
}
