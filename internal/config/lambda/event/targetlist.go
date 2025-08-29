// Copyright (c) 2015-2023 MinIO, Inc.
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
	"maps"
	"net/http"
	"strings"
	"sync"
)

// Target - lambda target interface
type Target interface {
	ID() TargetID
	IsActive() (bool, error)
	Send(Event) (*http.Response, error)
	Stat() TargetStat
	Close() error
}

// TargetStats is a collection of stats for multiple targets.
type TargetStats struct {
	TargetStats map[string]TargetStat
}

// TargetStat is the stats of a single target.
type TargetStat struct {
	ID             TargetID
	ActiveRequests int64
	TotalRequests  int64
	FailedRequests int64
}

// TargetList - holds list of targets indexed by target ID.
type TargetList struct {
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

// Lookup - checks whether target by target ID exists is valid or not.
func (list *TargetList) Lookup(arnStr string) (Target, error) {
	list.RLock()
	defer list.RUnlock()

	arn, err := ParseARN(arnStr)
	if err != nil {
		return nil, err
	}

	id, found := list.targets[arn.TargetID]
	if !found {
		return nil, &ErrARNNotFound{}
	}
	return id, nil
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

	targets := make([]Target, 0, len(list.targets))
	for _, tgt := range list.targets {
		targets = append(targets, tgt)
	}

	return targets
}

// Empty returns true if targetList is empty.
func (list *TargetList) Empty() bool {
	list.RLock()
	defer list.RUnlock()

	return len(list.targets) == 0
}

// List - returns available target IDs.
func (list *TargetList) List(region string) []ARN {
	list.RLock()
	defer list.RUnlock()

	keys := make([]ARN, 0, len(list.targets))
	for k := range list.targets {
		keys = append(keys, k.ToARN(region))
	}

	return keys
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
func (list *TargetList) Send(event Event, id TargetID) (*http.Response, error) {
	list.RLock()
	target, ok := list.targets[id]
	list.RUnlock()
	if ok {
		return target.Send(event)
	}
	return nil, ErrARNNotFound{}
}

// Stats returns stats for targets.
func (list *TargetList) Stats() TargetStats {
	t := TargetStats{}
	if list == nil {
		return t
	}
	list.RLock()
	defer list.RUnlock()
	t.TargetStats = make(map[string]TargetStat, len(list.targets))
	for id, target := range list.targets {
		t.TargetStats[strings.ReplaceAll(id.String(), ":", "_")] = target.Stat()
	}
	return t
}

// NewTargetList - creates TargetList.
func NewTargetList() *TargetList {
	return &TargetList{targets: make(map[TargetID]Target)}
}
