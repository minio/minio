/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package event

import (
	"fmt"
	"sync"
)

// Target - event target interface
type Target interface {
	ID() TargetID
	Send(Event) error
	Close() error
}

// TargetList - holds list of targets indexed by target ID.
type TargetList struct {
	sync.RWMutex
	targets map[TargetID]Target
}

// Add - adds unique target to target list.
func (list *TargetList) Add(target Target) error {
	list.Lock()
	defer list.Unlock()

	if _, ok := list.targets[target.ID()]; ok {
		return fmt.Errorf("target %v already exists", target.ID())
	}

	list.targets[target.ID()] = target
	return nil
}

// Exists - checks whether target by target ID exists or not.
func (list *TargetList) Exists(id TargetID) bool {
	list.RLock()
	defer list.RUnlock()

	_, found := list.targets[id]
	return found
}

// TargetIDErr returns error associated for a targetID
type TargetIDErr struct {
	// ID where the remove or send were initiated.
	ID TargetID
	// Stores any error while removing a target or while sending an event.
	Err error
}

// Remove - closes and removes targets by given target IDs.
func (list *TargetList) Remove(targetids ...TargetID) <-chan TargetIDErr {
	list.Lock()
	defer list.Unlock()

	errCh := make(chan TargetIDErr)

	go func() {
		defer close(errCh)

		var wg sync.WaitGroup
		for _, id := range targetids {
			if target, ok := list.targets[id]; ok {
				wg.Add(1)
				go func(id TargetID, target Target) {
					defer wg.Done()
					if err := target.Close(); err != nil {
						errCh <- TargetIDErr{
							ID:  id,
							Err: err,
						}
					}
				}(id, target)
			}
		}
		wg.Wait()

		for _, id := range targetids {
			delete(list.targets, id)
		}
	}()

	return errCh
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

// Send - sends events to targets identified by target IDs.
func (list *TargetList) Send(event Event, targetIDs ...TargetID) <-chan TargetIDErr {
	list.Lock()
	defer list.Unlock()

	errCh := make(chan TargetIDErr)

	go func() {
		defer close(errCh)

		var wg sync.WaitGroup
		for _, id := range targetIDs {
			if target, ok := list.targets[id]; ok {
				wg.Add(1)
				go func(id TargetID, target Target) {
					defer wg.Done()
					if err := target.Send(event); err != nil {
						errCh <- TargetIDErr{
							ID:  id,
							Err: err,
						}
					}
				}(id, target)
			}
		}
		wg.Wait()
	}()

	return errCh
}

// NewTargetList - creates TargetList.
func NewTargetList() *TargetList {
	return &TargetList{targets: make(map[TargetID]Target)}
}
