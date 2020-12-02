/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package logger

import (
	"sync"

	"github.com/minio/minio/pkg/madmin"
)

// Target is the entity that we will receive
// a single log entry and Send it to the log target
//   e.g. Send the log to a http server
type Target interface {
	String() string
	Endpoint() string
	Validate() error
	Send(entry interface{}, errKind string) error
}

// TargetList - holds list of targets indexed by target ID.
type TargetList struct {
	sync.RWMutex
	targets map[string]Target
}

// Available returns true if atleast one of the targets is configured
func (list *TargetList) Available() bool {
	list.RLock()
	defer list.RUnlock()

	return len(list.targets) > 0
}

// OnlineTargets - lists all available targets
func (list *TargetList) OnlineTargets(checkConn func(ep string) error) []madmin.TargetStatus {
	list.RLock()
	defer list.RUnlock()

	tgts := make([]madmin.TargetStatus, 0, len(list.targets))
	for k, t := range list.targets {
		if t.Endpoint() != "" {
			if err := checkConn(t.Endpoint()); err == nil {
				tgts = append(tgts, madmin.TargetStatus{
					k: madmin.Status{Status: "online"},
				})
			} else {
				tgts = append(tgts, madmin.TargetStatus{
					k: madmin.Status{Status: "offline"},
				})
			}
		}
	}

	return tgts
}

// Add - adds unique target to target list.
func (list *TargetList) Add(targets ...Target) error {
	list.Lock()
	defer list.Unlock()

	for _, t := range targets {
		if _, ok := list.targets[t.String()+t.Endpoint()]; !ok {
			if err := t.Validate(); err != nil {
				return err
			}
			list.targets[t.String()+t.Endpoint()] = t
		}
	}

	return nil
}

// Send - sends a message of errKind to all the targets
func (list *TargetList) Send(entry interface{}, errKind string) {
	list.RLock()
	defer list.RUnlock()

	for _, t := range list.targets {
		_ = t.Send(entry, errKind)
	}
}

// Targets is the set of enabled loggers
var Targets = TargetList{targets: make(map[string]Target)}

// AuditTargets is the list of enabled audit loggers
var AuditTargets = TargetList{targets: make(map[string]Target)}
