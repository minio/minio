// Copyright (c) 2015-2022 MinIO, Inc.
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

package logger

import (
	"sync"

	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	"github.com/minio/minio/internal/logger/target/types"
)

// Target is the entity that we will receive
// a single log entry and Send it to the log target
//   e.g. Send the log to a http server
type Target interface {
	String() string
	Endpoint() string
	Init() error
	Cancel()
	Send(entry interface{}, errKind string) error
	Type() types.TargetType
}

var (
	swapAuditMuRW  sync.RWMutex
	swapSystemMuRW sync.RWMutex

	// systemTargets is the set of enabled loggers.
	// Must be immutable at all times.
	// Can be swapped to another while holding swapMu
	systemTargets = []Target{}

	// This is always set represent /dev/console target
	consoleTgt Target
)

// SystemTargets returns active targets.
// Returned slice may not be modified in any way.
func SystemTargets() []Target {
	swapSystemMuRW.RLock()
	defer swapSystemMuRW.RUnlock()

	res := systemTargets
	return res
}

// AuditTargets returns active audit targets.
// Returned slice may not be modified in any way.
func AuditTargets() []Target {
	swapAuditMuRW.RLock()
	defer swapAuditMuRW.RUnlock()

	res := auditTargets
	return res
}

// auditTargets is the list of enabled audit loggers
// Must be immutable at all times.
// Can be swapped to another while holding swapMu
var (
	auditTargets = []Target{}
)

// AddSystemTarget adds a new logger target to the
// list of enabled loggers
func AddSystemTarget(t Target) error {
	if err := t.Init(); err != nil {
		return err
	}

	swapSystemMuRW.Lock()
	defer swapSystemMuRW.Unlock()

	if consoleTgt == nil {
		if t.Type() == types.TargetConsole {
			consoleTgt = t
		}
	}
	updated := append(make([]Target, 0, len(systemTargets)+1), systemTargets...)
	updated = append(updated, t)
	systemTargets = updated

	return nil
}

func cancelAllSystemTargets() {
	for _, tgt := range systemTargets {
		tgt.Cancel()
	}
}

func initSystemTargets(cfgMap map[string]http.Config) (tgts []Target, err error) {
	for _, l := range cfgMap {
		if l.Enabled {
			t := http.New(l)
			if err = t.Init(); err != nil {
				return tgts, err
			}
			tgts = append(tgts, t)
		}
	}
	return tgts, err
}

func initKafkaTargets(cfgMap map[string]kafka.Config) (tgts []Target, err error) {
	for _, l := range cfgMap {
		if l.Enabled {
			t := kafka.New(l)
			if err = t.Init(); err != nil {
				return tgts, err
			}
			tgts = append(tgts, t)
		}
	}
	return tgts, err
}

// UpdateSystemTargets swaps targets with newly loaded ones from the cfg
func UpdateSystemTargets(cfg Config) error {
	newTgts, err := initSystemTargets(cfg.HTTP)
	if err != nil {
		return err
	}

	swapSystemMuRW.Lock()
	defer swapSystemMuRW.Unlock()

	for _, tgt := range systemTargets {
		// Preserve console target when dynamically updating
		// other HTTP targets, console target is always present.
		if tgt.Type() == types.TargetConsole {
			newTgts = append(newTgts, tgt)
			break
		}
	}

	cancelAllSystemTargets() // cancel running targets
	systemTargets = newTgts

	return nil
}

func cancelAuditTargetType(t types.TargetType) {
	for _, tgt := range auditTargets {
		if tgt.Type() == t {
			tgt.Cancel()
		}
	}
}

func existingAuditTargets(t types.TargetType) []Target {
	tgts := make([]Target, 0, len(auditTargets))
	for _, tgt := range auditTargets {
		if tgt.Type() == t {
			tgts = append(tgts, tgt)
		}
	}
	return tgts
}

// UpdateAuditWebhookTargets swaps audit webhook targets with newly loaded ones from the cfg
func UpdateAuditWebhookTargets(cfg Config) error {
	newTgts, err := initSystemTargets(cfg.AuditWebhook)
	if err != nil {
		return err
	}

	// retain kafka targets
	swapAuditMuRW.Lock()
	defer swapAuditMuRW.Unlock()

	newTgts = append(existingAuditTargets(types.TargetKafka), newTgts...)
	cancelAuditTargetType(types.TargetHTTP) // cancel running targets
	auditTargets = newTgts

	return nil
}

// UpdateAuditKafkaTargets swaps audit kafka targets with newly loaded ones from the cfg
func UpdateAuditKafkaTargets(cfg Config) error {
	updated, err := initKafkaTargets(cfg.AuditKafka)
	if err != nil {
		return err
	}

	swapAuditMuRW.Lock()
	defer swapAuditMuRW.Unlock()

	// retain HTTP targets
	updated = append(existingAuditTargets(types.TargetHTTP), updated...)
	cancelAuditTargetType(types.TargetKafka) // cancel running targets
	auditTargets = updated

	return nil
}
