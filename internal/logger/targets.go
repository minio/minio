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

package logger

import (
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
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
}

var (
	// swapMu must be held while reading slice info or swapping targets or auditTargets.
	swapMu sync.Mutex

	// targets is the set of enabled loggers.
	// Must be immutable at all times.
	// Can be swapped to another while holding swapMu
	targets  = []Target{}
	nTargets int32 // atomic count of len(targets)
)

// Targets returns active targets.
// Returned slice may not be modified in any way.
func Targets() []Target {
	if atomic.LoadInt32(&nTargets) == 0 {
		// Lock free if none...
		return nil
	}
	swapMu.Lock()
	res := targets
	swapMu.Unlock()
	return res
}

// AuditTargets returns active audit targets.
// Returned slice may not be modified in any way.
func AuditTargets() []Target {
	if atomic.LoadInt32(&nAuditTargets) == 0 {
		// Lock free if none...
		return nil
	}
	swapMu.Lock()
	res := auditTargets
	swapMu.Unlock()
	return res
}

// auditTargets is the list of enabled audit loggers
// Must be immutable at all times.
// Can be swapped to another while holding swapMu
var (
	auditTargets  = []Target{}
	nAuditTargets int32 // atomic count of len(auditTargets)
)

// AddTarget adds a new logger target to the
// list of enabled loggers
func AddTarget(t Target) error {
	if err := t.Init(); err != nil {
		return err
	}
	swapMu.Lock()
	updated := append(make([]Target, 0, len(targets)+1), targets...)
	updated = append(updated, t)
	targets = updated
	atomic.StoreInt32(&nTargets, int32(len(updated)))
	swapMu.Unlock()

	return nil
}

func cancelAllTargets() {
	for _, tgt := range targets {
		tgt.Cancel()
	}
}

func initHttpTargets(cfgMap map[string]http.Config) (tgts []Target, err error) {
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

func initTargets(cfg Config) (tgts []Target, err error) {
	return initHttpTargets(cfg.HTTP)
}

// UpdateTargets swaps targets with newly loaded ones from the cfg
func UpdateTargets(cfg Config) error {
	updated, err := initTargets(cfg)
	if err != nil {
		return err
	}

	swapMu.Lock()
	for _, tgt := range targets {
		// Preserve console target when dynamically updating
		// other HTTP targets, console target is always present.
		if tgt.String() == ConsoleLoggerTgt {
			updated = append(updated, tgt)
			break
		}
	}
	atomic.StoreInt32(&nTargets, int32(len(updated)))
	cancelAllTargets() // cancel running targets
	targets = updated
	swapMu.Unlock()
	return nil
}

func cancelAllAuditTargets() {
	for _, tgt := range auditTargets {
		tgt.Cancel()
	}
}

func initAuditTargets(cfg Config) (tgts []Target, err error) {
	tgts, err = initHttpTargets(cfg.AuditWebhook)
	if err != nil {
		return tgts, err
	}
	ktgts, err := initKafkaTargets(cfg.AuditKafka)
	if err != nil {
		return tgts, err
	}
	return append(tgts, ktgts...), err
}

// UpdateAuditTargets swaps targets with newly loaded ones from the cfg
func UpdateAuditTargets(cfg Config) error {
	updated, err := initAuditTargets(cfg)
	if err != nil {
		return err
	}

	swapMu.Lock()
	atomic.StoreInt32(&nAuditTargets, int32(len(updated)))
	cancelAllAuditTargets() // cancel running targets
	targets = updated
	swapMu.Unlock()
	return nil
}
