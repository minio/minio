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
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	"github.com/minio/minio/internal/logger/target/types"
)

// Target is the entity that we will receive
// a single log entry and Send it to the log target
//
//	e.g. Send the log to a http server
type Target interface {
	String() string
	Endpoint() string
	Stats() types.TargetStats
	Init(ctx context.Context) error
	IsOnline(ctx context.Context) bool
	Cancel()
	Send(ctx context.Context, entry interface{}) error
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

// TargetStatus returns status of the target (online|offline)
func TargetStatus(ctx context.Context, h Target) madmin.Status {
	if h.IsOnline(ctx) {
		return madmin.Status{Status: string(madmin.ItemOnline)}
	}
	// Previous initialization had failed. Try again.
	if e := h.Init(ctx); e == nil {
		return madmin.Status{Status: string(madmin.ItemOnline)}
	}
	return madmin.Status{Status: string(madmin.ItemOffline)}
}

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

// CurrentStats returns the current statistics.
func CurrentStats() map[string]types.TargetStats {
	sys := SystemTargets()
	audit := AuditTargets()
	res := make(map[string]types.TargetStats, len(sys)+len(audit))
	cnt := make(map[string]int, len(sys)+len(audit))

	// Add system and audit.
	for _, t := range sys {
		key := strings.ToLower(t.Type().String())
		n := cnt[key]
		cnt[key]++
		key = fmt.Sprintf("sys_%s_%d", key, n)
		res[key] = t.Stats()
	}

	for _, t := range audit {
		key := strings.ToLower(t.Type().String())
		n := cnt[key]
		cnt[key]++
		key = fmt.Sprintf("audit_%s_%d", key, n)
		res[key] = t.Stats()
	}

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
func AddSystemTarget(ctx context.Context, t Target) error {
	if err := t.Init(ctx); err != nil {
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

func initSystemTargets(ctx context.Context, cfgMap map[string]http.Config) ([]Target, []error) {
	tgts := []Target{}
	errs := []error{}
	for _, l := range cfgMap {
		if l.Enabled {
			t := http.New(l)
			tgts = append(tgts, t)

			e := t.Init(ctx)
			if e != nil {
				errs = append(errs, e)
			}
		}
	}
	return tgts, errs
}

func initKafkaTargets(ctx context.Context, cfgMap map[string]kafka.Config) ([]Target, []error) {
	tgts := []Target{}
	errs := []error{}
	for _, l := range cfgMap {
		if l.Enabled {
			t := kafka.New(l)
			tgts = append(tgts, t)

			e := t.Init(ctx)
			if e != nil {
				errs = append(errs, e)
			}
		}
	}
	return tgts, errs
}

// Split targets into two groups:
//
//	group1 contains all targets of type t
//	group2 contains the remaining targets
func splitTargets(targets []Target, t types.TargetType) (group1 []Target, group2 []Target) {
	for _, target := range targets {
		if target.Type() == t {
			group1 = append(group1, target)
		} else {
			group2 = append(group2, target)
		}
	}
	return
}

func cancelTargets(targets []Target) {
	for _, target := range targets {
		target.Cancel()
	}
}

// UpdateSystemTargets swaps targets with newly loaded ones from the cfg
func UpdateSystemTargets(ctx context.Context, cfg Config) []error {
	newTgts, errs := initSystemTargets(ctx, cfg.HTTP)

	swapSystemMuRW.Lock()
	consoleTargets, otherTargets := splitTargets(systemTargets, types.TargetConsole)
	newTgts = append(newTgts, consoleTargets...)
	systemTargets = newTgts
	swapSystemMuRW.Unlock()

	cancelTargets(otherTargets) // cancel running targets
	return errs
}

// UpdateAuditWebhookTargets swaps audit webhook targets with newly loaded ones from the cfg
func UpdateAuditWebhookTargets(ctx context.Context, cfg Config) []error {
	newWebhookTgts, errs := initSystemTargets(ctx, cfg.AuditWebhook)

	swapAuditMuRW.Lock()
	// Retain kafka targets
	oldWebhookTgts, otherTgts := splitTargets(auditTargets, types.TargetHTTP)
	newWebhookTgts = append(newWebhookTgts, otherTgts...)
	auditTargets = newWebhookTgts
	swapAuditMuRW.Unlock()

	cancelTargets(oldWebhookTgts) // cancel running targets
	return errs
}

// UpdateAuditKafkaTargets swaps audit kafka targets with newly loaded ones from the cfg
func UpdateAuditKafkaTargets(ctx context.Context, cfg Config) []error {
	newKafkaTgts, errs := initKafkaTargets(ctx, cfg.AuditKafka)

	swapAuditMuRW.Lock()
	// Retain webhook targets
	oldKafkaTgts, otherTgts := splitTargets(auditTargets, types.TargetKafka)
	newKafkaTgts = append(newKafkaTgts, otherTgts...)
	auditTargets = newKafkaTgts
	swapAuditMuRW.Unlock()

	cancelTargets(oldKafkaTgts) // cancel running targets
	return errs
}
