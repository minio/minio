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

	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	types "github.com/minio/minio/internal/logger/target/loggertypes"
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
	Send(ctx context.Context, entry any) error
	Type() types.TargetType
}

type targetsList struct {
	list []Target
	mu   sync.RWMutex
}

func newTargetsList() *targetsList {
	return &targetsList{}
}

func (tl *targetsList) get() []Target {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	return tl.list
}

func (tl *targetsList) add(t Target) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.list = append(tl.list, t)
}

func (tl *targetsList) set(tgts []Target) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.list = tgts
}

var (

	// systemTargets is the set of enabled loggers.
	systemTargets = newTargetsList()

	// auditTargets is the list of enabled audit loggers
	auditTargets = newTargetsList()

	// This is always set represent /dev/console target
	consoleTgt Target
)

// SystemTargets returns active targets.
// Returned slice may not be modified in any way.
func SystemTargets() []Target {
	return systemTargets.get()
}

// AuditTargets returns active audit targets.
// Returned slice may not be modified in any way.
func AuditTargets() []Target {
	return auditTargets.get()
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

// AddSystemTarget adds a new logger target to the
// list of enabled loggers
func AddSystemTarget(ctx context.Context, t Target) error {
	if err := t.Init(ctx); err != nil {
		return err
	}

	if consoleTgt == nil {
		if t.Type() == types.TargetConsole {
			consoleTgt = t
		}
	}

	systemTargets.add(t)
	return nil
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
	return group1, group2
}

func cancelTargets(targets []Target) {
	for _, target := range targets {
		go target.Cancel()
	}
}

// UpdateHTTPWebhooks swaps system webhook targets with newly loaded ones from the cfg
func UpdateHTTPWebhooks(ctx context.Context, cfgs map[string]http.Config) (errs []error) {
	return updateHTTPTargets(ctx, cfgs, systemTargets)
}

// UpdateAuditWebhooks swaps audit webhook targets with newly loaded ones from the cfg
func UpdateAuditWebhooks(ctx context.Context, cfgs map[string]http.Config) (errs []error) {
	return updateHTTPTargets(ctx, cfgs, auditTargets)
}

func updateHTTPTargets(ctx context.Context, cfgs map[string]http.Config, targetsList *targetsList) (errs []error) {
	tgts := make([]*http.Target, 0)
	newWebhooks := make([]Target, 0)
	for _, cfg := range cfgs {
		if cfg.Enabled {
			t, err := http.New(cfg)
			if err != nil {
				errs = append(errs, err)
			}
			tgts = append(tgts, t)
			newWebhooks = append(newWebhooks, t)
		}
	}

	oldTargets, others := splitTargets(targetsList.get(), types.TargetHTTP)
	newWebhooks = append(newWebhooks, others...)

	for i := range oldTargets {
		currentTgt, ok := oldTargets[i].(*http.Target)
		if !ok {
			continue
		}
		var newTgt *http.Target

		for ii := range tgts {
			if currentTgt.Name() == tgts[ii].Name() {
				newTgt = tgts[ii]
				currentTgt.AssignMigrateTarget(newTgt)
				http.CreateOrAdjustGlobalBuffer(currentTgt, newTgt)
				break
			}
		}
	}

	for _, t := range tgts {
		err := t.Init(ctx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	targetsList.set(newWebhooks)

	cancelTargets(oldTargets)

	return errs
}

// UpdateAuditKafkaTargets swaps audit kafka targets with newly loaded ones from the cfg
func UpdateAuditKafkaTargets(ctx context.Context, cfg Config) []error {
	newKafkaTgts, errs := initKafkaTargets(ctx, cfg.AuditKafka)

	// Retain webhook targets
	oldKafkaTgts, otherTgts := splitTargets(auditTargets.get(), types.TargetKafka)
	newKafkaTgts = append(newKafkaTgts, otherTgts...)
	auditTargets.set(newKafkaTgts)

	cancelTargets(oldKafkaTgts) // cancel running targets
	return errs
}
