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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/minio/minio/internal/logger"
)

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError

	// Time in which the initiator of a scan must have reported back.
	metacacheMaxRunningAge = time.Minute

	// metacacheBlockSize is the number of file/directory entries to have in each block.
	metacacheBlockSize = 5000

	// metacacheSharePrefix controls whether prefixes on dirty paths are always shared.
	// This will make `test/a` and `test/b` share listings if they are concurrent.
	// Enabling this will make cache sharing more likely and cause less IO,
	// but may cause additional latency to some calls.
	metacacheSharePrefix = false
)

//go:generate msgp -file $GOFILE -unexported

// metacache contains a tracked cache entry.
type metacache struct {
	id           string     `msg:"id"`
	bucket       string     `msg:"b"`
	root         string     `msg:"root"`
	recursive    bool       `msg:"rec"`
	filter       string     `msg:"flt"`
	status       scanStatus `msg:"stat"`
	fileNotFound bool       `msg:"fnf"`
	error        string     `msg:"err"`
	started      time.Time  `msg:"st"`
	ended        time.Time  `msg:"end"`
	lastUpdate   time.Time  `msg:"u"`
	lastHandout  time.Time  `msg:"lh"`
	startedCycle uint64     `msg:"stc"`
	endedCycle   uint64     `msg:"endc"`
	dataVersion  uint8      `msg:"v"`
}

func (m *metacache) finished() bool {
	return !m.ended.IsZero()
}

// matches returns whether the metacache matches the options given.
func (m *metacache) matches(o *listPathOptions, extend time.Duration) bool {
	if o == nil {
		return false
	}

	// Never return transient caches if there is no id.
	if m.status == scanStateError || m.status == scanStateNone || m.dataVersion != metacacheStreamVersion {
		o.debugf("cache %s state or stream version mismatch", m.id)
		return false
	}
	if m.startedCycle < o.OldestCycle {
		o.debugf("cache %s cycle too old", m.id)
		return false
	}

	// Root of what we are looking for must at least have the same
	if !strings.HasPrefix(o.BaseDir, m.root) {
		o.debugf("cache %s prefix mismatch, cached:%v, want:%v", m.id, m.root, o.BaseDir)
		return false
	}
	if m.filter != "" && strings.HasPrefix(m.filter, o.FilterPrefix) {
		o.debugf("cache %s cannot be used because of filter %s", m.id, m.filter)
		return false
	}

	if o.Recursive && !m.recursive {
		o.debugf("cache %s not recursive", m.id)
		// If this is recursive the cached listing must be as well.
		return false
	}
	if o.Separator != slashSeparator && !m.recursive {
		o.debugf("cache %s not slashsep and not recursive", m.id)
		// Non slash separator requires recursive.
		return false
	}
	if !m.finished() && time.Since(m.lastUpdate) > metacacheMaxRunningAge {
		o.debugf("cache %s not running, time: %v", m.id, time.Since(m.lastUpdate))
		// Abandoned
		return false
	}

	if m.finished() && m.endedCycle <= o.OldestCycle {
		if extend <= 0 {
			// If scan has ended the oldest requested must be less.
			o.debugf("cache %s ended and cycle (%v) <= oldest allowed (%v)", m.id, m.endedCycle, o.OldestCycle)
			return false
		}
		if time.Since(m.lastUpdate) > metacacheMaxRunningAge+extend {
			// Cache ended within bloom cycle, but we can extend the life.
			o.debugf("cache %s ended (%v) and beyond extended life (%v)", m.id, m.lastUpdate, metacacheMaxRunningAge+extend)
			return false
		}
	}

	return true
}

// worthKeeping indicates if the cache by itself is worth keeping.
func (m *metacache) worthKeeping(currentCycle uint64) bool {
	if m == nil {
		return false
	}
	cache := m
	switch {
	case !cache.finished() && time.Since(cache.lastUpdate) > metacacheMaxRunningAge:
		// Not finished and update for metacacheMaxRunningAge, discard it.
		return false
	case cache.finished() && cache.startedCycle > currentCycle:
		// Cycle is somehow bigger.
		return false
	case cache.finished() && time.Since(cache.lastHandout) > 48*time.Hour:
		// Keep only for 2 days. Fallback if scanner is clogged.
		return false
	case cache.finished() && currentCycle >= dataUsageUpdateDirCycles && cache.startedCycle < currentCycle-dataUsageUpdateDirCycles:
		// Cycle is too old to be valuable.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings after 5 minutes.
		return time.Since(cache.lastUpdate) < 5*time.Minute
	}
	return true
}

// canBeReplacedBy.
// Both must pass the worthKeeping check.
func (m *metacache) canBeReplacedBy(other *metacache) bool {
	// If the other is older it can never replace.
	if other.started.Before(m.started) || m.id == other.id {
		return false
	}
	if other.status == scanStateNone || other.status == scanStateError {
		return false
	}
	if m.status == scanStateStarted && time.Since(m.lastUpdate) < metacacheMaxRunningAge {
		return false
	}

	// Keep it around a bit longer.
	if time.Since(m.lastHandout) < 30*time.Minute || time.Since(m.lastUpdate) < metacacheMaxRunningAge {
		return false
	}

	// Go through recursive combinations.
	switch {
	case !m.recursive && !other.recursive:
		// If both not recursive root must match.
		return m.root == other.root && strings.HasPrefix(m.filter, other.filter)
	case m.recursive && !other.recursive:
		// A recursive can never be replaced by a non-recursive
		return false
	case !m.recursive && other.recursive:
		// If other is recursive it must contain this root
		return strings.HasPrefix(m.root, other.root) && other.filter == ""
	case m.recursive && other.recursive:
		// Similar if both are recursive
		return strings.HasPrefix(m.root, other.root) && other.filter == ""
	}
	panic("should be unreachable")
}

// baseDirFromPrefix will return the base directory given an object path.
// For example an object with name prefix/folder/object.ext will return `prefix/folder/`.
func baseDirFromPrefix(prefix string) string {
	b := path.Dir(prefix)
	if b == "." || b == "./" || b == "/" {
		b = ""
	}
	if !strings.Contains(prefix, slashSeparator) {
		b = ""
	}
	if len(b) > 0 && !strings.HasSuffix(b, slashSeparator) {
		b += slashSeparator
	}
	return b
}

// update cache with new status.
// The updates are conditional so multiple callers can update with different states.
func (m *metacache) update(update metacache) {
	m.lastUpdate = UTCNow()

	if m.status == scanStateStarted && update.status == scanStateSuccess {
		m.ended = UTCNow()
		m.endedCycle = update.endedCycle
	}

	if m.status == scanStateStarted && update.status != scanStateStarted {
		m.status = update.status
	}

	if m.error == "" && update.error != "" {
		m.error = update.error
		m.status = scanStateError
		m.ended = UTCNow()
	}
	m.fileNotFound = m.fileNotFound || update.fileNotFound
}

// delete all cache data on disks.
func (m *metacache) delete(ctx context.Context) {
	if m.bucket == "" || m.id == "" {
		logger.LogIf(ctx, fmt.Errorf("metacache.delete: bucket (%s) or id (%s) empty", m.bucket, m.id))
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		logger.LogIf(ctx, errors.New("metacache.delete: no object layer"))
		return
	}
	ez, ok := objAPI.(*erasureServerPools)
	if !ok {
		logger.LogIf(ctx, errors.New("metacache.delete: expected objAPI to be *erasureServerPools"))
		return
	}
	ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(m.bucket, m.id))
}
