/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
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
)

//go:generate msgp -file $GOFILE -unexported

// metacache contains a tracked cache entry.
type metacache struct {
	id           string     `msg:"id"`
	bucket       string     `msg:"b"`
	root         string     `msg:"root"`
	recursive    bool       `msg:"rec"`
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
		// Keep only for 2 days. Fallback if crawler is clogged.
		return false
	case cache.finished() && currentCycle >= dataUsageUpdateDirCycles && cache.startedCycle < currentCycle-dataUsageUpdateDirCycles:
		// Cycle is too old to be valuable.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings after 10 minutes.
		return time.Since(cache.lastUpdate) < 10*time.Minute
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
	// Keep it around a bit longer.
	if time.Since(m.lastHandout) < time.Hour {
		return false
	}

	// Go through recursive combinations.
	switch {
	case !m.recursive && !other.recursive:
		// If both not recursive root must match.
		return m.root == other.root
	case m.recursive && !other.recursive:
		// A recursive can never be replaced by a non-recursive
		return false
	case !m.recursive && other.recursive:
		// If other is recursive it must contain this root
		return strings.HasPrefix(m.root, other.root)
	case m.recursive && other.recursive:
		// Similar if both are recursive
		return strings.HasPrefix(m.root, other.root)
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
	ez, ok := objAPI.(*erasureServerSets)
	if !ok {
		logger.LogIf(ctx, errors.New("metacache.delete: expected objAPI to be *erasureServerSets"))
		return
	}
	ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(m.bucket, m.id))
}
