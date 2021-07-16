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
	dataVersion  uint8      `msg:"v"`
}

func (m *metacache) finished() bool {
	return !m.ended.IsZero()
}

// worthKeeping indicates if the cache by itself is worth keeping.
func (m *metacache) worthKeeping() bool {
	if m == nil {
		return false
	}
	cache := m
	switch {
	case !cache.finished() && time.Since(cache.lastUpdate) > metacacheMaxRunningAge:
		// Not finished and update for metacacheMaxRunningAge, discard it.
		return false
	case cache.finished() && time.Since(cache.lastHandout) > 30*time.Minute:
		// Keep only for 30 minutes.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings after 5 minutes.
		return time.Since(cache.lastUpdate) > 5*time.Minute
	}
	return true
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
	}

	if m.status == scanStateStarted && update.status != scanStateStarted {
		m.status = update.status
	}

	if m.status == scanStateStarted && time.Since(m.lastHandout) > 15*time.Minute {
		m.status = scanStateError
		m.error = "client not seen"
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
