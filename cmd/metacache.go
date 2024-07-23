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

	"github.com/minio/pkg/v3/console"
)

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError

	// Time in which the initiator of a scan must have reported back.
	metacacheMaxRunningAge = time.Minute

	// Max time between client calls before dropping an async cache listing.
	metacacheMaxClientWait = 3 * time.Minute

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
	// do not re-arrange the struct this struct has been ordered to use less
	// space - if you do so please run https://github.com/orijtech/structslop
	// and verify if your changes are optimal.
	ended        time.Time  `msg:"end"`
	started      time.Time  `msg:"st"`
	lastHandout  time.Time  `msg:"lh"`
	lastUpdate   time.Time  `msg:"u"`
	bucket       string     `msg:"b"`
	filter       string     `msg:"flt"`
	id           string     `msg:"id"`
	error        string     `msg:"err"`
	root         string     `msg:"root"`
	fileNotFound bool       `msg:"fnf"`
	status       scanStatus `msg:"stat"`
	recursive    bool       `msg:"rec"`
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
	case cache.finished() && time.Since(cache.lastHandout) > 5*metacacheMaxClientWait:
		// Keep for 15 minutes after we last saw the client.
		// Since the cache is finished keeping it a bit longer doesn't hurt us.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings after 5 minutes.
		return time.Since(cache.lastUpdate) > 5*time.Minute
	}
	return true
}

// keepAlive will continuously update lastHandout until ctx is canceled.
func (m metacache) keepAlive(ctx context.Context, rpc *peerRESTClient) {
	// we intentionally operate on a copy of m, so we can update without locks.
	t := time.NewTicker(metacacheMaxClientWait / 10)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Request is done, stop updating.
			return
		case <-t.C:
			m.lastHandout = time.Now()

			if m2, err := rpc.UpdateMetacacheListing(ctx, m); err == nil {
				if m2.status != scanStateStarted {
					if serverDebugLog {
						console.Debugln("returning", m.id, "due to scan state", m2.status, time.Now().Format(time.RFC3339))
					}
					return
				}
				m = m2
				if serverDebugLog {
					console.Debugln("refreshed", m.id, time.Now().Format(time.RFC3339))
				}
			} else if serverDebugLog {
				console.Debugln("error refreshing", m.id, time.Now().Format(time.RFC3339))
			}
		}
	}
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
	now := UTCNow()
	m.lastUpdate = now

	if update.lastHandout.After(m.lastHandout) {
		m.lastHandout = update.lastUpdate
		if m.lastHandout.After(now) {
			m.lastHandout = now
		}
	}
	if m.status == scanStateStarted && update.status == scanStateSuccess {
		m.ended = now
	}

	if m.status == scanStateStarted && update.status != scanStateStarted {
		m.status = update.status
	}

	if m.status == scanStateStarted && time.Since(m.lastHandout) > metacacheMaxClientWait {
		// Drop if client hasn't been seen for 3 minutes.
		m.status = scanStateError
		m.error = "client not seen"
	}

	if m.error == "" && update.error != "" {
		m.error = update.error
		m.status = scanStateError
		m.ended = now
	}
	m.fileNotFound = m.fileNotFound || update.fileNotFound
}

// delete all cache data on disks.
func (m *metacache) delete(ctx context.Context) {
	if m.bucket == "" || m.id == "" {
		bugLogIf(ctx, fmt.Errorf("metacache.delete: bucket (%s) or id (%s) empty", m.bucket, m.id))
		return
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		internalLogIf(ctx, errors.New("metacache.delete: no object layer"))
		return
	}
	ez, ok := objAPI.(deleteAllStorager)
	if !ok {
		bugLogIf(ctx, errors.New("metacache.delete: expected objAPI to be 'deleteAllStorager'"))
		return
	}
	ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(m.bucket, m.id))
}
