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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file $GOFILE -unexported
//msgp:ignore tierJournal tierDiskJournal walkfn

type tierDiskJournal struct {
	sync.RWMutex
	diskPath string
	file     *os.File // active journal file
}

type tierJournal struct {
	*tierDiskJournal // for processing legacy journal entries
	*tierMemJournal  // for processing new journal entries
}

type jentry struct {
	ObjName   string `msg:"obj"`
	VersionID string `msg:"vid"`
	TierName  string `msg:"tier"`
}

const (
	tierJournalVersion = 1
	tierJournalHdrLen  = 2 // 2 bytes
)

var (
	errUnsupportedJournalVersion = errors.New("unsupported pending deletes journal version")
)

func newTierDiskJournal() *tierDiskJournal {
	return &tierDiskJournal{}
}

// initTierDeletionJournal intializes an in-memory journal built using a
// buffered channel for new journal entries. It also initializes the on-disk
// journal only to process existing journal entries made from previous versions.
func initTierDeletionJournal(ctx context.Context) (*tierJournal, error) {
	j := &tierJournal{
		tierMemJournal:  newTierMemJoural(1000),
		tierDiskJournal: newTierDiskJournal(),
	}
	for _, diskPath := range globalEndpoints.LocalDisksPaths() {
		j.diskPath = diskPath
		if err := os.MkdirAll(filepath.Dir(j.JournalPath()), os.FileMode(0700)); err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		err := j.Open()
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		go j.deletePending(ctx)  // for existing journal entries from previous MinIO versions
		go j.processEntries(ctx) // for newer journal entries circa free-versions
		return j, nil
	}

	return nil, errors.New("no local disk found")
}

// rotate rotates the journal. If a read-only journal already exists it does
// nothing. Otherwise renames the active journal to a read-only journal and
// opens a new active journal.
func (jd *tierDiskJournal) rotate() error {
	// Do nothing if a read-only journal file already exists.
	if _, err := os.Stat(jd.ReadOnlyPath()); err == nil {
		return nil
	}
	// Close the active journal if present.
	jd.Close()
	// Open a new active journal for subsequent journalling.
	return jd.Open()
}

type walkFn func(ctx context.Context, objName, rvID, tierName string) error

func (jd *tierDiskJournal) ReadOnlyPath() string {
	return filepath.Join(jd.diskPath, minioMetaBucket, "ilm", "deletion-journal.ro.bin")
}

func (jd *tierDiskJournal) JournalPath() string {
	return filepath.Join(jd.diskPath, minioMetaBucket, "ilm", "deletion-journal.bin")
}

func (jd *tierDiskJournal) WalkEntries(ctx context.Context, fn walkFn) {
	err := jd.rotate()
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("tier-journal: failed to rotate pending deletes journal %s", err))
		return
	}

	ro, err := jd.OpenRO()
	switch {
	case errors.Is(err, os.ErrNotExist):
		return // No read-only journal to process; nothing to do.
	case err != nil:
		logger.LogIf(ctx, fmt.Errorf("tier-journal: failed open read-only journal for processing %s", err))
		return
	}
	defer ro.Close()
	mr := msgp.NewReader(ro)

	done := false
	for {
		var entry jentry
		err := entry.DecodeMsg(mr)
		if errors.Is(err, io.EOF) {
			done = true
			break
		}
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("tier-journal: failed to decode journal entry %s", err))
			break
		}
		err = fn(ctx, entry.ObjName, entry.VersionID, entry.TierName)
		if err != nil && !isErrObjectNotFound(err) {
			logger.LogIf(ctx, fmt.Errorf("tier-journal: failed to delete transitioned object %s from %s due to %s", entry.ObjName, entry.TierName, err))
			// We add the entry into the active journal to try again
			// later.
			jd.addEntry(entry)
		}
	}
	if done {
		os.Remove(jd.ReadOnlyPath())
	}
}

func deleteObjectFromRemoteTier(ctx context.Context, objName, rvID, tierName string) error {
	w, err := globalTierConfigMgr.getDriver(tierName)
	if err != nil {
		return err
	}
	err = w.Remove(ctx, objName, remoteVersionID(rvID))
	if err != nil {
		return err
	}
	return nil
}

func (jd *tierDiskJournal) deletePending(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			jd.WalkEntries(ctx, deleteObjectFromRemoteTier)

		case <-ctx.Done():
			jd.Close()
			return
		}
	}
}

func (jd *tierDiskJournal) addEntry(je jentry) error {
	// Open journal if it hasn't been
	err := jd.Open()
	if err != nil {
		return err
	}

	b, err := je.MarshalMsg(nil)
	if err != nil {
		return err
	}

	jd.Lock()
	defer jd.Unlock()
	_, err = jd.file.Write(b)
	if err != nil {
		jd.file = nil // reset to allow subsequent reopen when file/disk is available.
	}
	return err
}

// Close closes the active journal and renames it to read-only for pending
// deletes processing. Note: calling Close on a closed journal is a no-op.
func (jd *tierDiskJournal) Close() error {
	jd.Lock()
	defer jd.Unlock()
	if jd.file == nil { // already closed
		return nil
	}

	var (
		f   *os.File
		fi  os.FileInfo
		err error
	)
	// Setting j.file to nil
	f, jd.file = jd.file, f
	if fi, err = f.Stat(); err != nil {
		return err
	}
	defer f.Close()
	// Skip renaming active journal if empty.
	if fi.Size() == tierJournalHdrLen {
		return nil
	}

	jPath := jd.JournalPath()
	jroPath := jd.ReadOnlyPath()
	// Rotate active journal to perform pending deletes.
	err = os.Rename(jPath, jroPath)
	if err != nil {
		return err
	}

	return nil
}

// Open opens a new active journal. Note: calling Open on an opened journal is a
// no-op.
func (jd *tierDiskJournal) Open() error {
	jd.Lock()
	defer jd.Unlock()
	if jd.file != nil { // already open
		return nil
	}

	var err error
	jd.file, err = os.OpenFile(jd.JournalPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY|writeMode, 0666)
	if err != nil {
		return err
	}

	// write journal version header if active journal is empty
	fi, err := jd.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		var data [tierJournalHdrLen]byte
		binary.LittleEndian.PutUint16(data[:], tierJournalVersion)
		_, err = jd.file.Write(data[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (jd *tierDiskJournal) OpenRO() (io.ReadCloser, error) {
	file, err := os.Open(jd.ReadOnlyPath())
	if err != nil {
		return nil, err
	}

	// read journal version header
	var data [tierJournalHdrLen]byte
	if _, err := io.ReadFull(file, data[:]); err != nil {
		return nil, err
	}

	switch binary.LittleEndian.Uint16(data[:]) {
	case tierJournalVersion:
		return file, nil
	default:
		return nil, errUnsupportedJournalVersion
	}
}

// jentryV1 represents the entry in the journal before RemoteVersionID was
// added. It remains here for use in tests for the struct element addition.
type jentryV1 struct {
	ObjName  string `msg:"obj"`
	TierName string `msg:"tier"`
}
