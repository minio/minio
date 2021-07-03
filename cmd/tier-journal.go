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
//msgp:ignore tierJournal walkfn

type tierJournal struct {
	sync.RWMutex
	diskPath string
	file     *os.File // active journal file
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

func initTierDeletionJournal(ctx context.Context) (*tierJournal, error) {
	for _, diskPath := range globalEndpoints.LocalDisksPaths() {
		j := &tierJournal{
			diskPath: diskPath,
		}

		if err := os.MkdirAll(filepath.Dir(j.JournalPath()), os.FileMode(0700)); err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		err := j.Open()
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		go j.deletePending(ctx.Done())
		return j, nil
	}

	return nil, errors.New("no local disk found")
}

// rotate rotates the journal. If a read-only journal already exists it does
// nothing. Otherwise renames the active journal to a read-only journal and
// opens a new active journal.
func (j *tierJournal) rotate() error {
	// Do nothing if a read-only journal file already exists.
	if _, err := os.Stat(j.ReadOnlyPath()); err == nil {
		return nil
	}
	// Close the active journal if present.
	j.Close()
	// Open a new active journal for subsequent journalling.
	return j.Open()
}

type walkFn func(objName, rvID, tierName string) error

func (j *tierJournal) ReadOnlyPath() string {
	return filepath.Join(j.diskPath, minioMetaBucket, "ilm", "deletion-journal.ro.bin")
}

func (j *tierJournal) JournalPath() string {
	return filepath.Join(j.diskPath, minioMetaBucket, "ilm", "deletion-journal.bin")
}

func (j *tierJournal) WalkEntries(fn walkFn) {
	err := j.rotate()
	if err != nil {
		logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to rotate pending deletes journal %s", err))
		return
	}

	ro, err := j.OpenRO()
	switch {
	case errors.Is(err, os.ErrNotExist):
		return // No read-only journal to process; nothing to do.
	case err != nil:
		logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed open read-only journal for processing %s", err))
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
			logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to decode journal entry %s", err))
			break
		}
		err = fn(entry.ObjName, entry.VersionID, entry.TierName)
		if err != nil && !isErrObjectNotFound(err) {
			logger.LogIf(context.Background(), fmt.Errorf("tier-journal: failed to delete transitioned object %s from %s due to %s", entry.ObjName, entry.TierName, err))
			// We add the entry into the active journal to try again
			// later.
			j.AddEntry(entry)
		}
	}
	if done {
		os.Remove(j.ReadOnlyPath())
	}
}

func deleteObjectFromRemoteTier(objName, rvID, tierName string) error {
	w, err := globalTierConfigMgr.getDriver(tierName)
	if err != nil {
		return err
	}
	err = w.Remove(context.Background(), objName, remoteVersionID(rvID))
	if err != nil {
		return err
	}
	return nil
}

func (j *tierJournal) deletePending(done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			j.WalkEntries(deleteObjectFromRemoteTier)

		case <-done:
			j.Close()
			return
		}
	}
}

func (j *tierJournal) AddEntry(je jentry) error {
	// Open journal if it hasn't been
	err := j.Open()
	if err != nil {
		return err
	}

	b, err := je.MarshalMsg(nil)
	if err != nil {
		return err
	}

	j.Lock()
	defer j.Unlock()
	_, err = j.file.Write(b)
	if err != nil {
		j.file = nil // reset to allow subsequent reopen when file/disk is available.
	}
	return err
}

// Close closes the active journal and renames it to read-only for pending
// deletes processing. Note: calling Close on a closed journal is a no-op.
func (j *tierJournal) Close() error {
	j.Lock()
	defer j.Unlock()
	if j.file == nil { // already closed
		return nil
	}

	var (
		f   *os.File
		fi  os.FileInfo
		err error
	)
	// Setting j.file to nil
	f, j.file = j.file, f
	if fi, err = f.Stat(); err != nil {
		return err
	}
	defer f.Close()
	// Skip renaming active journal if empty.
	if fi.Size() == tierJournalHdrLen {
		return nil
	}

	jPath := j.JournalPath()
	jroPath := j.ReadOnlyPath()
	// Rotate active journal to perform pending deletes.
	err = os.Rename(jPath, jroPath)
	if err != nil {
		return err
	}

	return nil
}

// Open opens a new active journal. Note: calling Open on an opened journal is a
// no-op.
func (j *tierJournal) Open() error {
	j.Lock()
	defer j.Unlock()
	if j.file != nil { // already open
		return nil
	}

	var err error
	j.file, err = os.OpenFile(j.JournalPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY|writeMode, 0644)
	if err != nil {
		return err
	}

	// write journal version header if active journal is empty
	fi, err := j.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		var data [tierJournalHdrLen]byte
		binary.LittleEndian.PutUint16(data[:], tierJournalVersion)
		_, err = j.file.Write(data[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *tierJournal) OpenRO() (io.ReadCloser, error) {
	file, err := os.Open(j.ReadOnlyPath())
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
