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
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/disk"
)

//go:generate stringer -type=osMetric -trimprefix=osMetric $GOFILE

type osMetric uint8

const (
	osMetricRemoveAll osMetric = iota
	osMetricMkdirAll
	osMetricRename
	osMetricOpenFile
	osMetricOpen
	osMetricOpenFileDirectIO
	osMetricLstat
	osMetricRemove
	osMetricStat
	osMetricAccess
	// .... add more

	osMetricLast
)

func osTrace(s osMetric, startTime time.Time, duration time.Duration, path string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceOS,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "os." + s.String(),
		OSStats: madmin.TraceOSStats{
			Duration: duration,
			Path:     path,
		},
	}
}

func updateOSMetrics(s osMetric, paths ...string) func() {
	if globalTrace.NumSubscribers() == 0 {
		return func() {}
	}

	startTime := time.Now()
	return func() {
		duration := time.Since(startTime)

		globalTrace.Publish(osTrace(s, startTime, duration, strings.Join(paths, " -> ")))
	}
}

// RemoveAll captures time taken to call the underlying os.RemoveAll
func RemoveAll(dirPath string) error {
	defer updateOSMetrics(osMetricRemoveAll, dirPath)()
	return os.RemoveAll(dirPath)
}

// MkdirAll captures time taken to call os.MkdirAll
func MkdirAll(dirPath string, mode os.FileMode) error {
	defer updateOSMetrics(osMetricMkdirAll, dirPath)()
	return os.MkdirAll(dirPath, mode)
}

// Copy the src file to dst.
// Any existing file will be overwritten
// and will not copy file attributes.
func Copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

// isDirectory determines if a file represented
// by `path` is a directory or not
func isDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fileInfo.IsDir(), err
}

// Rename captures time taken to call os.Rename
func Rename(src, dst string) error {
	defer updateOSMetrics(osMetricRename, src, dst)()
	res, _ := isDirectory(dst)
	if !res {
		Copy(src, dst)
		return os.Remove(src)
	} else {
		return os.Rename(src, dst)
	}
}

// OpenFile captures time taken to call os.OpenFile
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	defer updateOSMetrics(osMetricOpenFile, name)()
	return os.OpenFile(name, flag, perm)
}

// Access captures time taken to call syscall.Access()
// on windows, plan9 and solaris syscall.Access uses
// os.Lstat()
func Access(name string) error {
	defer updateOSMetrics(osMetricAccess, name)()
	return access(name)
}

// Open captures time taken to call os.Open
func Open(name string) (*os.File, error) {
	defer updateOSMetrics(osMetricOpen, name)()
	return os.Open(name)
}

// OpenFileDirectIO captures time taken to call disk.OpenFileDirectIO
func OpenFileDirectIO(name string, flag int, perm os.FileMode) (*os.File, error) {
	defer updateOSMetrics(osMetricOpenFileDirectIO, name)()
	return disk.OpenFileDirectIO(name, flag, perm)
}

// Lstat captures time taken to call os.Lstat
func Lstat(name string) (os.FileInfo, error) {
	defer updateOSMetrics(osMetricLstat, name)()
	return os.Lstat(name)
}

// Remove captures time taken to call os.Remove
func Remove(deletePath string) error {
	defer updateOSMetrics(osMetricRemove, deletePath)()
	return os.Remove(deletePath)
}

// Stat captures time taken to call os.Stat
func Stat(name string) (os.FileInfo, error) {
	defer updateOSMetrics(osMetricStat, name)()
	return os.Stat(name)
}
