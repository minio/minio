/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"os"
	"strings"
	"time"

	"github.com/minio/minio/pkg/disk"
	trace "github.com/minio/minio/pkg/trace"
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
	// .... add more

	osMetricLast
)

func osTrace(s osMetric, startTime time.Time, duration time.Duration, path string) trace.Info {
	return trace.Info{
		TraceType: trace.OS,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "os." + s.String(),
		OSStats: trace.OSStats{
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

		globalTrace.Publish(osTrace(s, startTime, duration, strings.Join(paths, " ")))
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

// Rename captures time taken to call os.Rename
func Rename(src, dst string) error {
	defer updateOSMetrics(osMetricRename, src, dst)()
	return os.Rename(src, dst)
}

// OpenFile captures time taken to call os.OpenFile
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	defer updateOSMetrics(osMetricOpenFile, name)()
	return os.OpenFile(name, flag, perm)
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
