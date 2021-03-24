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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/env"
)

var (
	logTime   bool = false
	threshold time.Duration
)

func init() {
	logTime = env.IsSet(config.EnvLogPosixTimes)
	t, _ := env.GetInt(
		config.EnvLogPosixThresholdInMS,
		100,
	)
	threshold = time.Duration(t) * time.Millisecond
}

func reportTime(name *strings.Builder, startTime time.Time) {
	delta := time.Since(startTime)
	if delta > threshold {
		name.WriteString(" ")
		name.WriteString(delta.String())
		fmt.Println(name.String())
	}
}

// RemoveAll captures time taken to call the underlying os.RemoveAll
func RemoveAll(dirPath string) error {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.RemoveAll: ")
		s.WriteString(dirPath)
		defer reportTime(&s, startTime)
	}
	return os.RemoveAll(dirPath)
}

// MkdirAll captures time taken to call os.MkdirAll
func MkdirAll(dirPath string, mode os.FileMode) error {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.MkdirAll: ")
		s.WriteString(dirPath)
		defer reportTime(&s, startTime)
	}
	return os.MkdirAll(dirPath, mode)
}

// Rename captures time taken to call os.Rename
func Rename(src, dst string) error {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.Rename: ")
		s.WriteString(src)
		s.WriteString(" to ")
		s.WriteString(dst)
		defer reportTime(&s, startTime)
	}
	return os.Rename(src, dst)
}

// OpenFile captures time taken to call os.OpenFile
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.OpenFile: ")
		s.WriteString(name)
		defer reportTime(&s, startTime)
	}
	return os.OpenFile(name, flag, perm)
}

// Open captures time taken to call os.Open
func Open(name string) (*os.File, error) {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.Open: ")
		s.WriteString(name)
		defer reportTime(&s, startTime)
	}
	return os.Open(name)
}

// OpenFileDirectIO captures time taken to call disk.OpenFileDirectIO
func OpenFileDirectIO(name string, flag int, perm os.FileMode) (*os.File, error) {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("disk.OpenFileDirectIO: ")
		s.WriteString(name)
		defer reportTime(&s, startTime)
	}
	return disk.OpenFileDirectIO(name, flag, perm)
}

// Lstat captures time taken to call os.Lstat
func Lstat(name string) (os.FileInfo, error) {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.Lstat: ")
		s.WriteString(name)
		defer reportTime(&s, startTime)
	}
	return os.Lstat(name)
}

// Remove captures time taken to call os.Remove
func Remove(deletePath string) error {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.Remove: ")
		s.WriteString(deletePath)
		defer reportTime(&s, startTime)
	}
	return os.Remove(deletePath)
}

// Stat captures time taken to call os.Stat
func Stat(name string) (os.FileInfo, error) {
	if logTime {
		startTime := time.Now()
		var s strings.Builder
		s.WriteString("os.Stat: ")
		s.WriteString(name)
		defer reportTime(&s, startTime)
	}
	return os.Stat(name)
}
