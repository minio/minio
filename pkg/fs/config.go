/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio-xl/pkg/quick"
)

// Workaround for docker images with fully static binary and 32bit linux operating systems.
// For static binaries NSS library will not be a part of the static binary hence user.Current() fails.
// For 32bit linux CGO is not enabled so it will not provide linux specific codebase.
func userCurrent() (*user.User, *probe.Error) {
	if os.Getenv("DOCKERIMAGE") == "1" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, probe.NewError(err)
		}
		return &user.User{Uid: "0", Gid: "0", Username: "root", Name: "root", HomeDir: wd}, nil
	}
	if runtime.GOARCH == "386" && runtime.GOOS == "linux" {
		return &user.User{
			Uid:      strconv.Itoa(os.Getuid()),
			Gid:      strconv.Itoa(os.Getgid()),
			Username: os.Getenv("USER"),
			Name:     os.Getenv("USER"),
			HomeDir:  os.Getenv("HOME"),
		}, nil
	}
	user, err := user.Current()
	if err != nil {
		return nil, probe.NewError(err)
	}
	return user, nil
}

func getFSBucketsConfigPath() (string, *probe.Error) {
	if customBucketsConfigPath != "" {
		return customBucketsConfigPath, nil
	}
	u, err := userCurrent()
	if err != nil {
		return "", err.Trace()
	}
	fsBucketsConfigPath := filepath.Join(u.HomeDir, ".minio", "buckets.json")
	return fsBucketsConfigPath, nil
}

func getFSMultipartsSessionConfigPath() (string, *probe.Error) {
	if customMultipartsConfigPath != "" {
		return customMultipartsConfigPath, nil
	}
	u, err := userCurrent()
	if err != nil {
		return "", err.Trace()
	}
	fsMultipartsConfigPath := filepath.Join(u.HomeDir, ".minio", "multiparts-session.json")
	return fsMultipartsConfigPath, nil
}

// internal variable only accessed via get/set methods
var customMultipartsConfigPath, customBucketsConfigPath string

// SetFSBucketsConfigPath - set custom fs buckets config path
func SetFSBucketsConfigPath(configPath string) {
	customBucketsConfigPath = configPath
}

// SetFSMultipartsConfigPath - set custom multiparts session config path
func SetFSMultipartsConfigPath(configPath string) {
	customMultipartsConfigPath = configPath
}

// SaveMultipartsSession - save multiparts
func SaveMultipartsSession(multiparts *Multiparts) *probe.Error {
	fsMultipartsConfigPath, err := getFSMultipartsSessionConfigPath()
	if err != nil {
		return err.Trace()
	}
	qc, err := quick.New(multiparts)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(fsMultipartsConfigPath); err != nil {
		return err.Trace()
	}
	return nil
}

// SaveBucketsMetadata - save metadata of all buckets
func SaveBucketsMetadata(buckets *Buckets) *probe.Error {
	fsBucketsConfigPath, err := getFSBucketsConfigPath()
	if err != nil {
		return err.Trace()
	}
	qc, err := quick.New(buckets)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(fsBucketsConfigPath); err != nil {
		return err.Trace()
	}
	return nil
}

// loadMultipartsSession load multipart session file
func loadMultipartsSession() (*Multiparts, *probe.Error) {
	fsMultipartsConfigPath, err := getFSMultipartsSessionConfigPath()
	if err != nil {
		return nil, err.Trace()
	}
	multiparts := &Multiparts{}
	multiparts.Version = "1"
	multiparts.ActiveSession = make(map[string]*MultipartSession)
	qc, err := quick.New(multiparts)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(fsMultipartsConfigPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*Multiparts), nil
}

// loadBucketsMetadata load buckets metadata file
func loadBucketsMetadata() (*Buckets, *probe.Error) {
	fsBucketsConfigPath, err := getFSBucketsConfigPath()
	if err != nil {
		return nil, err.Trace()
	}
	buckets := &Buckets{}
	buckets.Version = "1"
	buckets.Metadata = make(map[string]*BucketMetadata)
	qc, err := quick.New(buckets)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(fsBucketsConfigPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*Buckets), nil
}
