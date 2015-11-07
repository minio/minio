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

package main

import (
	"encoding/base64"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/minio/minio-xl/pkg/probe"
)

// isValidMD5 - verify if valid md5
func isValidMD5(md5 string) bool {
	if md5 == "" {
		return true
	}
	_, err := base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
	if err != nil {
		return false
	}
	return true
}

/// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// maximum object size per PUT request is 5GB
	maxObjectSize = 1024 * 1024 * 1024 * 5
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size string) bool {
	i, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return true
	}
	if i > maxObjectSize {
		return true
	}
	return false
}

// workaround for docker images with fully static binary.
// for static binaries NSS library will not be a part of the static binary
// hence user.Current() fails
// more here : http://gnu.ist.utl.pt/software/libc/FAQ.html
// FAQ says : NSS (for details just type `info libc "Name Service Switch"') won't work properly without shared libraries
func userCurrent() (*user.User, *probe.Error) {
	if os.Getenv("DOCKERIMAGE") == "1" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, probe.NewError(err)
		}
		return &user.User{Uid: "0", Gid: "0", Username: "root", Name: "root", HomeDir: wd}, nil
	}
	user, err := user.Current()
	if err != nil {
		return nil, probe.NewError(err)
	}
	return user, nil
}
