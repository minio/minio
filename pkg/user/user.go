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

package user

import (
	"os"
	"os/user"
	"runtime"
	"strconv"
)

// Current is a portable implementation to determine the current user.
// Golang's user.Current does not work reliably under docker or 32bit linux
//
// Two issues this code handles :-
//
//   Docker Container - For static binaries NSS library will not be a part of the static binary hence user.Current() fails.
//   Linux Intel 32 bit - CGO is not enabled so it will not link with NSS library.
//
func Current() (*user.User, error) {
	if os.Getenv("DOCKERIMAGE") == "1" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		return &user.User{Uid: "0", Gid: "0", Username: "root", Name: "root", HomeDir: wd}, nil
	}
	if runtime.GOARCH == "386" {
		if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
			return &user.User{
				Uid:      strconv.Itoa(os.Getuid()),
				Gid:      strconv.Itoa(os.Getgid()),
				Username: os.Getenv("USER"),
				Name:     os.Getenv("USER"),
				HomeDir:  os.Getenv("HOME"),
			}, nil
		}
	}
	user, e := user.Current()
	if e != nil {
		return nil, e
	}
	return user, nil
}

// HomeDir - return current home directory.
func HomeDir() (string, error) {
	user, err := Current()
	if err != nil {
		return "", err
	}
	return user.HomeDir, nil
}
