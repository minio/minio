/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"os"
	"path/filepath"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/user"
)

var customWebConfigDir = ""

// getWebConfigDir get web config dir.
func getWebConfigDir() (string, *probe.Error) {
	if customWebConfigDir != "" {
		return customWebConfigDir, nil
	}
	homeDir, e := user.HomeDir()
	if e != nil {
		return "", probe.NewError(e)
	}
	webConfigDir := filepath.Join(homeDir, ".minio", "web")
	return webConfigDir, nil
}

func mustGetWebConfigDir() string {
	webConfigDir, err := getWebConfigDir()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return webConfigDir
}

// createWebConfigDir create users config path
func createWebConfigDir() *probe.Error {
	webConfigDir, err := getWebConfigDir()
	if err != nil {
		return err.Trace()
	}
	if err := os.MkdirAll(webConfigDir, 0700); err != nil {
		return probe.NewError(err)
	}
	return nil
}

func mustGetPrivateKeyPath() string {
	webConfigDir, err := getWebConfigDir()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return webConfigDir + "/private.key"
}
