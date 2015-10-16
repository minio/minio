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
	"os/user"
	"path/filepath"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio-xl/pkg/quick"
)

func getFSMultipartConfigPath() (string, *probe.Error) {
	if customMultipartsConfigPath != "" {
		return customMultipartsConfigPath, nil
	}
	u, err := user.Current()
	if err != nil {
		return "", probe.NewError(err)
	}
	fsMultipartsConfigPath := filepath.Join(u.HomeDir, ".minio", "multiparts.json")
	return fsMultipartsConfigPath, nil
}

// internal variable only accessed via get/set methods
var customConfigPath, customMultipartsConfigPath string

// SetFSConfigPath - set custom fs config path
func SetFSConfigPath(configPath string) {
	customConfigPath = configPath
}

// SetFSMultipartsConfigPath - set custom multiparts session config path
func SetFSMultipartsConfigPath(configPath string) {
	customMultipartsConfigPath = configPath
}

// SaveMultipartsSession - save multiparts
func SaveMultipartsSession(multiparts *Multiparts) *probe.Error {
	fsMultipartsConfigPath, err := getFSMultipartConfigPath()
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

// loadMultipartsSession load multipart session file
func loadMultipartsSession() (*Multiparts, *probe.Error) {
	fsMultipartsConfigPath, err := getFSMultipartConfigPath()
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
