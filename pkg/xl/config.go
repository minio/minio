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

package xl

import (
	"os/user"
	"path/filepath"

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

// getXLConfigPath get xl config file path
func getXLConfigPath() (string, *probe.Error) {
	if customConfigPath != "" {
		return customConfigPath, nil
	}
	u, err := user.Current()
	if err != nil {
		return "", probe.NewError(err)
	}
	xlConfigPath := filepath.Join(u.HomeDir, ".minio", "xl.json")
	return xlConfigPath, nil
}

// internal variable only accessed via get/set methods
var customConfigPath string

// SetXLConfigPath - set custom xl config path
func SetXLConfigPath(configPath string) {
	customConfigPath = configPath
}

// SaveConfig save xl config
func SaveConfig(a *Config) *probe.Error {
	xlConfigPath, err := getXLConfigPath()
	if err != nil {
		return err.Trace()
	}
	qc, err := quick.New(a)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(xlConfigPath); err != nil {
		return err.Trace()
	}
	return nil
}

// LoadConfig load xl config
func LoadConfig() (*Config, *probe.Error) {
	xlConfigPath, err := getXLConfigPath()
	if err != nil {
		return nil, err.Trace()
	}
	a := &Config{}
	a.Version = "0.0.1"
	qc, err := quick.New(a)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(xlConfigPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*Config), nil
}
