/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"os/user"
	"path/filepath"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/quick"
)

// getDonutConfigPath get donut config file path
func getDonutConfigPath() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", iodine.New(err, nil)
	}
	donutConfigPath := filepath.Join(u.HomeDir, ".minio", "donut.json")
	return donutConfigPath, nil
}

// SaveConfig save donut config
func SaveConfig(a *Config) error {
	donutConfigPath, err := getDonutConfigPath()
	if err != nil {
		return iodine.New(err, nil)
	}
	qc, err := quick.New(a)
	if err != nil {
		return iodine.New(err, nil)
	}
	if err := qc.Save(donutConfigPath); err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// LoadConfig load donut config
func LoadConfig() (*Config, error) {
	donutConfigPath, err := getDonutConfigPath()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	a := &Config{}
	a.Version = "0.0.1"
	qc, err := quick.New(a)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	if err := qc.Load(donutConfigPath); err != nil {
		return nil, iodine.New(err, nil)
	}
	return qc.Data().(*Config), nil
}

// LoadDonut load donut from config
func LoadDonut() (Interface, error) {
	conf, err := LoadConfig()
	if err != nil {
		conf = &Config{
			Version: "0.0.1",
			MaxSize: 512000000,
		}
	}
	donut, err := New(conf)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return donut, nil
}
