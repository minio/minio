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

import "github.com/minio/minio/pkg/quick"

type fsFormat struct {
	Version string `json:"version"`
}

type xlFormat struct {
	Version string   `json:"version"`
	Disks   []string `json:"disks"`
}

type formatConfigV1 struct {
	// must have "Version" to "quick" to work
	Version string   `json:"version"`
	Format  string   `json:"format"`
	FS      fsFormat `json:"fs,omitempty"`
	XL      xlFormat `json:"xl,omitempty"`
}

func (f formatConfigV1) Save() error {
	configFile, err := getFormatConfigFile()
	if err != nil {
		return err
	}

	// initialize quick.
	qc, err := quick.New(&f)
	if err != nil {
		return err
	}

	// Save config file.
	return qc.Save(configFile)
}

func (f *formatConfigV1) Load() error {
	configFile, err := getFormatConfigFile()
	if err != nil {
		return err
	}

	f.Version = globalMinioConfigVersion
	qc, err := quick.New(f)
	if err != nil {
		return err
	}
	if err := qc.Load(configFile); err != nil {
		return err
	}

	return nil
}

// saveFormatFS - save FS format configuration
func saveFormatFS(fs fsFormat) error {
	config := formatConfigV1{Version: globalMinioConfigVersion, Format: "fs", FS: fs}
	return config.Save()
}

// saveFormatXL - save XL format configuration
func saveFormatXL(xl xlFormat) error {
	config := formatConfigV1{Version: globalMinioConfigVersion, Format: "xl", XL: xl}
	return config.Save()
}

// getSavedFormatConfig - get saved format configuration
func getSavedFormatConfig() (formatConfigV1, error) {
	config := formatConfigV1{Version: globalMinioConfigVersion}

	if err := config.Load(); err != nil {
		return config, err
	}

	return config, nil
}

// getFormatFS - get saved FS format configuration
func getFormatFS() (fsFormat, error) {
	config, err := getSavedFormatConfig()
	if err != nil {
		return fsFormat{}, err
	}
	return config.FS, nil
}

// getFormatXL - get saved XL format configuration
func getFormatXL() (xlFormat, error) {
	config, err := getSavedFormatConfig()
	if err != nil {
		return xlFormat{}, err
	}
	return config.XL, nil
}
