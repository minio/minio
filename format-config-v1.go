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
	"encoding/json"
	"fmt"
)

type fsFormat struct {
	Version string `json:"version"`
}

type xlFormat struct {
	Version string   `json:"version"`
	Disks   []string `json:"disks"`
}

type formatConfigV1 struct {
	Version string    `json:"version"`
	Format  string    `json:"format"`
	FS      *fsFormat `json:"fs,omitempty"`
	XL      *xlFormat `json:"xl,omitempty"`
}

// FIXME: currently we don't check single exportPath which uses FS layer.

// loadFormatXL - load XL format.json.
func loadFormatXL(storage StorageAPI) (xl *xlFormat, err error) {
	offset := int64(0)
	r, err := storage.ReadFile(minioMetaBucket, formatConfigFile, offset)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(r)
	formatXL := formatConfigV1{}
	err = decoder.Decode(&formatXL)
	if err != nil {
		return nil, err
	}
	if err = r.Close(); err != nil {
		return nil, err
	}
	if formatXL.Version != "1" {
		return nil, fmt.Errorf("Unsupported version of backend format [%s] found.", formatXL.Version)
	}
	if formatXL.Format != "xl" {
		return nil, fmt.Errorf("Unsupported backend format [%s] found.", formatXL.Format)
	}
	return formatXL.XL, nil
}

// checkFormat - validates if format.json file exists.
func checkFormat(storage StorageAPI) error {
	_, err := storage.StatFile(minioMetaBucket, formatConfigFile)
	if err != nil {
		return err
	}
	return nil
}

// saveFormatXL - save XL format configuration
func saveFormatXL(storage StorageAPI, xl *xlFormat) error {
	w, err := storage.CreateFile(minioMetaBucket, formatConfigFile)
	if err != nil {
		return err
	}
	formatXL := formatConfigV1{
		Version: "1",
		Format:  "xl",
		XL:      xl,
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(&formatXL)
	if err != nil {
		if clErr := safeCloseAndRemove(w); clErr != nil {
			return clErr
		}
		return err
	}
	if err = w.Close(); err != nil {
		if clErr := safeCloseAndRemove(w); clErr != nil {
			return clErr
		}
		return err
	}
	return nil
}
