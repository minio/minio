/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package format

import (
	"fmt"
)

// fsv2 - FS version 2; version bump to indicate multipart backend namespace change to
// .minio.sys/multipart/SHA256(BUCKET/OBJECT)/UPLOADID/{fs.json, part.1, part.2, ...}
type fsv2 struct {
	Version string `json:"version"` // FS version "2".
}

func (fs fsv2) validate() error {
	if fs.Version != V2 {
		return fmt.Errorf("unknown FS version %v", fs.Version)
	}

	return nil
}

// FSV2 - Format FS version 2.
type FSV2 struct {
	formatV1
	FS fsv2 `json:"fs"`
}

// Validate - checks if FSV2 is valid or not.
func (f FSV2) Validate() error {
	if err := f.validateFS(); err != nil {
		return err
	}

	return f.FS.validate()
}

// NewFSV2 - creates new format FS version 2.
func NewFSV2() *FSV2 {
	return &FSV2{
		formatV1: formatV1{
			Version: V1,
			Format:  fsType,
		},
		FS: fsv2{
			Version: V2,
		},
	}
}
