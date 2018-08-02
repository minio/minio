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

import "fmt"

// fsv1 - FS version 1.
type fsv1 struct {
	Version string `json:"version"` // FS version "1".
}

func (fs fsv1) validate() error {
	if fs.Version != V1 {
		return fmt.Errorf("unknown FS version %v", fs.Version)
	}

	return nil
}

// FSV1 - Format FS version 1.
type FSV1 struct {
	formatV1
	FS fsv1 `json:"fs"`
}

// Validate - checks if FSV1 is valid or not.
func (f FSV1) Validate() error {
	if err := f.validateFS(); err != nil {
		return err
	}

	return f.FS.validate()
}
