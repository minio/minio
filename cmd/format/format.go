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

const (
	// fsType - FS format type
	fsType = "fs"

	// V1 - version 1
	V1 = "1"

	// V2 - version 2
	V2 = "2"
)

type formatV1 struct {
	Version string `json:"version"` // Format version "1".
	Format  string `json:"format"`  // Format type "fs" or "xl".
}

func (f formatV1) validate(formatType string) error {
	if f.Version != V1 {
		return fmt.Errorf("unknown format version %v", f.Version)
	}

	if f.Format != formatType {
		return fmt.Errorf("%v found for %v format", f.Format, formatType)
	}

	return nil
}

func (f formatV1) validateFS() error {
	return f.validate(fsType)
}
