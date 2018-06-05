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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
)

// ParseFS - parses FS format from given reader and returns FSV2, migration flag and error.
func ParseFS(reader io.Reader) (*FSV2, bool, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, false, err
	}

	var f FSV2
	if err = json.Unmarshal(data, &f); err != nil {
		return nil, false, err
	}

	if err = f.validateFS(); err != nil {
		return nil, false, err
	}

	switch f.FS.Version {
	case V1:
		// Migrate to V2.
		var f1 FSV1
		if err = json.Unmarshal(data, &f1); err != nil {
			return nil, false, err
		}

		if err = f1.Validate(); err != nil {
			return nil, false, err
		}

		return NewFSV2(), true, nil
	case V2:
		if err = f.Validate(); err != nil {
			return nil, false, err
		}

		return &f, false, nil
	}

	return nil, false, fmt.Errorf("unknown FS version %v", f.FS.Version)
}
