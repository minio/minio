/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
)

func getFileInfo(xlMetaBuf []byte, volume, path, versionID string) (FileInfo, error) {
	version, meta, err := xlMetaUnmarshalJSON(xlMetaBuf)
	if err != nil {
		return FileInfo{}, err
	}
	switch version {
	case xlMetaVersion100, xlMetaVersion101:
		m, ok := meta.(*xlMetaV1)
		if !ok {
			return FileInfo{}, errFileCorrupt
		}
		if !m.Valid() {
			return FileInfo{}, errFileCorrupt
		}
		return m.ToFileInfo(volume, path), nil
	case xlMetaVersion:
		m, ok := meta.(*xlMetaV2)
		if !ok {
			return FileInfo{}, errFileCorrupt
		}
		if !m.Valid() {
			return FileInfo{}, errFileCorrupt
		}
		return m.ToFileInfo(volume, path, versionID), nil
	default:
		return FileInfo{}, errFileCorrupt
	}
}

// Constructs meta using `jsoniter` lib, additionally uses
// gjson to validate the version and format.
func xlMetaUnmarshalJSON(xlMetaBuf []byte) (version string, meta interface{}, err error) {
	vr := gjson.GetBytes(xlMetaBuf, "version")
	switch vr.String() {
	default:
		return "", nil, errFileCorrupt
	case xlMetaVersion, xlMetaVersion101, xlMetaVersion100:
		// valid
		fr := gjson.GetBytes(xlMetaBuf, "format")
		if fr.String() != xlMetaFormat {
			return "", nil, errFileCorrupt
		}
	} // If we have reached here, its valid.

	switch vr.String() {
	case xlMetaVersion100, xlMetaVersion101:
		meta = &xlMetaV1{}
	case xlMetaVersion:
		meta = &xlMetaV2{}
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal(xlMetaBuf, meta)
	return vr.String(), meta, err
}
