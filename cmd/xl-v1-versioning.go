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

package cmd

import (
	"fmt"
	"path"
	"time"

	"crypto/sha1"
	"encoding/base64"
)

// The maximum number of versions that Minio supports for each object
const MaxVersionsLimit = 100

type xlObjectVersion struct {
	Id           string    `json:"id"`                     // Object version id
	Postfix      string    `json:"postfix,omitempty"`  	   // Postfix for path of object (2nd version and beyond)
	DeleteMarker bool      `json:"deleteMarker,omitempty"` // Delete marker for this version
	TimeStamp    time.Time `json:"timeStamp"`              // Timestamp for this version
}

// DeriveVersionId derives a pseudo-random, yet deterministic, versionId
// It is meant to generate identical versionIds across replicated buckets
func (m xlMetaV1) DeriveVersionId(object, etag string) (id, postfix string, err error) {

	// Check that we are not about to create more versions than allowed
	if len(m.ObjectVersions) >= MaxVersionsLimit {
		err = TooManyVersions{}
		return
	}

	h := sha1.New()
	// Derive hash from concatenation of the base of the key name of object, an index and the etag
	// Note that the etag can be empty for delete markers
	s := fmt.Sprintf("%s;%d;%s", path.Base(object), len(m.ObjectVersions)+1, etag)
	h.Write([]byte(s))
	bts := h.Sum(nil)

	id = base64.RawURLEncoding.EncodeToString(bts)

	// For every version after the initial version,
	// derive a postfix extension for the name where to store this version
	if len(m.ObjectVersions) > 0 {
		postfix = "-versionId-" + id
	}
	return
}

// FindVersion gets the corresponding index of a version (if any)
func (m xlMetaV1) FindVersion(version string) (idx int, postfix string, found bool) {
	idx = len(m.ObjectVersions)
	for i, vo := range m.ObjectVersions {
		if version == vo.Id {
			idx, postfix, found = i, vo.Postfix, true
			return
		}
	}
	return
}

// list of all errors that can be ignored in a versioning operation.
var objVersioningOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound, errFileAccessDenied, errCorruptedFormat)
