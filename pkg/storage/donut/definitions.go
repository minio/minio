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

import "time"

// objectMetadata object specific metadata per object
type objectMetadata struct {
	// version
	Version string `json:"version"`

	// object metadata
	Size    int64     `json:"size"`
	Created time.Time `json:"created"`
	Bucket  string    `json:"bucket"`
	Object  string    `json:"object"`

	// checksums
	MD5Sum    string `json:"md5sum"`
	SHA512Sum string `json:"sha512sum"`

	// additional metadata
	Metadata map[string]string `json:"metadata"`
}

// donutObjectMetadata container for donut specific internal metadata per object
type donutObjectMetadata struct {
	// version
	Version string `json:"version"`

	// erasure
	DataDisks        uint8  `json:"sys.erasureK"`
	ParityDisks      uint8  `json:"sys.erasureM"`
	ErasureTechnique string `json:"sys.erasureTechnique"`

	// object metadata
	Size       int64 `json:"sys.size"`
	BlockSize  int   `json:"sys.blockSize"`
	ChunkCount int   `json:"sys.chunkCount"`

	// checksums
	MD5Sum    string `json:"sys.md5sum"`
	SHA512Sum string `json:"sys.sha512sum"`
}

// donutMetadata container for donut level metadata
type donutMetadata struct {
	Version string `json:"version"`
}

// bucketMetadata container for bucket level metadata
type bucketMetadata struct {
	Version  string            `json:"version"`
	ACL      string            `json:"acl"`
	Metadata map[string]string `json:"metadata"`
}
