/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"os"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// VolInfo - represents volume stat information.
type VolInfo struct {
	// Name of the volume.
	Name string

	// Date and time when the volume was created.
	Created time.Time
}

// FilesInfo represent a list of files, additionally
// indicates if the list is last.
type FilesInfo struct {
	Files       []FileInfo
	IsTruncated bool
}

// FileInfo - represents file stat information.
type FileInfo struct {
	// Name of the volume.
	Volume string

	// Name of the file.
	Name string

	// Date and time when the file was last modified.
	ModTime time.Time

	// Total file size.
	Size int64

	// File mode bits.
	Mode os.FileMode

	// File metadata
	Metadata map[string]string

	// All the parts per object.
	Parts []ObjectPartInfo

	Quorum int
}

// ToObjectInfo converts FileInfo into objectInfo.
func (entry FileInfo) ToObjectInfo() ObjectInfo {
	var objInfo ObjectInfo
	if HasSuffix(entry.Name, SlashSeparator) {
		objInfo = ObjectInfo{
			Bucket: entry.Volume,
			Name:   entry.Name,
			IsDir:  true,
		}
	} else {
		objInfo = ObjectInfo{
			IsDir:           false,
			Bucket:          entry.Volume,
			Name:            entry.Name,
			ModTime:         entry.ModTime,
			Size:            entry.Size,
			ContentType:     entry.Metadata["content-type"],
			ContentEncoding: entry.Metadata["content-encoding"],
		}

		// Extract etag from metadata.
		objInfo.ETag = extractETag(entry.Metadata)

		// All the parts per object.
		objInfo.Parts = entry.Parts

		// etag/md5Sum has already been extracted. We need to
		// remove to avoid it from appearing as part of
		// response headers. e.g, X-Minio-* or X-Amz-*.
		objInfo.UserDefined = cleanMetadata(entry.Metadata)

		// Update storage class
		if sc, ok := entry.Metadata[xhttp.AmzStorageClass]; ok {
			objInfo.StorageClass = sc
		} else {
			objInfo.StorageClass = globalMinioDefaultStorageClass
		}
	}
	return objInfo
}
