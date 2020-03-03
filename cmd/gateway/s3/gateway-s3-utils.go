/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package s3

import (
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/bucket/object/tagging"
)

// List of header keys to be filtered, usually
// from all S3 API http responses.
var defaultFilterKeys = []string{
	"Connection",
	"Transfer-Encoding",
	"Accept-Ranges",
	"Date",
	"Server",
	"Vary",
	"x-amz-bucket-region",
	"x-amz-request-id",
	"x-amz-id-2",
	"Content-Security-Policy",
	"X-Xss-Protection",

	// Add new headers to be ignored.
}

// FromGatewayObjectPart converts ObjectInfo for custom part stored as object to PartInfo
func FromGatewayObjectPart(partID int, oi minio.ObjectInfo) (pi minio.PartInfo) {
	return minio.PartInfo{
		Size:         oi.Size,
		ETag:         minio.CanonicalizeETag(oi.ETag),
		LastModified: oi.ModTime,
		PartNumber:   partID,
	}
}

func getTagMap(tagStr string) (map[string]string, error) {
	var tags tagging.Tagging
	var err error
	if tagStr == "" {
		return nil, nil
	}
	if tags, err = tagging.FromString(tagStr); err != nil {
		return nil, err
	}
	return tagging.ToMap(tags), nil
}

func getTagMapParse(tagStr string) (map[string]string, error) {
	var tags *tagging.Tagging
	var err error
	if tagStr == "" {
		return nil, nil
	}
	if tags, err = tagging.ParseTagging(strings.NewReader(tagStr)); err != nil {
		return nil, err
	}
	if tags == nil {
		return nil, nil
	}
	return tagging.ToMap(*tags), nil
}
