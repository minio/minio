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
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio/cmd/logger"
)

// markerTagVersion is the marker version.
// Should not need to be updated unless a fundamental change is made to the marker format.
const markerTagVersion = "v1"

// parseMarker will parse a marker possibly encoded with encodeMarker
func parseMarker(s string) (marker, uuid string) {
	if !strings.Contains(s, "[minio_cache:"+markerTagVersion) {
		return s, ""
	}
	start := strings.LastIndex(s, "[")
	marker = s[:start]
	end := strings.LastIndex(s, "]")
	tag := strings.Trim(s[start:end], "[]")
	tags := strings.Split(tag, ",")
	for _, tag := range tags {
		kv := strings.Split(tag, ":")
		if len(kv) < 2 {
			continue
		}
		switch kv[0] {
		case "minio_cache":
			if kv[1] != markerTagVersion {
				break
			}
		case "id":
			uuid = kv[1]
		default:
			// Ignore unknown
		}
	}
	return
}

// encodeMarker will encode a uuid and return it as a marker.
// uuid cannot contain '[', ':' or ','.
func encodeMarker(marker, uuid string) string {
	if uuid == "" {
		return marker
	}
	if strings.ContainsAny(uuid, "[:,") {
		logger.LogIf(context.Background(), fmt.Errorf("encodeMarker: uuid %s contained invalid characters", uuid))
	}
	return fmt.Sprintf("%s[minio_cache:%s,id:%s]", marker, markerTagVersion, uuid)
}
