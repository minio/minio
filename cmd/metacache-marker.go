// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio/internal/logger"
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
