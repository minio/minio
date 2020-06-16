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
 *
 */

package kv

import (
	"path"
	"strings"
)

// KeyTransform transform
// Return null to skip key
// Return key transformed or as it is
type KeyTransform func(prefix string, key string) string

// KeyTransformTrimJSON trims prefix and tailing .json.
func KeyTransformTrimJSON(prefix string, key string) string {
	return path.Clean(strings.TrimSuffix(strings.TrimPrefix(key, prefix), ".json"))
}

// KeyTransformDefault trims prefix
func KeyTransformDefault(prefix string, key string) string {
	return path.Clean(strings.TrimSuffix(strings.TrimPrefix(key, prefix), path.Base(key)))
}
