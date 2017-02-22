/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"strings"
	"testing"
	"time"
)

// Tests update notifier string builder.
func TestUpdateNotifier(t *testing.T) {
	colorUpdateMsg := colorizeUpdateMessage(minioReleaseURL, time.Duration(72*time.Hour))
	if !strings.Contains(colorUpdateMsg, "minutes") {
		t.Fatal("Duration string not found in colorized update message", colorUpdateMsg)
	}
	if !strings.Contains(colorUpdateMsg, minioReleaseURL) {
		t.Fatal("Update message not found in colorized update message", minioReleaseURL)
	}
}
