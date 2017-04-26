/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fatih/color"
)

// Tests update notifier string builder.
func TestUpdateNotifier(t *testing.T) {
	plainMsg := "You are running an older version of Minio released "
	colorMsg := plainMsg
	yellow := color.New(color.FgYellow, color.Bold).SprintfFunc()
	if runtime.GOOS == "windows" {
		plainMsg += "3 days from now"
		colorMsg += yellow("3 days from now")
	} else {
		plainMsg += "2 days from now"
		colorMsg += yellow("2 days from now")
	}

	updateMsg := colorizeUpdateMessage(minioReleaseURL, time.Duration(72*time.Hour))

	if !(strings.Contains(updateMsg, plainMsg) || strings.Contains(updateMsg, colorMsg)) {
		t.Fatal("Duration string not found in colorized update message", updateMsg)
	}

	if !strings.Contains(updateMsg, minioReleaseURL) {
		t.Fatal("Update message not found in colorized update message", minioReleaseURL)
	}
}
