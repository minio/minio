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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fatih/color"
)

// Tests update notifier string builder.
func TestPrepareUpdateMessage(t *testing.T) {
	testCases := []struct {
		older time.Duration
		dlURL string

		expectedSubStr string
	}{
		// Testcase index 0
		{72 * time.Hour, "my_download_url", "3 days ago"},
		{3 * time.Hour, "https://my_download_url_is_huge/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "3 hours ago"},
		{-72 * time.Hour, "another_update_url", ""},
		{0, "another_update_url", ""},
		{time.Hour, "", ""},
		{1 * time.Second, "my_download_url", "now"},
		{2 * time.Second, "my_download_url", "1 second ago"},
		{37 * time.Second, "my_download_url", "37 seconds ago"},
		{60 * time.Second, "my_download_url", "60 seconds ago"},
		{61 * time.Second, "my_download_url", "1 minute ago"},

		// Testcase index 10
		{37 * time.Minute, "my_download_url", "37 minutes ago"},
		{1 * time.Hour, "my_download_url", "60 minutes ago"},
		{61 * time.Minute, "my_download_url", "1 hour ago"},
		{122 * time.Minute, "my_download_url", "2 hours ago"},
		{24 * time.Hour, "my_download_url", "24 hours ago"},
		{25 * time.Hour, "my_download_url", "1 day ago"},
		{49 * time.Hour, "my_download_url", "2 days ago"},
		{7 * 24 * time.Hour, "my_download_url", "7 days ago"},
		{8 * 24 * time.Hour, "my_download_url", "1 week ago"},
		{15 * 24 * time.Hour, "my_download_url", "2 weeks ago"},

		// Testcase index 20
		{30 * 24 * time.Hour, "my_download_url", "4 weeks ago"},
		{31 * 24 * time.Hour, "my_download_url", "1 month ago"},
		{61 * 24 * time.Hour, "my_download_url", "2 months ago"},
		{360 * 24 * time.Hour, "my_download_url", "12 months ago"},
		{361 * 24 * time.Hour, "my_download_url", "1 year ago"},
		{2 * 365 * 24 * time.Hour, "my_download_url", "2 years ago"},
	}

	plainMsg := "You are running an older version of Minio released"
	yellow := color.New(color.FgYellow, color.Bold).SprintfFunc()
	cyan := color.New(color.FgCyan, color.Bold).SprintFunc()

	for i, testCase := range testCases {
		output := prepareUpdateMessage(testCase.dlURL, testCase.older)
		line1 := fmt.Sprintf("%s %s", plainMsg, yellow(testCase.expectedSubStr))
		line2 := fmt.Sprintf("Update: %s", cyan(testCase.dlURL))
		// Uncomment below to see message appearance:
		// fmt.Println(output)
		switch {
		case testCase.dlURL == "" && output != "":
			t.Errorf("Testcase %d: no newer release available but got an update message: %s", i, output)
		case output == "" && testCase.dlURL != "" && testCase.older > 0:
			t.Errorf("Testcase %d: newer release is available but got empty update message!", i)
		case output == "" && (testCase.dlURL == "" || testCase.older <= 0):
			// Valid no update message case. No further
			// validation needed.
			continue
		case !strings.Contains(output, line1):
			t.Errorf("Testcase %d: output '%s' did not contain line 1: '%s'", i, output, line1)
		case !strings.Contains(output, line2):
			t.Errorf("Testcase %d: output '%s' did not contain line 2: '%s'", i, output, line2)
		}
	}
}
