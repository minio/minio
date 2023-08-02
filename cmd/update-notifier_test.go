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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/color"
)

// Tests update notifier string builder.
func TestPrepareUpdateMessage(t *testing.T) {
	testCases := []struct {
		older time.Duration
		dlURL string

		expectedSubStr string
	}{
		// Testcase index 0
		{72 * time.Hour, "my_download_url", "3 days before the latest release"},
		{3 * time.Hour, "https://my_download_url_is_huge/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "3 hours before the latest release"},
		{-72 * time.Hour, "another_update_url", ""},
		{0, "another_update_url", ""},
		{time.Hour, "", ""},
		{0 * time.Second, "my_download_url", "now"},
		{1 * time.Second, "my_download_url", "1 second before the latest release"},
		{37 * time.Second, "my_download_url", "37 seconds before the latest release"},
		{60 * time.Second, "my_download_url", "1 minute before the latest release"},
		{61 * time.Second, "my_download_url", "1 minute before the latest release"},

		// Testcase index 10
		{37 * time.Minute, "my_download_url", "37 minutes before the latest release"},
		{1 * time.Hour, "my_download_url", "1 hour before the latest release"},
		{61 * time.Minute, "my_download_url", "1 hour before the latest release"},
		{122 * time.Minute, "my_download_url", "2 hours before the latest release"},
		{24 * time.Hour, "my_download_url", "1 day before the latest release"},
		{25 * time.Hour, "my_download_url", "1 day before the latest release"},
		{49 * time.Hour, "my_download_url", "2 days before the latest release"},
		{7 * 24 * time.Hour, "my_download_url", "1 week before the latest release"},
		{8 * 24 * time.Hour, "my_download_url", "1 week before the latest release"},
		{15 * 24 * time.Hour, "my_download_url", "2 weeks before the latest release"},

		// Testcase index 20
		{30 * 24 * time.Hour, "my_download_url", "1 month before the latest release"},
		{31 * 24 * time.Hour, "my_download_url", "1 month before the latest release"},
		{61 * 24 * time.Hour, "my_download_url", "2 months before the latest release"},
		{360 * 24 * time.Hour, "my_download_url", "1 year before the latest release"},
		{361 * 24 * time.Hour, "my_download_url", "1 year before the latest release"},
		{2 * 365 * 24 * time.Hour, "my_download_url", "2 years before the latest release"},
	}

	plainMsg := "You are running an older version of MinIO released"

	for i, testCase := range testCases {
		output := prepareUpdateMessage(testCase.dlURL, testCase.older)
		line1 := fmt.Sprintf("%s %s", plainMsg, color.YellowBold(testCase.expectedSubStr))
		line2 := fmt.Sprintf("Update: %s", color.CyanBold(testCase.dlURL))
		// Uncomment below to see message appearance:
		// fmt.Println(output)
		switch {
		case testCase.dlURL == "" && output != "":
			t.Errorf("Testcase %d: no newer release available but got an update message: %s", i+1, output)
		case output == "" && testCase.dlURL != "" && testCase.older > 0:
			t.Errorf("Testcase %d: newer release is available but got empty update message!", i+1)
		case output == "" && (testCase.dlURL == "" || testCase.older <= 0):
			// Valid no update message case. No further
			// validation needed.
			continue
		case !strings.Contains(output, line1):
			t.Errorf("Testcase %d: output '%s' did not contain line 1: '%s'", i+1, output, line1)
		case !strings.Contains(output, line2):
			t.Errorf("Testcase %d: output '%s' did not contain line 2: '%s'", i+1, output, line2)
		}
	}
}
