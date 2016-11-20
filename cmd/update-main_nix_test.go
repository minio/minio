// +build linux darwin dragonfly freebsd netbsd openbsd

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// Validate when release versions are properly set.
func TestReleaseUpdateVersion(t *testing.T) {
	Version = "2016-10-06T00:08:32Z"
	ReleaseTag = "RELEASE.2016-10-06T00-08-32Z"
	CommitID = "d1c38ba8f0b3aecdf9b932c087dd65c21eebac33"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z")
	}))
	userAgentSuffix = "Minio/" + Version + " " + "Minio/" + ReleaseTag + " " + "Minio/" + CommitID
	defer ts.Close()
	testCases := []struct {
		updateURL  string
		updateMsg  updateMessage
		errMsg     string
		shouldPass bool
	}{
		{
			updateURL: ts.URL,
			updateMsg: updateMessage{
				Download:  ts.URL + "/" + runtime.GOOS + "-" + runtime.GOARCH + "/minio",
				Update:    true,
				NewerThan: 90487000000000,
			},
			errMsg:     "",
			shouldPass: true,
		},
	}

	// Validates all the errors reported.
	for i, testCase := range testCases {
		updateMsg, errMsg, err := getReleaseUpdate(testCase.updateURL, time.Second*1)
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d: Unable to fetch release update %s", i+1, err)
		}
		if errMsg != testCase.errMsg {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.errMsg, errMsg)
		}
		if !reflect.DeepEqual(updateMsg, testCase.updateMsg) {
			t.Errorf("Test %d: Expected %#v, got %#v", i+1, testCase.updateMsg, updateMsg)
		}
	}
}

func TestReleaseUpdate(t *testing.T) {
	Version = "DEVELOPMENT.GOGET"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()
	testCases := []struct {
		updateURL  string
		updateMsg  updateMessage
		errMsg     string
		shouldPass bool
	}{
		{
			updateURL: ts.URL,
			updateMsg: updateMessage{
				Download: ts.URL + "/" + runtime.GOOS + "-" + runtime.GOARCH + "/minio",
			},
			errMsg:     "Failed to retrieve update notice. Please try again later. Please report this issue at https://github.com/minio/minio/issues",
			shouldPass: false,
		},
	}

	// Validates all the errors reported.
	for i, testCase := range testCases {
		updateMsg, errMsg, err := getReleaseUpdate(testCase.updateURL, time.Second*1)
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d: Unable to fetch release update %s", i+1, err)
		}
		if errMsg != testCase.errMsg {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.errMsg, errMsg)
		}
		if !reflect.DeepEqual(updateMsg, testCase.updateMsg) {
			t.Errorf("Test %d: Expected %#v, got %#v", i+1, testCase.updateMsg, updateMsg)
		}
	}
}
