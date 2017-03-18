/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestGetCurrentReleaseTime(t *testing.T) {
	minioVersion1 := UTCNow().Format(time.RFC3339)
	releaseTime1, _ := time.Parse(time.RFC3339, minioVersion1)

	minioVersion2 := "DEVELOPMENT.GOGET"
	tmpfile1, err := ioutil.TempFile("", "get-current-release-time-testcase-1")
	if err != nil {
		t.Fatalf("Unable to create temporary file. %s", err)
	}
	defer os.Remove(tmpfile1.Name())

	minioBinaryPath2 := tmpfile1.Name()
	fi, err := tmpfile1.Stat()
	if err != nil {
		t.Fatalf("Unable to get temporary file info. %s", err)
	}
	if err = tmpfile1.Close(); err != nil {
		t.Fatalf("Unable to create temporary file. %s", err)
	}
	releaseTime2 := fi.ModTime().UTC()

	minioBinaryPath3 := "go"
	if runtime.GOOS == globalWindowsOSName {
		minioBinaryPath3 = "go.exe"
	}
	goBinAbsPath, err := exec.LookPath(minioBinaryPath3)
	if err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat(goBinAbsPath)
	if err != nil {
		t.Fatal(err)
	}
	releaseTime3 := fi.ModTime().UTC()

	// Get a non-absolute binary path.
	minioBinaryPath4 := filepath.Base(tmpfile1.Name())

	// Get a non-existent absolute binary path
	minioBinaryPath5 := "/tmp/non-existent-file"
	if runtime.GOOS == globalWindowsOSName {
		minioBinaryPath5 = "C:\\tmp\\non-existent-file"
	}
	errorMessage1 := "exec: \"\": executable file not found in $PATH"
	if runtime.GOOS == globalWindowsOSName {
		errorMessage1 = "exec: \"\": executable file not found in %PATH%"
	}
	errorMessage2 := "exec: \"non-existent-file\": executable file not found in $PATH"
	if runtime.GOOS == globalWindowsOSName {
		errorMessage2 = "exec: \"non-existent-file\": executable file not found in %PATH%"
	}
	errorMessage3 := fmt.Sprintf("exec: \"%s\": executable file not found in $PATH", minioBinaryPath4)
	if runtime.GOOS == globalWindowsOSName {
		errorMessage3 = "exec: \"" + minioBinaryPath4 + "\": executable file not found in %PATH%"
	}
	errorMessage4 := "Unable to get ModTime of /tmp/non-existent-file. stat /tmp/non-existent-file: no such file or directory"
	if runtime.GOOS == "windows" {
		errorMessage4 = "Unable to get ModTime of C:\\tmp\\non-existent-file. GetFileAttributesEx C:\\tmp\\non-existent-file: The system cannot find the path specified."
	}

	testCases := []struct {
		minioVersion    string
		minioBinaryPath string
		expectedResult  time.Time
		expectedErr     error
	}{
		{minioVersion1, "", releaseTime1, nil},
		{minioVersion1, minioBinaryPath2, releaseTime1, nil},
		{minioVersion2, minioBinaryPath2, releaseTime2, nil},
		{minioVersion2, minioBinaryPath3, releaseTime3, nil},
		{"junk", minioBinaryPath2, releaseTime2, nil},
		{"3.2.0", minioBinaryPath2, releaseTime2, nil},
		{minioVersion2, "", time.Time{}, errors.New(errorMessage1)},
		{"junk", "non-existent-file", time.Time{}, errors.New(errorMessage2)},
		{"3.2.0", "non-existent-file", time.Time{}, errors.New(errorMessage2)},
		{minioVersion2, minioBinaryPath4, time.Time{}, errors.New(errorMessage3)},
		{minioVersion2, minioBinaryPath5, time.Time{}, errors.New(errorMessage4)},
	}

	if runtime.GOOS == "linux" {
		testCases = append(testCases, struct {
			minioVersion    string
			minioBinaryPath string
			expectedResult  time.Time
			expectedErr     error
		}{"3.2a", "/proc/1/cwd", time.Time{}, errors.New("Unable to get ModTime of /proc/1/cwd. stat /proc/1/cwd: permission denied")})
	}

	for _, testCase := range testCases {
		result, err := getCurrentReleaseTime(testCase.minioVersion, testCase.minioBinaryPath)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		}

		if !testCase.expectedResult.Equal(result) {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

func TestIsDocker(t *testing.T) {
	createTempFile := func(content string) string {
		tmpfile, err := ioutil.TempFile("", "isdocker-testcase")
		if err != nil {
			t.Fatalf("Unable to create temporary file. %s", err)
		}
		if _, err = tmpfile.Write([]byte(content)); err != nil {
			t.Fatalf("Unable to create temporary file. %s", err)
		}
		if err = tmpfile.Close(); err != nil {
			t.Fatalf("Unable to create temporary file. %s", err)
		}
		return tmpfile.Name()
	}

	filename1 := createTempFile(`11:pids:/user.slice/user-1000.slice/user@1000.service
10:blkio:/
9:hugetlb:/
8:perf_event:/
7:cpuset:/
6:devices:/user.slice
5:net_cls,net_prio:/
4:cpu,cpuacct:/
3:memory:/user/bala/0
2:freezer:/user/bala/0
1:name=systemd:/user.slice/user-1000.slice/user@1000.service/gnome-terminal-server.service
`)
	defer os.Remove(filename1)
	filename2 := createTempFile(`14:name=systemd:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
13:pids:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
12:hugetlb:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
11:net_prio:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
10:perf_event:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
9:net_cls:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
8:freezer:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
7:devices:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
6:memory:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
5:blkio:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
4:cpuacct:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
3:cpu:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
2:cpuset:/docker/d5eb950884d828237f60f624ff575a1a7a4daa28a8d4d750040527ed9545e727
1:name=openrc:/docker
`)
	defer os.Remove(filename2)

	testCases := []struct {
		filename       string
		expectedResult bool
		expectedErr    error
	}{
		{"", false, nil},
		{"/tmp/non-existing-file", false, nil},
		{filename1, false, nil},
		{filename2, true, nil},
	}

	if runtime.GOOS == "linux" {
		testCases = append(testCases, struct {
			filename       string
			expectedResult bool
			expectedErr    error
		}{"/proc/1/cwd", false, fmt.Errorf("open /proc/1/cwd: permission denied")})
	}

	for _, testCase := range testCases {
		result, err := isDocker(testCase.filename)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		}

		if testCase.expectedResult != result {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

func TestIsSourceBuild(t *testing.T) {
	testCases := []struct {
		minioVersion   string
		expectedResult bool
	}{
		{UTCNow().Format(time.RFC3339), false},
		{"DEVELOPMENT.GOGET", true},
		{"junk", true},
		{"3.2.4", true},
	}

	for _, testCase := range testCases {
		result := isSourceBuild(testCase.minioVersion)
		if testCase.expectedResult != result {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

func TestDownloadReleaseData(t *testing.T) {
	httpServer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer httpServer1.Close()
	httpServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z")
	}))
	defer httpServer2.Close()
	httpServer3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "", http.StatusNotFound)
	}))
	defer httpServer3.Close()

	testCases := []struct {
		releaseChecksumURL string
		expectedResult     string
		expectedErr        error
	}{
		{httpServer1.URL, "", nil},
		{httpServer2.URL, "fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n", nil},
		{httpServer3.URL, "", fmt.Errorf("Error downloading URL " + httpServer3.URL + ". Response: 404 Not Found")},
	}

	for _, testCase := range testCases {
		result, err := downloadReleaseData(testCase.releaseChecksumURL, 1*time.Second, "")
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		}

		if testCase.expectedResult != result {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

func TestParseReleaseData(t *testing.T) {
	releaseTime, _ := time.Parse(minioReleaseTagTimeLayout, "2016-10-07T01-16-39Z")
	testCases := []struct {
		data           string
		expectedResult time.Time
		expectedErr    error
	}{
		{"more than two fields", time.Time{}, fmt.Errorf("Unknown release data `more than two fields`")},
		{"more than", time.Time{}, fmt.Errorf("Unknown release information `than`")},
		{"more than.two.fields", time.Time{}, fmt.Errorf("Unknown release 'than.two.fields'")},
		{"more minio.RELEASE.fields", time.Time{}, fmt.Errorf(`Unknown release time format. parsing time "fields" as "2006-01-02T15-04-05Z": cannot parse "fields" as "2006"`)},
		{"more minio.RELEASE.2016-10-07T01-16-39Z", releaseTime, nil},
		{"fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n", releaseTime, nil},
	}

	for _, testCase := range testCases {
		result, err := parseReleaseData(testCase.data)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected: %v, got: %v", testCase.expectedErr, err)
		}

		if !testCase.expectedResult.Equal(result) {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}
