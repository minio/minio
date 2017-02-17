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

func TestDownloadURL(t *testing.T) {
	minioVersion1 := UTCNow()
	durl := getDownloadURL(minioVersion1)
	if runtime.GOOS == "windows" {
		if durl != minioReleaseURL+"minio.exe" {
			t.Errorf("Expected %s, got %s", minioReleaseURL+"minio.exe", durl)
		}
	} else {
		if durl != minioReleaseURL+"minio" {
			t.Errorf("Expected %s, got %s", minioReleaseURL+"minio", durl)
		}
	}

	os.Setenv("KUBERNETES_SERVICE_HOST", "10.11.148.5")
	durl = getDownloadURL(minioVersion1)
	if durl != kubernetesDeploymentDoc {
		t.Errorf("Expected %s, got %s", kubernetesDeploymentDoc, durl)
	}
	os.Unsetenv("KUBERNETES_SERVICE_HOST")

	os.Setenv("MESOS_CONTAINER_NAME", "mesos-1111")
	durl = getDownloadURL(minioVersion1)
	if durl != mesosDeploymentDoc {
		t.Errorf("Expected %s, got %s", mesosDeploymentDoc, durl)
	}
	os.Unsetenv("MESOS_CONTAINER_NAME")
}

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
	fi, err = osStat(goBinAbsPath)
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
	if runtime.GOOS == globalWindowsOSName {
		errorMessage4 = "Unable to get ModTime of C:\\tmp\\non-existent-file. CreateFile C:\\tmp\\non-existent-file: The system cannot find the path specified."
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

// Tests user agent string.
func TestUserAgent(t *testing.T) {
	testCases := []struct {
		envName     string
		envValue    string
		mode        string
		expectedStr string
	}{
		{
			envName:     "",
			envValue:    "",
			mode:        globalMinioModeFS,
			expectedStr: fmt.Sprintf("Minio (%s; %s; %s; source) Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET", runtime.GOOS, runtime.GOARCH, globalMinioModeFS),
		},
		{
			envName:     "MESOS_CONTAINER_NAME",
			envValue:    "mesos-11111",
			mode:        globalMinioModeXL,
			expectedStr: fmt.Sprintf("Minio (%s; %s; %s; %s; source) Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET Minio/universe-%s", runtime.GOOS, runtime.GOARCH, globalMinioModeXL, "dcos", "mesos-1111"),
		},
		{
			envName:     "KUBERNETES_SERVICE_HOST",
			envValue:    "10.11.148.5",
			mode:        globalMinioModeXL,
			expectedStr: fmt.Sprintf("Minio (%s; %s; %s; %s; source) Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET Minio/DEVELOPMENT.GOGET", runtime.GOOS, runtime.GOARCH, globalMinioModeXL, "kubernetes"),
		},
	}

	for i, testCase := range testCases {
		os.Setenv(testCase.envName, testCase.envValue)
		if testCase.envName == "MESOS_CONTAINER_NAME" {
			os.Setenv("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION", "mesos-1111")
		}
		str := getUserAgent(testCase.mode)
		if str != testCase.expectedStr {
			t.Errorf("Test %d: expected: %s, got: %s", i+1, testCase.expectedStr, str)
		}
		os.Unsetenv("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION")
		os.Unsetenv(testCase.envName)
	}
}

// Tests if the environment we are running is in DCOS.
func TestIsDCOS(t *testing.T) {
	os.Setenv("MESOS_CONTAINER_NAME", "mesos-1111")
	dcos := IsDCOS()
	if !dcos {
		t.Fatalf("Expected %t, got %t", true, dcos)
	}

	os.Unsetenv("MESOS_CONTAINER_NAME")
	dcos = IsDCOS()
	if dcos {
		t.Fatalf("Expected %t, got %t", false, dcos)
	}
}

// Tests if the environment we are running is in kubernetes.
func TestIsKubernetes(t *testing.T) {
	os.Setenv("KUBERNETES_SERVICE_HOST", "10.11.148.5")
	kubernetes := IsKubernetes()
	if !kubernetes {
		t.Fatalf("Expected %t, got %t", true, kubernetes)
	}
	os.Unsetenv("KUBERNETES_SERVICE_HOST")
	kubernetes = IsKubernetes()
	if kubernetes {
		t.Fatalf("Expected %t, got %t", false, kubernetes)
	}
}

// Tests if the environment we are running is Helm chart.
func TestGetHelmVersion(t *testing.T) {
	createTempFile := func(content string) string {
		tmpfile, err := ioutil.TempFile("", "helm-testfile-")
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

	filename := createTempFile(
		`app="virtuous-rat-minio"
chart="minio-0.1.3"
heritage="Tiller"
pod-template-hash="818089471"`)

	defer os.Remove(filename)

	testCases := []struct {
		filename       string
		expectedResult string
	}{
		{"", ""},
		{"/tmp/non-existing-file", ""},
		{filename, "minio-0.1.3"},
	}

	for _, testCase := range testCases {
		result := getHelmVersion(testCase.filename)

		if testCase.expectedResult != result {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

// Tests if the environment we are running is in docker.
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

	filename := createTempFile("")
	defer os.Remove(filename)

	testCases := []struct {
		filename       string
		expectedResult bool
		expectedErr    error
	}{
		{"", false, nil},
		{"/tmp/non-existing-file", false, nil},
		{filename, true, nil},
	}

	if runtime.GOOS == "linux" {
		testCases = append(testCases, struct {
			filename       string
			expectedResult bool
			expectedErr    error
		}{"/proc/1/cwd", false, errors.New("stat /proc/1/cwd: permission denied")})
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
