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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestMinioVersionToReleaseTime(t *testing.T) {
	testCases := []struct {
		version    string
		isOfficial bool
	}{
		{"2017-09-29T19:16:56Z", true},
		{"RELEASE.2017-09-29T19-16-56Z", false},
		{"DEVELOPMENT.GOGET", false},
	}
	for i, testCase := range testCases {
		_, err := minioVersionToReleaseTime(testCase.version)
		if (err == nil) != testCase.isOfficial {
			t.Errorf("Test %d: Expected %v but got %v",
				i+1, testCase.isOfficial, err == nil)
		}
	}
}

func TestReleaseTagToNFromTimeConversion(t *testing.T) {
	utcLoc, _ := time.LoadLocation("")
	testCases := []struct {
		t      time.Time
		tag    string
		errStr string
	}{
		{time.Date(2017, time.September, 29, 19, 16, 56, 0, utcLoc),
			"RELEASE.2017-09-29T19-16-56Z", ""},
		{time.Date(2017, time.August, 5, 0, 0, 53, 0, utcLoc),
			"RELEASE.2017-08-05T00-00-53Z", ""},
		{time.Now().UTC(), "2017-09-29T19:16:56Z",
			"2017-09-29T19:16:56Z is not a valid release tag"},
		{time.Now().UTC(), "DEVELOPMENT.GOGET",
			"DEVELOPMENT.GOGET is not a valid release tag"},
	}
	for i, testCase := range testCases {
		if testCase.errStr != "" {
			got := releaseTimeToReleaseTag(testCase.t)
			if got != testCase.tag && testCase.errStr == "" {
				t.Errorf("Test %d: Expected %v but got %v", i+1, testCase.tag, got)
			}
		}
		tagTime, err := releaseTagToReleaseTime(testCase.tag)
		if err != nil && err.Error() != testCase.errStr {
			t.Errorf("Test %d: Expected %v but got %v", i+1, testCase.errStr, err.Error())
		}
		if err == nil && tagTime != testCase.t {
			t.Errorf("Test %d: Expected %v but got %v", i+1, testCase.t, tagTime)
		}
	}

}

func TestDownloadURL(t *testing.T) {
	minioVersion1 := releaseTimeToReleaseTag(UTCNow())
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
		result, err := downloadReleaseURL(testCase.releaseChecksumURL, 1*time.Second, "")
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
	releaseTime, _ := releaseTagToReleaseTime("RELEASE.2016-10-07T01-16-39Z")
	testCases := []struct {
		data           string
		expectedResult time.Time
		expectedErr    error
	}{
		{"more than two fields", time.Time{}, fmt.Errorf("Unknown release data `more than two fields`")},
		{"more than", time.Time{}, fmt.Errorf("Unknown release information `than`")},
		{"more than.two.fields", time.Time{}, fmt.Errorf("Unknown release `than.two.fields`")},
		{"more minio.RELEASE.fields", time.Time{}, fmt.Errorf(`Unknown release tag format. parsing time "fields" as "2006-01-02T15-04-05Z": cannot parse "fields" as "2006"`)},
		{"more minio.RELEASE.2016-10-07T01-16-39Z", releaseTime, nil},
		{"fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z\n", releaseTime, nil},
	}

	for i, testCase := range testCases {
		result, err := parseReleaseData(testCase.data)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Errorf("error case %d: expected: %v, got: %v", i+1, testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Errorf("error case %d: expected: %v, got: %v", i+1, testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Errorf("error case %d: expected: %v, got: %v", i+1, testCase.expectedErr, err)
		}

		if !testCase.expectedResult.Equal(result) {
			t.Errorf("case %d: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
