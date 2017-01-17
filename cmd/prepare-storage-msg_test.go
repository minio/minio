/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"net/url"
	"reflect"
	"testing"
)

// Tests and validates the output for heal endpoint.
func TestGetHealEndpoint(t *testing.T) {
	// Test for a SSL scheme.
	tls := true
	hURL := getHealEndpoint(tls, &url.URL{
		Scheme: httpScheme,
		Host:   "localhost:9000",
	})
	sHURL := &url.URL{
		Scheme: httpsScheme,
		Host:   "localhost:9000",
	}
	if !reflect.DeepEqual(hURL, sHURL) {
		t.Fatalf("Expected %#v, but got %#v", sHURL, hURL)
	}

	// Test a non-TLS scheme.
	tls = false
	hURL = getHealEndpoint(tls, &url.URL{
		Scheme: httpsScheme,
		Host:   "localhost:9000",
	})
	sHURL = &url.URL{
		Scheme: httpScheme,
		Host:   "localhost:9000",
	}
	if !reflect.DeepEqual(hURL, sHURL) {
		t.Fatalf("Expected %#v, but got %#v", sHURL, hURL)
	}

	// FIXME(GLOBAL): purposefully Host is left empty because
	// we need to bring in safe handling on global values
	// add a proper test case here once that happens.
	/*
		tls = false
		hURL = getHealEndpoint(tls, &url.URL{
			Path: "/export",
		})
		sHURL = &url.URL{
			Scheme: httpScheme,
			Host:   "",
		}
		globalMinioAddr = ""
		if !reflect.DeepEqual(hURL, sHURL) {
			t.Fatalf("Expected %#v, but got %#v", sHURL, hURL)
		}
	*/
}

// Tests heal message to be correct and properly formatted.
func TestHealMsg(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Unable to initialize test config", err)
	}
	defer removeAll(rootPath)
	storageDisks, fsDirs := prepareXLStorageDisks(t)
	errs := make([]error, len(storageDisks))
	defer removeRoots(fsDirs)
	nilDisks := deepCopyStorageDisks(storageDisks)
	nilDisks[5] = nil
	authErrs := make([]error, len(storageDisks))
	authErrs[5] = errAuthentication
	endpointURL, err := url.Parse("http://10.1.10.1:9000")
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	endpointURLs := make([]*url.URL, len(storageDisks))
	for idx := 0; idx < len(endpointURLs); idx++ {
		endpointURLs[idx] = endpointURL
	}

	testCases := []struct {
		endPoints    []*url.URL
		storageDisks []StorageAPI
		serrs        []error
	}{
		// Test - 1 for valid disks and errors.
		{
			endPoints:    endpointURLs,
			storageDisks: storageDisks,
			serrs:        errs,
		},
		// Test - 2 for one of the disks is nil.
		{
			endPoints:    endpointURLs,
			storageDisks: nilDisks,
			serrs:        errs,
		},
		// Test - 3 for one of the errs is authentication.
		{
			endPoints:    endpointURLs,
			storageDisks: nilDisks,
			serrs:        authErrs,
		},
	}
	for i, testCase := range testCases {
		msg := getHealMsg(testCase.endPoints, testCase.storageDisks)
		if msg == "" {
			t.Fatalf("Test: %d Unable to get heal message.", i+1)
		}
		msg = getStorageInitMsg("init", testCase.endPoints, testCase.storageDisks)
		if msg == "" {
			t.Fatalf("Test: %d Unable to get regular message.", i+1)
		}
		msg = getConfigErrMsg(testCase.storageDisks, testCase.serrs)
		if msg == "" {
			t.Fatalf("Test: %d Unable to get config error message.", i+1)
		}
	}
}

// Tests disk info, validates if we do return proper disk info structure
// even in case of certain disks not available.
func TestDisksInfo(t *testing.T) {
	storageDisks, fsDirs := prepareXLStorageDisks(t)
	defer removeRoots(fsDirs)

	testCases := []struct {
		storageDisks []StorageAPI
		onlineDisks  int
		offlineDisks int
	}{
		{
			storageDisks: storageDisks,
			onlineDisks:  16,
			offlineDisks: 0,
		},
		{
			storageDisks: prepareNOfflineDisks(deepCopyStorageDisks(storageDisks), 4, t),
			onlineDisks:  12,
			offlineDisks: 4,
		},
		{
			storageDisks: prepareNOfflineDisks(deepCopyStorageDisks(storageDisks), 16, t),
			onlineDisks:  0,
			offlineDisks: 16,
		},
	}

	for i, testCase := range testCases {
		_, onlineDisks, offlineDisks := getDisksInfo(testCase.storageDisks)
		if testCase.onlineDisks != onlineDisks {
			t.Errorf("Test %d: Expected online disks %d, got %d", i+1, testCase.onlineDisks, onlineDisks)
		}
		if testCase.offlineDisks != offlineDisks {
			t.Errorf("Test %d: Expected offline disks %d, got %d", i+1, testCase.offlineDisks, offlineDisks)
		}
	}

}
