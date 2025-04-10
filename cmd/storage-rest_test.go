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
	"bytes"
	"errors"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/minio/minio/internal/grid"
	xnet "github.com/minio/pkg/v3/net"
)

// Storage REST server, storageRESTReceiver and StorageRESTClient are
// inter-dependent, below test functions are sufficient to test all of them.
func testStorageAPIDiskInfo(t *testing.T, storage StorageAPI) {
	testCases := []struct {
		expectErr bool
	}{
		{true},
	}

	for i, testCase := range testCases {
		_, err := storage.DiskInfo(t.Context(), DiskInfoOptions{Metrics: true})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
		if err != errUnformattedDisk {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, errUnformattedDisk, err)
		}
	}
}

func testStorageAPIStatInfoFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", pathJoin("myobject", xlStorageFormatFile), []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		expectErr  bool
	}{
		{"foo", "myobject", false},
		// file not found error.
		{"foo", "yourobject", true},
	}

	for i, testCase := range testCases {
		_, err := storage.StatInfoFile(t.Context(), testCase.volumeName, testCase.objectName+"/"+xlStorageFormatFile, false)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v, err: %v", i+1, expectErr, testCase.expectErr, err)
		}
	}
}

func testStorageAPIListDir(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "path/to/myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		prefix         string
		expectedResult []string
		expectErr      bool
	}{
		{"foo", "path", []string{"to/"}, false},
		// prefix not found error.
		{"foo", "nodir", nil, true},
	}

	for i, testCase := range testCases {
		result, err := storage.ListDir(t.Context(), "", testCase.volumeName, testCase.prefix, -1)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testStorageAPIReadAll(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		expectedResult []byte
		expectErr      bool
	}{
		{"foo", "myobject", []byte("foo"), false},
		// file not found error.
		{"foo", "yourobject", nil, true},
	}

	for i, testCase := range testCases {
		result, err := storage.ReadAll(t.Context(), testCase.volumeName, testCase.objectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func testStorageAPIReadFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		offset         int64
		expectedResult []byte
		expectErr      bool
	}{
		{"foo", "myobject", 0, []byte("foo"), false},
		{"foo", "myobject", 1, []byte("oo"), false},
		// file not found error.
		{"foo", "yourobject", 0, nil, true},
	}

	result := make([]byte, 100)
	for i, testCase := range testCases {
		result = result[testCase.offset:3]
		_, err := storage.ReadFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.offset, result, nil)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func testStorageAPIAppendFile(t *testing.T, storage StorageAPI) {
	testData := []byte("foo")
	testCases := []struct {
		volumeName      string
		objectName      string
		data            []byte
		expectErr       bool
		ignoreIfWindows bool
	}{
		{"foo", "myobject", testData, false, false},
		{"foo", "myobject-0byte", []byte{}, false, false},
		// volume not found error.
		{"foo-bar", "myobject", testData, true, false},
		// Test some weird characters over the wire.
		{"foo", "newline\n", testData, false, true},
		{"foo", "newline\t", testData, false, true},
		{"foo", "newline \n", testData, false, true},
		{"foo", "newline$$$\n", testData, false, true},
		{"foo", "newline%%%\n", testData, false, true},
		{"foo", "newline \t % $ & * ^ # @ \n", testData, false, true},
		{"foo", "\n\tnewline \t % $ & * ^ # @ \n", testData, false, true},
	}

	for i, testCase := range testCases {
		if testCase.ignoreIfWindows && runtime.GOOS == "windows" {
			continue
		}
		err := storage.AppendFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			data, err := storage.ReadAll(t.Context(), testCase.volumeName, testCase.objectName)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data, testCase.data) {
				t.Fatalf("case %v: expected %v, got %v", i+1, testCase.data, data)
			}
		}
	}
}

func testStorageAPIDeleteFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		expectErr  bool
	}{
		{"foo", "myobject", false},
		// file not found not returned
		{"foo", "myobject", false},
		// file not found not returned
		{"foo", "yourobject", false},
	}

	for i, testCase := range testCases {
		err := storage.Delete(t.Context(), testCase.volumeName, testCase.objectName, DeleteOptions{
			Recursive: false,
			Immediate: false,
		})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIRenameFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile(t.Context(), "foo", "otherobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		destVolumeName string
		destObjectName string
		expectErr      bool
	}{
		{"foo", "myobject", "foo", "yourobject", false},
		{"foo", "yourobject", "bar", "myobject", false},
		// overwrite.
		{"foo", "otherobject", "bar", "myobject", false},
	}

	for i, testCase := range testCases {
		err := storage.RenameFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.destVolumeName, testCase.destObjectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func newStorageRESTHTTPServerClient(t testing.TB) *storageRESTClient {
	// Grid with 2 hosts
	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatalf("SetupTestGrid: %v", err)
	}
	t.Cleanup(tg.Cleanup)
	prevHost, prevPort := globalMinioHost, globalMinioPort
	defer func() {
		globalMinioHost, globalMinioPort = prevHost, prevPort
	}()
	// tg[0] = local, tg[1] = remote

	// Remote URL
	url, err := xnet.ParseHTTPURL(tg.Servers[1].URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	url.Path = t.TempDir()

	globalMinioHost, globalMinioPort = mustSplitHostPort(url.Host)
	globalNodeAuthToken, _ = authenticateNode(globalActiveCred.AccessKey, globalActiveCred.SecretKey)

	endpoint, err := NewEndpoint(url.String())
	if err != nil {
		t.Fatalf("NewEndpoint failed %v", endpoint)
	}

	if err = endpoint.UpdateIsLocal(); err != nil {
		t.Fatalf("UpdateIsLocal failed %v", err)
	}

	endpoint.PoolIdx = 0
	endpoint.SetIdx = 0
	endpoint.DiskIdx = 0

	poolEps := []PoolEndpoints{{
		Endpoints: Endpoints{endpoint},
	}}
	poolEps[0].SetCount = 1
	poolEps[0].DrivesPerSet = 1

	// Register handlers on newly created servers
	registerStorageRESTHandlers(tg.Mux[0], poolEps, tg.Managers[0])
	registerStorageRESTHandlers(tg.Mux[1], poolEps, tg.Managers[1])

	storage := globalLocalSetDrives[0][0][0]
	if err = storage.MakeVol(t.Context(), "foo"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err = storage.MakeVol(t.Context(), "bar"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	restClient, err := newStorageRESTClient(endpoint, false, tg.Managers[0])
	if err != nil {
		t.Fatal(err)
	}

	for {
		_, err := restClient.DiskInfo(t.Context(), DiskInfoOptions{})
		if err == nil || errors.Is(err, errUnformattedDisk) {
			break
		}
		time.Sleep(time.Duration(rand.Float64() * float64(100*time.Millisecond)))
	}

	return restClient
}

func TestStorageRESTClientDiskInfo(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDiskInfo(t, restClient)
}

func TestStorageRESTClientStatInfoFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIStatInfoFile(t, restClient)
}

func TestStorageRESTClientListDir(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIListDir(t, restClient)
}

func TestStorageRESTClientReadAll(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIReadAll(t, restClient)
}

func TestStorageRESTClientReadFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIReadFile(t, restClient)
}

func TestStorageRESTClientAppendFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIAppendFile(t, restClient)
}

func TestStorageRESTClientDeleteFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDeleteFile(t, restClient)
}

func TestStorageRESTClientRenameFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIRenameFile(t, restClient)
}
