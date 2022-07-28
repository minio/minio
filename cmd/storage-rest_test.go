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
	"context"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"

	"github.com/gorilla/mux"
	xnet "github.com/minio/pkg/net"
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
		_, err := storage.DiskInfo(context.Background())
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
		if err != errUnformattedDisk {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, errUnformattedDisk, err)
		}
	}
}

func testStorageAPIMakeVol(t *testing.T, storage StorageAPI) {
	testCases := []struct {
		volumeName string
		expectErr  bool
	}{
		{"foo", false},
		// volume exists error.
		{"foo", true},
	}

	for i, testCase := range testCases {
		err := storage.MakeVol(context.Background(), testCase.volumeName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIListVols(t *testing.T, storage StorageAPI) {
	testCases := []struct {
		volumeNames    []string
		expectedResult []VolInfo
		expectErr      bool
	}{
		{nil, []VolInfo{{Name: ".minio.sys"}}, false},
		{[]string{"foo"}, []VolInfo{{Name: ".minio.sys"}, {Name: "foo"}}, false},
	}

	for i, testCase := range testCases {
		for _, volumeName := range testCase.volumeNames {
			err := storage.MakeVol(context.Background(), volumeName)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
		}

		result, err := storage.ListVols(context.Background())
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if len(result) != len(testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %+v, got: %+v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testStorageAPIStatVol(t *testing.T, storage StorageAPI) {
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		expectErr  bool
	}{
		{"foo", false},
		// volume not found error.
		{"bar", true},
	}

	for i, testCase := range testCases {
		result, err := storage.StatVol(context.Background(), testCase.volumeName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result.Name != testCase.volumeName {
				t.Fatalf("case %v: result: expected: %+v, got: %+v", i+1, testCase.volumeName, result.Name)
			}
		}
	}
}

func testStorageAPIDeleteVol(t *testing.T, storage StorageAPI) {
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		expectErr  bool
	}{
		{"foo", false},
		// volume not found error.
		{"bar", true},
	}

	for i, testCase := range testCases {
		err := storage.DeleteVol(context.Background(), testCase.volumeName, false)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIStatInfoFile(t *testing.T, storage StorageAPI) {
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile(context.Background(), "foo", pathJoin("myobject", xlStorageFormatFile), []byte("foo"))
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
		_, err := storage.StatInfoFile(context.Background(), testCase.volumeName, testCase.objectName+"/"+xlStorageFormatFile, false)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v, err: %v", i+1, expectErr, testCase.expectErr, err)
		}
	}
}

func testStorageAPIListDir(t *testing.T, storage StorageAPI) {
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile(context.Background(), "foo", "path/to/myobject", []byte("foo"))
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
		result, err := storage.ListDir(context.Background(), testCase.volumeName, testCase.prefix, -1)
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
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile(context.Background(), "foo", "myobject", []byte("foo"))
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
		result, err := storage.ReadAll(context.Background(), testCase.volumeName, testCase.objectName)
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
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile(context.Background(), "foo", "myobject", []byte("foo"))
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
		_, err := storage.ReadFile(context.Background(), testCase.volumeName, testCase.objectName, testCase.offset, result, nil)
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
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

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
		{"bar", "myobject", testData, true, false},
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
		err := storage.AppendFile(context.Background(), testCase.volumeName, testCase.objectName, testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			data, err := storage.ReadAll(context.Background(), testCase.volumeName, testCase.objectName)
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
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile(context.Background(), "foo", "myobject", []byte("foo"))
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
		err := storage.Delete(context.Background(), testCase.volumeName, testCase.objectName, DeleteOptions{
			Recursive: false,
			Force:     false,
		})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIRenameFile(t *testing.T, storage StorageAPI) {
	err := storage.MakeVol(context.Background(), "foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.MakeVol(context.Background(), "bar")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile(context.Background(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile(context.Background(), "foo", "otherobject", []byte("foo"))
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
		err := storage.RenameFile(context.Background(), testCase.volumeName, testCase.objectName, testCase.destVolumeName, testCase.destObjectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func newStorageRESTHTTPServerClient(t *testing.T) *storageRESTClient {
	prevHost, prevPort := globalMinioHost, globalMinioPort
	defer func() {
		globalMinioHost, globalMinioPort = prevHost, prevPort
	}()

	router := mux.NewRouter()
	httpServer := httptest.NewServer(router)
	t.Cleanup(httpServer.Close)

	url, err := xnet.ParseHTTPURL(httpServer.URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	url.Path = t.TempDir()

	globalMinioHost, globalMinioPort = mustSplitHostPort(url.Host)

	endpoint, err := NewEndpoint(url.String())
	if err != nil {
		t.Fatalf("NewEndpoint failed %v", endpoint)
	}

	if err = endpoint.UpdateIsLocal(); err != nil {
		t.Fatalf("UpdateIsLocal failed %v", err)
	}

	registerStorageRESTHandlers(router, []PoolEndpoints{{
		Endpoints: Endpoints{endpoint},
	}})

	restClient := newStorageRESTClient(endpoint, false)

	return restClient
}

func TestStorageRESTClientDiskInfo(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDiskInfo(t, restClient)
}

func TestStorageRESTClientMakeVol(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIMakeVol(t, restClient)
}

func TestStorageRESTClientListVols(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIListVols(t, restClient)
}

func TestStorageRESTClientStatVol(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIStatVol(t, restClient)
}

func TestStorageRESTClientDeleteVol(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDeleteVol(t, restClient)
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
