/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"io/ioutil"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/config"
	xnet "github.com/minio/minio/pkg/net"
)

///////////////////////////////////////////////////////////////////////////////
//
// Storage REST server, storageRESTReceiver and StorageRESTClient are
// inter-dependent, below test functions are sufficient to test all of them.
//
///////////////////////////////////////////////////////////////////////////////

func testStorageAPIDiskInfo(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	testCases := []struct {
		expectErr bool
	}{
		{false},
	}

	for i, testCase := range testCases {
		_, err := storage.DiskInfo()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIMakeVol(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	testCases := []struct {
		volumeName string
		expectErr  bool
	}{
		{"foo", false},
		// volume exists error.
		{"foo", true},
	}

	for i, testCase := range testCases {
		err := storage.MakeVol(testCase.volumeName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIListVols(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	testCases := []struct {
		volumeNames    []string
		expectedResult []VolInfo
		expectErr      bool
	}{
		{nil, []VolInfo{}, false},
		{[]string{"foo"}, []VolInfo{{Name: "foo"}}, false},
	}

	for i, testCase := range testCases {
		for _, volumeName := range testCase.volumeNames {
			err := storage.MakeVol(volumeName)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
		}

		result, err := storage.ListVols()
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
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
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
		result, err := storage.StatVol(testCase.volumeName)
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
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
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
		err := storage.DeleteVol(testCase.volumeName, false)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPICheckFile(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile("foo", pathJoin("myobject", xlStorageFormatFile), []byte("foo"))
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
		err := storage.CheckFile(testCase.volumeName, testCase.objectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIListDir(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile("foo", "path/to/myobject", []byte("foo"))
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
		result, err := storage.ListDir(testCase.volumeName, testCase.prefix, -1)
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
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile("foo", "myobject", []byte("foo"))
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
		result, err := storage.ReadAll(testCase.volumeName, testCase.objectName)
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
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	err = storage.AppendFile("foo", "myobject", []byte("foo"))
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
		_, err := storage.ReadFile(testCase.volumeName, testCase.objectName, testCase.offset, result, nil)
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
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		data       []byte
		expectErr  bool
	}{
		{"foo", "myobject", []byte("foo"), false},
		{"foo", "myobject", []byte{}, false},
		// volume not found error.
		{"bar", "myobject", []byte{}, true},
	}

	for i, testCase := range testCases {
		err := storage.AppendFile(testCase.volumeName, testCase.objectName, testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIDeleteFile(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile("foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		expectErr  bool
	}{
		{"foo", "myobject", false},
		// should removed by above case.
		{"foo", "myobject", true},
		// file not found error
		{"foo", "yourobject", true},
	}

	for i, testCase := range testCases {
		err := storage.DeleteFile(testCase.volumeName, testCase.objectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIRenameFile(t *testing.T, storage StorageAPI) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	err := storage.MakeVol("foo")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.MakeVol("bar")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile("foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile("foo", "otherobject", []byte("foo"))
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
		err := storage.RenameFile(testCase.volumeName, testCase.objectName, testCase.destVolumeName, testCase.destObjectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func newStorageRESTHTTPServerClient(t *testing.T) (*httptest.Server, *storageRESTClient, config.Config, string) {
	prevHost, prevPort := globalMinioHost, globalMinioPort
	defer func() {
		globalMinioHost, globalMinioPort = prevHost, prevPort
	}()

	endpointPath, err := ioutil.TempDir("", ".TestStorageREST.")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	router := mux.NewRouter()
	httpServer := httptest.NewServer(router)

	url, err := xnet.ParseHTTPURL(httpServer.URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	url.Path = endpointPath

	globalMinioHost, globalMinioPort = mustSplitHostPort(url.Host)

	endpoint, err := NewEndpoint(url.String())
	if err != nil {
		t.Fatalf("NewEndpoint failed %v", endpoint)
	}

	if err = endpoint.UpdateIsLocal(); err != nil {
		t.Fatalf("UpdateIsLocal failed %v", err)
	}

	registerStorageRESTHandlers(router, []ZoneEndpoints{{
		Endpoints: Endpoints{endpoint},
	}})

	restClient := newStorageRESTClient(endpoint)
	prevGlobalServerConfig := globalServerConfig
	globalServerConfig = newServerConfig()

	return httpServer, restClient, prevGlobalServerConfig, endpointPath
}

func TestStorageRESTClientDiskInfo(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIDiskInfo(t, restClient)
}

func TestStorageRESTClientMakeVol(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIMakeVol(t, restClient)
}

func TestStorageRESTClientListVols(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIListVols(t, restClient)
}

func TestStorageRESTClientStatVol(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIStatVol(t, restClient)
}

func TestStorageRESTClientDeleteVol(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIDeleteVol(t, restClient)
}

func TestStorageRESTClientCheckFile(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPICheckFile(t, restClient)
}

func TestStorageRESTClientListDir(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIListDir(t, restClient)
}

func TestStorageRESTClientReadAll(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIReadAll(t, restClient)
}

func TestStorageRESTClientReadFile(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIReadFile(t, restClient)
}

func TestStorageRESTClientAppendFile(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIAppendFile(t, restClient)
}

func TestStorageRESTClientDeleteFile(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIDeleteFile(t, restClient)
}

func TestStorageRESTClientRenameFile(t *testing.T) {
	httpServer, restClient, prevGlobalServerConfig, endpointPath := newStorageRESTHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()
	defer os.RemoveAll(endpointPath)

	testStorageAPIRenameFile(t, restClient)
}
