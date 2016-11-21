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
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"net/url"
	"runtime"
	"testing"
)

// Tests storage error transformation.
func TestStorageErr(t *testing.T) {
	unknownErr := errors.New("Unknown error")
	testCases := []struct {
		expectedErr error
		err         error
	}{
		{
			expectedErr: nil,
			err:         nil,
		},
		{
			expectedErr: io.EOF,
			err:         fmt.Errorf("%s", io.EOF.Error()),
		},
		{
			expectedErr: io.ErrUnexpectedEOF,
			err:         fmt.Errorf("%s", io.ErrUnexpectedEOF.Error()),
		},
		{
			expectedErr: errDiskNotFound,
			err:         &net.OpError{},
		},
		{
			expectedErr: rpc.ErrShutdown,
			err:         rpc.ErrShutdown,
		},
		{
			expectedErr: errUnexpected,
			err:         fmt.Errorf("%s", errUnexpected.Error()),
		},
		{
			expectedErr: errDiskFull,
			err:         fmt.Errorf("%s", errDiskFull.Error()),
		},
		{
			expectedErr: errVolumeNotFound,
			err:         fmt.Errorf("%s", errVolumeNotFound.Error()),
		},
		{
			expectedErr: errVolumeExists,
			err:         fmt.Errorf("%s", errVolumeExists.Error()),
		},
		{
			expectedErr: errFileNotFound,
			err:         fmt.Errorf("%s", errFileNotFound.Error()),
		},
		{
			expectedErr: errFileAccessDenied,
			err:         fmt.Errorf("%s", errFileAccessDenied.Error()),
		},
		{
			expectedErr: errIsNotRegular,
			err:         fmt.Errorf("%s", errIsNotRegular.Error()),
		},
		{
			expectedErr: errVolumeNotEmpty,
			err:         fmt.Errorf("%s", errVolumeNotEmpty.Error()),
		},
		{
			expectedErr: errVolumeAccessDenied,
			err:         fmt.Errorf("%s", errVolumeAccessDenied.Error()),
		},
		{
			expectedErr: errCorruptedFormat,
			err:         fmt.Errorf("%s", errCorruptedFormat.Error()),
		},
		{
			expectedErr: errUnformattedDisk,
			err:         fmt.Errorf("%s", errUnformattedDisk.Error()),
		},
		{
			expectedErr: errFileNameTooLong,
			err:         fmt.Errorf("%s", errFileNameTooLong.Error()),
		},
		{
			expectedErr: errInvalidAccessKeyID,
			err:         fmt.Errorf("%s", errInvalidAccessKeyID.Error()),
		},
		{
			expectedErr: errAuthentication,
			err:         fmt.Errorf("%s", errAuthentication.Error()),
		},
		{
			expectedErr: errServerVersionMismatch,
			err:         fmt.Errorf("%s", errServerVersionMismatch.Error()),
		},
		{
			expectedErr: errServerTimeMismatch,
			err:         fmt.Errorf("%s", errServerTimeMismatch.Error()),
		},
		{
			expectedErr: unknownErr,
			err:         unknownErr,
		},
	}
	for i, testCase := range testCases {
		resultErr := toStorageErr(testCase.err)
		if testCase.expectedErr != resultErr {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, resultErr)
		}
	}
}

// API suite container common to both FS and XL.
type TestRPCStorageSuite struct {
	serverType  string
	testServer  TestServer
	remoteDisks []StorageAPI
}

// Setting up the test suite.
// Starting the Test server with temporary FS backend.
func (s *TestRPCStorageSuite) SetUpSuite(c *testing.T) {
	s.testServer = StartTestStorageRPCServer(c, s.serverType, 1)
	listenAddress := s.testServer.Server.Listener.Addr().String()

	for _, ep := range s.testServer.Disks {
		ep.Host = listenAddress
		storageDisk, err := newStorageRPC(ep)
		if err != nil {
			c.Fatal("Unable to initialize RPC client", err)
		}
		s.remoteDisks = append(s.remoteDisks, storageDisk)
	}
	_, err := newStorageRPC(nil)
	if err != errInvalidArgument {
		c.Fatalf("Unexpected error %s, expecting %s", err, errInvalidArgument)
	}
	u, err := url.Parse("http://abcd:abcd123@localhost/mnt/disk")
	if err != nil {
		c.Fatal("Unexpected error", err)
	}
	_, err = newStorageRPC(u)
	if err != nil {
		c.Fatal("Unexpected error", err)
	}
}

// No longer used with gocheck, but used in explicit teardown code in
// each test function. // Called implicitly by "gopkg.in/check.v1"
// after all tests are run.
func (s *TestRPCStorageSuite) TearDownSuite(c *testing.T) {
	s.testServer.Stop()
}

func TestRPCStorageClient(t *testing.T) {
	// Setup code
	s := &TestRPCStorageSuite{serverType: "XL"}
	s.SetUpSuite(t)

	// Run the test.
	s.testRPCStorageClient(t)

	// Teardown code
	s.TearDownSuite(t)
}

func (s *TestRPCStorageSuite) testRPCStorageClient(t *testing.T) {
	// TODO - Fix below tests to run on windows.
	if runtime.GOOS == "windows" {
		return
	}
	s.testRPCStorageDisksInfo(t)
	s.testRPCStorageVolOps(t)
	s.testRPCStorageFileOps(t)
	s.testRPCStorageListDir(t)
}

// Test storage disks info.
func (s *TestRPCStorageSuite) testRPCStorageDisksInfo(t *testing.T) {
	for _, storageDisk := range s.remoteDisks {
		diskInfo, err := storageDisk.DiskInfo()
		if err != nil {
			t.Error("Unable to initiate DiskInfo", err)
		}
		if diskInfo.Total == 0 {
			t.Error("Invalid diskInfo total")
		}
		if storageDisk.String() == "" {
			t.Error("String representation of storageAPI should not be empty")
		}
	}
}

// Test storage vol operations.
func (s *TestRPCStorageSuite) testRPCStorageVolOps(t *testing.T) {
	for _, storageDisk := range s.remoteDisks {
		numVols := 0
		err := storageDisk.MakeVol("myvol")
		if err != nil {
			t.Error("Unable to initiate MakeVol", err)
		}
		numVols++
		volInfo, err := storageDisk.StatVol("myvol")
		if err != nil {
			t.Error("Unable to initiate StatVol", err)
		}
		if volInfo.Name != "myvol" {
			t.Errorf("Expected `myvol` found %s instead", volInfo.Name)
		}
		if volInfo.Created.IsZero() {
			t.Error("Expected created time to be non zero")
		}

		for i := 0; i < 10; i++ {
			err = storageDisk.MakeVol(fmt.Sprintf("myvol-%d", i))
			if err != nil {
				t.Error("Unable to initiate MakeVol", err)
			}
			numVols++
		}
		vols, err := storageDisk.ListVols()
		if err != nil {
			t.Error("Unable to initiate ListVol")
		}
		if len(vols) != numVols {
			t.Errorf("Expected %d volumes but found only %d", numVols, len(vols))
		}

		for i := 0; i < 10; i++ {
			err = storageDisk.DeleteVol(fmt.Sprintf("myvol-%d", i))
			if err != nil {
				t.Error("Unable to initiate DeleteVol", err)
			}
		}

		err = storageDisk.DeleteVol("myvol")
		if err != nil {
			t.Error("Unable to initiate DeleteVol", err)
		}
		vols, err = storageDisk.ListVols()
		if err != nil {
			t.Error("Unable to initiate ListVol")
		}
		if len(vols) > 0 {
			t.Errorf("Expected no volumes but found %d", len(vols))
		}
	}
}

// Tests all file operations.
func (s *TestRPCStorageSuite) testRPCStorageFileOps(t *testing.T) {
	for _, storageDisk := range s.remoteDisks {
		err := storageDisk.MakeVol("myvol")
		if err != nil {
			t.Error("Unable to initiate MakeVol", err)
		}
		err = storageDisk.PrepareFile("myvol", "file1", int64(len([]byte("Hello, world"))))
		if err != nil {
			t.Error("Unable to initiate AppendFile", err)
		}
		err = storageDisk.AppendFile("myvol", "file1", []byte("Hello, world"))
		if err != nil {
			t.Error("Unable to initiate AppendFile", err)
		}
		fi, err := storageDisk.StatFile("myvol", "file1")
		if err != nil {
			t.Error("Unable to initiate StatFile", err)
		}
		if fi.Name != "file1" {
			t.Errorf("Expected `file1` but got %s", fi.Name)
		}
		if fi.Volume != "myvol" {
			t.Errorf("Expected `myvol` but got %s", fi.Volume)
		}
		if fi.Size != 12 {
			t.Errorf("Expected 12 but got %d", fi.Size)
		}
		if !fi.Mode.IsRegular() {
			t.Error("Expected file to be regular found", fi.Mode)
		}
		if fi.ModTime.IsZero() {
			t.Error("Expected created time to be non zero")
		}
		buf, err := storageDisk.ReadAll("myvol", "file1")
		if err != nil {
			t.Error("Unable to initiate ReadAll", err)
		}
		if !bytes.Equal(buf, []byte("Hello, world")) {
			t.Errorf("Expected `Hello, world`, got %s", string(buf))
		}
		buf1 := make([]byte, 5)
		n, err := storageDisk.ReadFile("myvol", "file1", 4, buf1)
		if err != nil {
			t.Error("Unable to initiate ReadFile", err)
		}
		if n != 5 {
			t.Errorf("Expected `5`, got %d", n)
		}
		if !bytes.Equal(buf[4:9], buf1) {
			t.Errorf("Expected %s, got %s", string(buf[4:9]), string(buf1))
		}
		err = storageDisk.RenameFile("myvol", "file1", "myvol", "file2")
		if err != nil {
			t.Error("Unable to initiate RenameFile", err)
		}
		err = storageDisk.DeleteFile("myvol", "file2")
		if err != nil {
			t.Error("Unable to initiate DeleteFile", err)
		}
		err = storageDisk.DeleteVol("myvol")
		if err != nil {
			t.Error("Unable to initiate DeleteVol", err)
		}
	}
}

// Tests for ListDirHandler.
func (s *TestRPCStorageSuite) testRPCStorageListDir(t *testing.T) {
	for _, storageDisk := range s.remoteDisks {
		err := storageDisk.MakeVol("myvol")
		if err != nil {
			t.Error("Unable to initiate MakeVol", err)
		}
		dirCount := 10
		for i := 0; i < dirCount; i++ {
			err = storageDisk.MakeVol(fmt.Sprintf("myvol/mydir-%d", i))
			if err != nil {
				t.Error("Unable to initiate MakeVol", err)
			}
		}
		dirs, err := storageDisk.ListDir("myvol", "")
		if len(dirs) != dirCount {
			t.Errorf("Expected %d directories but found only %d", dirCount, len(dirs))
		}
		for i := 0; i < dirCount; i++ {
			err = storageDisk.DeleteVol(fmt.Sprintf("myvol/mydir-%d", i))
			if err != nil {
				t.Error("Unable to initiate DeleteVol", err)
			}
		}
		dirs, err = storageDisk.ListDir("myvol", "")
		if len(dirs) != 0 {
			t.Errorf("Expected no directories but found %d", dirCount)
		}

		err = storageDisk.DeleteVol("myvol")
		if err != nil {
			t.Error("Unable to initiate DeleteVol", err)
		}
		vols, err := storageDisk.ListVols()
		if len(vols) != 0 {
			t.Errorf("Expected no volumes but found %d", dirCount)
		}
	}
}
