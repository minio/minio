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
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/madmin-go"
)

// Tests if we generate storage info.
func TestStorageInfoMsg(t *testing.T) {
	infoStorage := StorageInfo{}
	infoStorage.Disks = []madmin.Disk{
		{Endpoint: "http://127.0.0.1:9000/data/1/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9000/data/2/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9000/data/3/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9000/data/4/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9001/data/1/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9001/data/2/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9001/data/3/", State: madmin.DriveStateOk},
		{Endpoint: "http://127.0.0.1:9001/data/4/", State: madmin.DriveStateOffline},
	}
	infoStorage.Backend.Type = madmin.Erasure

	if msg := getStorageInfoMsg(infoStorage); !strings.Contains(msg, "7 Online, 1 Offline") {
		t.Fatal("Unexpected storage info message, found:", msg)
	}
}

// Tests stripping standard ports from apiEndpoints.
func TestStripStandardPorts(t *testing.T) {
	apiEndpoints := []string{"http://127.0.0.1:9000", "http://127.0.0.2:80", "https://127.0.0.3:443"}
	expectedAPIEndpoints := []string{"http://127.0.0.1:9000", "http://127.0.0.2", "https://127.0.0.3"}
	newAPIEndpoints := stripStandardPorts(apiEndpoints, "")

	if !reflect.DeepEqual(expectedAPIEndpoints, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", expectedAPIEndpoints, newAPIEndpoints)
	}

	apiEndpoints = []string{"http://%%%%%:9000"}
	newAPIEndpoints = stripStandardPorts(apiEndpoints, "")
	if !reflect.DeepEqual([]string{""}, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", apiEndpoints, newAPIEndpoints)
	}

	apiEndpoints = []string{"http://127.0.0.1:443", "https://127.0.0.1:80"}
	newAPIEndpoints = stripStandardPorts(apiEndpoints, "")
	if !reflect.DeepEqual(apiEndpoints, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", apiEndpoints, newAPIEndpoints)
	}
}

// Test printing server common message.
func TestPrintServerCommonMessage(t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printServerCommonMsg(apiEndpoints)
}

// Tests print cli access message.
func TestPrintCLIAccessMsg(t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printCLIAccessMsg(apiEndpoints[0], "myminio")
}

// Test print startup message.
func TestPrintStartupMessage(t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printStartupMessage(apiEndpoints, nil)
}
