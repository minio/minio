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
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestParseStorageClass(t *testing.T) {
	ExecObjectLayerTest(t, testParseStorageClass)
}

func testParseStorageClass(obj ObjectLayer, instanceType string, t TestErrHandler) {
	tests := []struct {
		name            int
		storageClassEnv string
		wantSc          storageClass
		expectedError   error
	}{
		{1, "EC:3", storageClass{
			Scheme: "EC",
			Parity: 3},
			nil},
		{2, "EC:4", storageClass{
			Scheme: "EC",
			Parity: 4},
			nil},
		{3, "AB:4", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Unsupported scheme AB. Supported scheme is EC")},
		{4, "EC:4:5", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Too many sections in EC:4:5")},
		{5, "AB", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Too few sections in AB")},
	}
	for _, tt := range tests {
		gotSc, err := parseStorageClass(tt.storageClassEnv)
		if err != nil && tt.expectedError == nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if err == nil && tt.expectedError != nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if tt.expectedError == nil && !reflect.DeepEqual(gotSc, tt.wantSc) {
			t.Errorf("Test %d, Expected %v, got %v", tt.name, tt.wantSc, gotSc)
			return
		}
		if tt.expectedError != nil && !reflect.DeepEqual(err, tt.expectedError) {
			t.Errorf("Test %d, Expected %v, got %v", tt.name, tt.expectedError, err)
		}
	}
}

func TestValidateRRSParity(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testValidateRRSParity)
}

func testValidateRRSParity(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	// Set globalEndpoints for a single node XL setup.
	globalEndpoints = mustGetNewEndpointList(dirs...)

	tests := []struct {
		name          int
		rrsParity     int
		ssParity      int
		expectedError error
	}{
		{1, 2, 4, nil},
		{2, 1, 4, errors.New("Reduced redundancy storage class parity should be greater than or equal to 2")},
		{3, 7, 6, errors.New("Reduced redundancy storage class parity disks should be less than 6")},
		{4, 9, 0, errors.New("Reduced redundancy storage class parity disks should be less than 8")},
		{5, 3, 3, errors.New("Reduced redundancy storage class parity disks should be less than 3")},
	}
	for _, tt := range tests {
		err := validateRRSParity(tt.rrsParity, tt.ssParity)
		if err != nil && tt.expectedError == nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if err == nil && tt.expectedError != nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if tt.expectedError != nil && !reflect.DeepEqual(err, tt.expectedError) {
			t.Errorf("Test %d, Expected %v, got %v", tt.name, tt.expectedError, err)
		}
	}
}

func TestValidateSSParity(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testValidateSSParity)
}

func testValidateSSParity(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	// Set globalEndpoints for a single node XL setup.
	globalEndpoints = mustGetNewEndpointList(dirs...)

	tests := []struct {
		name          int
		ssParity      int
		rrsParity     int
		expectedError error
	}{
		{1, 4, 2, nil},
		{2, 6, 5, nil},
		{3, 1, 0, errors.New("Standard storage class parity disks should be greater than or equal to 2")},
		{4, 4, 6, errors.New("Standard storage class parity disks should be greater than 6")},
		{5, 9, 0, errors.New("Standard storage class parity disks should be less than or equal to 8")},
		{6, 3, 3, errors.New("Standard storage class parity disks should be greater than 3")},
	}
	for _, tt := range tests {
		err := validateSSParity(tt.ssParity, tt.rrsParity)
		if err != nil && tt.expectedError == nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if err == nil && tt.expectedError != nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if tt.expectedError != nil && !reflect.DeepEqual(err, tt.expectedError) {
			t.Errorf("Test %d, Expected %v, got %v", tt.name, tt.expectedError, err)
		}
	}
}

func TestRedundancyCount(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testGetRedundancyCount)
}

func testGetRedundancyCount(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	xl := obj.(*xlObjects)

	tests := []struct {
		name           int
		sc             string
		disks          []StorageAPI
		expectedData   int
		expectedParity int
	}{
		{1, reducedRedundancyStorageClass, xl.storageDisks, 14, 2},
		{2, standardStorageClass, xl.storageDisks, 8, 8},
		{3, "", xl.storageDisks, 8, 8},
		{4, reducedRedundancyStorageClass, xl.storageDisks, 9, 7},
		{5, standardStorageClass, xl.storageDisks, 10, 6},
	}
	for _, tt := range tests {
		// Set env var for test case 4
		if tt.name == 4 {
			globalRRStorageClass.Parity = 7
		}
		// Set env var for test case 5
		if tt.name == 5 {
			globalStandardStorageClass.Parity = 6
		}
		data, parity := getRedundancyCount(tt.sc, len(tt.disks))
		if data != tt.expectedData {
			t.Errorf("Test %d, Expected data disks %d, got %d", tt.name, tt.expectedData, data)
			return
		}
		if parity != tt.expectedParity {
			t.Errorf("Test %d, Expected parity disks %d, got %d", tt.name, tt.expectedParity, parity)
			return
		}
	}
}

func TestObjectQuorumFromMeta(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testObjectQuorumFromMeta)
}

func testObjectQuorumFromMeta(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	bucket := getRandomBucketName()

	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), int(globalPutPartSize)*partCount)
	xl := obj.(*xlObjects)
	xlDisks := xl.storageDisks

	err := obj.MakeBucketWithLocation(bucket, globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	// Object for test case 1 - No StorageClass defined, no MetaData in PutObject
	object1 := "object1"
	_, err = obj.PutObject(bucket, object1, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), nil)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts1, errs1 := readAllXLMetadata(xlDisks, bucket, object1)

	// Object for test case 2 - No StorageClass defined, MetaData in PutObject requesting RRS Class
	object2 := "object2"
	metadata2 := make(map[string]string)
	metadata2["x-amz-storage-class"] = reducedRedundancyStorageClass
	_, err = obj.PutObject(bucket, object2, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata2)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts2, errs2 := readAllXLMetadata(xlDisks, bucket, object2)

	// Object for test case 3 - No StorageClass defined, MetaData in PutObject requesting Standard Storage Class
	object3 := "object3"
	metadata3 := make(map[string]string)
	metadata3["x-amz-storage-class"] = standardStorageClass
	_, err = obj.PutObject(bucket, object3, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata3)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts3, errs3 := readAllXLMetadata(xlDisks, bucket, object3)

	// Object for test case 4 - Standard StorageClass defined as Parity 6, MetaData in PutObject requesting Standard Storage Class
	object4 := "object4"
	metadata4 := make(map[string]string)
	metadata4["x-amz-storage-class"] = standardStorageClass
	globalStandardStorageClass = storageClass{
		Parity: 6,
		Scheme: "EC",
	}

	_, err = obj.PutObject(bucket, object4, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata4)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts4, errs4 := readAllXLMetadata(xlDisks, bucket, object4)

	// Object for test case 5 - RRS StorageClass defined as Parity 2, MetaData in PutObject requesting RRS Class
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	object5 := "object5"
	metadata5 := make(map[string]string)
	metadata5["x-amz-storage-class"] = reducedRedundancyStorageClass
	globalRRStorageClass = storageClass{
		Parity: 2,
		Scheme: "EC",
	}

	_, err = obj.PutObject(bucket, object5, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata5)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts5, errs5 := readAllXLMetadata(xlDisks, bucket, object5)

	// Object for test case 6 - RRS StorageClass defined as Parity 2, MetaData in PutObject requesting Standard Storage Class
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	object6 := "object6"
	metadata6 := make(map[string]string)
	metadata6["x-amz-storage-class"] = standardStorageClass
	globalRRStorageClass = storageClass{
		Parity: 2,
		Scheme: "EC",
	}

	_, err = obj.PutObject(bucket, object6, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata6)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts6, errs6 := readAllXLMetadata(xlDisks, bucket, object6)

	// Object for test case 7 - Standard StorageClass defined as Parity 5, MetaData in PutObject requesting RRS Class
	// Reset global storage class flags
	resetGlobalStorageEnvs()
	object7 := "object7"
	metadata7 := make(map[string]string)
	metadata7["x-amz-storage-class"] = reducedRedundancyStorageClass
	globalStandardStorageClass = storageClass{
		Parity: 5,
		Scheme: "EC",
	}

	_, err = obj.PutObject(bucket, object7, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), metadata7)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts7, errs7 := readAllXLMetadata(xlDisks, bucket, object7)

	tests := []struct {
		name                int
		xl                  xlObjects
		parts               []xlMetaV1
		errs                []error
		expectedReadQuorum  int
		expectedWriteQuorum int
		expectedError       error
	}{
		{1, *xl, parts1, errs1, 8, 9, nil},
		{2, *xl, parts2, errs2, 14, 15, nil},
		{3, *xl, parts3, errs3, 8, 9, nil},
		{4, *xl, parts4, errs4, 10, 11, nil},
		{5, *xl, parts5, errs5, 14, 15, nil},
		{6, *xl, parts6, errs6, 8, 9, nil},
		{7, *xl, parts7, errs7, 14, 15, nil},
	}
	for _, tt := range tests {
		actualReadQuorum, actualWriteQuorum, err := objectQuorumFromMeta(tt.xl, tt.parts, tt.errs)
		if tt.expectedError != nil && err == nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if tt.expectedError == nil && err != nil {
			t.Errorf("Test %d, Expected %s, got %s", tt.name, tt.expectedError, err)
			return
		}
		if tt.expectedReadQuorum != actualReadQuorum {
			t.Errorf("Test %d, Expected Read Quorum %d, got %d", tt.name, tt.expectedReadQuorum, actualReadQuorum)
			return
		}
		if tt.expectedWriteQuorum != actualWriteQuorum {
			t.Errorf("Test %d, Expected Write Quorum %d, got %d", tt.name, tt.expectedWriteQuorum, actualWriteQuorum)
			return
		}
	}
}

// Test isValidStorageClassMeta method with valid and invalid inputs
func TestIsValidStorageClassMeta(t *testing.T) {
	tests := []struct {
		name int
		sc   string
		want bool
	}{
		{1, "STANDARD", true},
		{2, "REDUCED_REDUNDANCY", true},
		{3, "", false},
		{4, "INVALID", false},
		{5, "123", false},
		{6, "MINIO_STORAGE_CLASS_RRS", false},
		{7, "MINIO_STORAGE_CLASS_STANDARD", false},
	}
	for _, tt := range tests {
		if got := isValidStorageClassMeta(tt.sc); got != tt.want {
			t.Errorf("Test %d, Expected Storage Class to be %t, got %t", tt.name, tt.want, got)
		}
	}
}
