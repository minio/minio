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
		storageClassEnv string
		wantSc          storageClass
		expectedError   error
	}{
		{"EC:3", storageClass{
			Scheme: "EC",
			Parity: 3},
			nil},
		{"EC:4", storageClass{
			Scheme: "EC",
			Parity: 4},
			nil},
		{"AB:4", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Unsupported scheme AB. Supported scheme is EC")},
		{"EC:4:5", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Too many sections in EC:4:5")},
		{"AB", storageClass{
			Scheme: "EC",
			Parity: 4},
			errors.New("Too few sections in AB")},
	}
	for i, tt := range tests {
		gotSc, err := parseStorageClass(tt.storageClassEnv)
		if err != nil && tt.expectedError == nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if err == nil && tt.expectedError != nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if tt.expectedError == nil && !reflect.DeepEqual(gotSc, tt.wantSc) {
			t.Errorf("Test %d, Expected %v, got %v", i+1, tt.wantSc, gotSc)
			return
		}
		if tt.expectedError != nil && !reflect.DeepEqual(err, tt.expectedError) {
			t.Errorf("Test %d, Expected %v, got %v", i+1, tt.expectedError, err)
		}
	}
}

func TestValidateParity(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testValidateParity)
}

func testValidateParity(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	// Reset global storage class flags
	resetGlobalStorageEnvs()

	// Set proper envs for a single node XL setup.
	saveIsXL := globalIsXL
	defer func() {
		globalIsXL = saveIsXL
	}()
	globalIsXL = true
	saveSetDriveCount := globalXLSetDriveCount
	defer func() {
		globalXLSetDriveCount = saveSetDriveCount
	}()
	globalXLSetCount = len(dirs)

	tests := []struct {
		rrsParity int
		ssParity  int
		success   bool
	}{
		{2, 4, true},
		{3, 3, true},
		{1, 4, false},
		{7, 6, false},
		{9, 0, false},
		{9, 9, false},
		{2, 9, false},
	}
	for i, tt := range tests {
		err := validateParity(tt.ssParity, tt.rrsParity)
		if err != nil && tt.success {
			t.Errorf("Test %d, Expected success, got %s", i+1, err)
		}
		if err == nil && !tt.success {
			t.Errorf("Test %d, Expected failure, got success", i+1)
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
		sc             string
		disksCount     int
		expectedData   int
		expectedParity int
	}{
		{reducedRedundancyStorageClass, len(xl.storageDisks), 14, 2},
		{standardStorageClass, len(xl.storageDisks), 8, 8},
		{"", len(xl.storageDisks), 8, 8},
		{reducedRedundancyStorageClass, len(xl.storageDisks), 9, 7},
		{standardStorageClass, len(xl.storageDisks), 10, 6},
		{"", len(xl.storageDisks), 9, 7},
	}
	for i, tt := range tests {
		// Set env var for test case 4
		if i+1 == 4 {
			globalRRStorageClass.Parity = 7
		}
		// Set env var for test case 5
		if i+1 == 5 {
			globalStandardStorageClass.Parity = 6
		}
		// Set env var for test case 6
		if i+1 == 6 {
			globalStandardStorageClass.Parity = 7
		}
		data, parity := getRedundancyCount(tt.sc, tt.disksCount)
		if data != tt.expectedData {
			t.Errorf("Test %d, Expected data disks %d, got %d", i+1, tt.expectedData, data)
			return
		}
		if parity != tt.expectedParity {
			t.Errorf("Test %d, Expected parity disks %d, got %d", i+1, tt.expectedParity, parity)
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
		parts               []xlMetaV1
		errs                []error
		expectedReadQuorum  int
		expectedWriteQuorum int
		expectedError       error
	}{
		{parts1, errs1, 8, 9, nil},
		{parts2, errs2, 14, 15, nil},
		{parts3, errs3, 8, 9, nil},
		{parts4, errs4, 10, 11, nil},
		{parts5, errs5, 14, 15, nil},
		{parts6, errs6, 8, 9, nil},
		{parts7, errs7, 14, 15, nil},
	}
	for i, tt := range tests {
		actualReadQuorum, actualWriteQuorum, err := objectQuorumFromMeta(*xl, tt.parts, tt.errs)
		if tt.expectedError != nil && err == nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if tt.expectedError == nil && err != nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if tt.expectedReadQuorum != actualReadQuorum {
			t.Errorf("Test %d, Expected Read Quorum %d, got %d", i+1, tt.expectedReadQuorum, actualReadQuorum)
			return
		}
		if tt.expectedWriteQuorum != actualWriteQuorum {
			t.Errorf("Test %d, Expected Write Quorum %d, got %d", i+1, tt.expectedWriteQuorum, actualWriteQuorum)
			return
		}
	}
}

// Test isValidStorageClassMeta method with valid and invalid inputs
func TestIsValidStorageClassMeta(t *testing.T) {
	tests := []struct {
		sc   string
		want bool
	}{
		{"STANDARD", true},
		{"REDUCED_REDUNDANCY", true},
		{"", false},
		{"INVALID", false},
		{"123", false},
		{"MINIO_STORAGE_CLASS_RRS", false},
		{"MINIO_STORAGE_CLASS_STANDARD", false},
	}
	for i, tt := range tests {
		if got := isValidStorageClassMeta(tt.sc); got != tt.want {
			t.Errorf("Test %d, Expected Storage Class to be %t, got %t", i+1, tt.want, got)
		}
	}
}
