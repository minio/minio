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
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/kms"
)

// Return pointer to testOneByteReadEOF{}
func newTestReaderEOF(data []byte) io.Reader {
	return &testOneByteReadEOF{false, data}
}

// OneByteReadEOF - implements io.Reader which returns 1 byte along with io.EOF error.
type testOneByteReadEOF struct {
	eof  bool
	data []byte
}

func (r *testOneByteReadEOF) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}
	n = copy(p, r.data)
	r.eof = true
	return n, io.EOF
}

// Return pointer to testOneByteReadNoEOF{}
func newTestReaderNoEOF(data []byte) io.Reader {
	return &testOneByteReadNoEOF{false, data}
}

// testOneByteReadNoEOF - implements io.Reader which returns 1 byte and nil error, but
// returns io.EOF on the next Read().
type testOneByteReadNoEOF struct {
	eof  bool
	data []byte
}

func (r *testOneByteReadNoEOF) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}
	n = copy(p, r.data)
	r.eof = true
	return n, nil
}

// Wrapper for calling testMakeBucket for both Erasure and FS.
func TestMakeBucket(t *testing.T) {
	ExecObjectLayerTest(t, testMakeBucket)
}

// Tests validate bucket creation.
func testMakeBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "bucket-unknown", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipartObjectCreation for both Erasure and FS.
func TestMultipartObjectCreation(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testMultipartObjectCreation)
}

// Tests validate creation of part files during Multipart operation.
func testMultipartObjectCreation(obj ObjectLayer, instanceType string, t TestErrHandler) {
	var opts ObjectOptions
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	res, err := obj.NewMultipartUpload(context.Background(), "bucket", "key", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID := res.UploadID

	// Create a byte array of 5MiB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*humanize.MiByte/16)
	completedParts := CompleteMultipartUpload{}
	for i := 1; i <= 10; i++ {
		expectedETaghex := getMD5Hash(data)

		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart(context.Background(), "bucket", "key", uploadID, i, mustGetPutObjReader(t, bytes.NewBuffer(data), int64(len(data)), expectedETaghex, ""), opts)
		if err != nil {
			t.Errorf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedETaghex {
			t.Errorf("MD5 Mismatch")
		}
		completedParts.Parts = append(completedParts.Parts, CompletePart{
			PartNumber: i,
			ETag:       calcPartInfo.ETag,
		})
	}
	objInfo, err := obj.CompleteMultipartUpload(context.Background(), "bucket", "key", uploadID, completedParts.Parts, ObjectOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if objInfo.ETag != "7d364cb728ce42a74a96d22949beefb2-10" {
		t.Errorf("Md5 mismtch")
	}
}

// Wrapper for calling testMultipartObjectAbort for both Erasure and FS.
func TestMultipartObjectAbort(t *testing.T) {
	ExecObjectLayerTest(t, testMultipartObjectAbort)
}

// Tests validate abortion of Multipart operation.
func testMultipartObjectAbort(obj ObjectLayer, instanceType string, t TestErrHandler) {
	var opts ObjectOptions
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	res, err := obj.NewMultipartUpload(context.Background(), "bucket", "key", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID := res.UploadID

	parts := make(map[int]string)
	metadata := make(map[string]string)
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString += strconv.Itoa(num)
		}

		expectedETaghex := getMD5Hash([]byte(randomString))

		metadata["md5"] = expectedETaghex
		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart(context.Background(), "bucket", "key", uploadID, i, mustGetPutObjReader(t, bytes.NewBufferString(randomString), int64(len(randomString)), expectedETaghex, ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedETaghex {
			t.Errorf("Md5 Mismatch")
		}
		parts[i] = expectedETaghex
	}
	err = obj.AbortMultipartUpload(context.Background(), "bucket", "key", uploadID, ObjectOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipleObjectCreation for both Erasure and FS.
func TestMultipleObjectCreation(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testMultipleObjectCreation)
}

// Tests validate object creation.
func testMultipleObjectCreation(obj ObjectLayer, instanceType string, t TestErrHandler) {
	objects := make(map[string][]byte)
	var opts ObjectOptions
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	for i := range 10 {
		randomPerm := rand.Perm(100)
		randomString := ""
		for _, num := range randomPerm {
			randomString += strconv.Itoa(num)
		}

		expectedETaghex := getMD5Hash([]byte(randomString))

		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		metadata := make(map[string]string)
		metadata["etag"] = expectedETaghex
		var objInfo ObjectInfo
		objInfo, err = obj.PutObject(context.Background(), "bucket", key, mustGetPutObjReader(t, bytes.NewBufferString(randomString), int64(len(randomString)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.ETag != expectedETaghex {
			t.Errorf("Md5 Mismatch")
		}
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		err = GetObject(context.Background(), obj, "bucket", key, 0, int64(len(value)), &byteBuffer, "", opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if !bytes.Equal(byteBuffer.Bytes(), value) {
			t.Errorf("%s: Mismatch of GetObject data with the expected one.", instanceType)
		}

		objInfo, err := obj.GetObjectInfo(context.Background(), "bucket", key, opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.Size != int64(len(value)) {
			t.Errorf("%s: Size mismatch of the GetObject data.", instanceType)
		}
	}
}

// Wrapper for calling TestPaging for both Erasure and FS.
func TestPaging(t *testing.T) {
	ExecObjectLayerTest(t, testPaging)
}

// Tests validate creation of objects and the order of listing using various filters for ListObjects operation.
func testPaging(obj ObjectLayer, instanceType string, t TestErrHandler) {
	obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	result, err := obj.ListObjects(context.Background(), "bucket", "", "", "", 0)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(result.Objects) != 0 {
		t.Errorf("%s: Number of objects in the result different from expected value.", instanceType)
	}
	if result.IsTruncated {
		t.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}

	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	var opts ObjectOptions
	// check before paging occurs.
	for i := range 5 {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject(context.Background(), "bucket", key, mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		result, err = obj.ListObjects(context.Background(), "bucket", "", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != i+1 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, len(result.Objects), i+1)
		}
		if result.IsTruncated {
			t.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}

	// check after paging occurs pages work.
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject(context.Background(), "bucket", key, mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects(context.Background(), "bucket", "obj", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 5 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 5, len(result.Objects))
		}
		if !result.IsTruncated {
			t.Errorf("%s: Expected IsTruncated to be `true`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}
	// check paging with prefix at end returns less objects.
	{
		_, err = obj.PutObject(context.Background(), "bucket", "newPrefix", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject(context.Background(), "bucket", "newPrefix2", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects(context.Background(), "bucket", "new", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 2 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 2, len(result.Objects))
		}
	}

	// check ordering of pages.
	{
		result, err = obj.ListObjects(context.Background(), "bucket", "", "", "", 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
	}

	// check delimited results with delimiter and prefix.
	{
		_, err = obj.PutObject(context.Background(), "bucket", "this/is/delimited", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject(context.Background(), "bucket", "this/is/also/a/delimited/file", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects(context.Background(), "bucket", "this/is/", "", SlashSeparator, 10)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 1 {
			t.Errorf("%s: Expected the number of objects in the result to be %d, but instead found %d", instanceType, 1, len(result.Objects))
		}
		if result.Prefixes[0] != "this/is/also/" {
			t.Errorf("%s: Expected prefix to be `%s`, but instead found `%s`", instanceType, "this/is/also/", result.Prefixes[0])
		}
	}

	// check delimited results with delimiter without prefix.
	{
		result, err = obj.ListObjects(context.Background(), "bucket", "", "", SlashSeparator, 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
		if result.Prefixes[0] != "this/" {
			t.Errorf("%s: Expected the prefix to be `%s`, but instead found `%s`", instanceType, "this/", result.Prefixes[0])
		}
	}

	// check results with Marker.
	{
		result, err = obj.ListObjects(context.Background(), "bucket", "", "newPrefix", "", 3)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[2].Name)
		}
	}
	// check ordering of results with prefix.
	{
		result, err = obj.ListObjects(context.Background(), "bucket", "obj", "", "", 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj10", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj2", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj3" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj3", result.Objects[4].Name)
		}
	}
	// check ordering of results with prefix and no paging.
	{
		result, err = obj.ListObjects(context.Background(), "bucket", "new", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
	}

	// check paging works.
	ag := []string{"a", "b", "c", "d", "e", "f", "g"}
	checkObjCount := make(map[string]int)
	for i := range 7 {
		dirName := strings.Repeat(ag[i], 3)
		key := fmt.Sprintf("testPrefix/%s/obj%s", dirName, dirName)
		checkObjCount[key]++
		_, err = obj.PutObject(context.Background(), "bucket", key, mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		token := ""
		for ctx.Err() == nil {
			result, err := obj.ListObjectsV2(ctx, "bucket", "testPrefix", token, "", 2, false, "")
			if err != nil {
				t.Fatalf("%s: <ERROR> %s", instanceType, err)
			}
			token = result.NextContinuationToken
			if len(result.Objects) == 0 {
				break
			}
			for _, obj := range result.Objects {
				checkObjCount[obj.Name]--
			}
			if token == "" {
				break
			}
		}
		for key, value := range checkObjCount {
			if value != 0 {
				t.Errorf("%s: Expected value of objects to be %d, instead found to be %d", instanceType, 0, value)
			}
			delete(checkObjCount, key)
		}
	}
}

// Wrapper for calling testObjectOverwriteWorks for both Erasure and FS.
func TestObjectOverwriteWorks(t *testing.T) {
	ExecObjectLayerTest(t, testObjectOverwriteWorks)
}

// Tests validate overwriting of an existing object.
func testObjectOverwriteWorks(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var opts ObjectOptions
	uploadContent := "The list of parts was not in ascending order. The parts list must be specified in order by part number."
	length := int64(len(uploadContent))
	_, err = obj.PutObject(context.Background(), "bucket", "object", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent = "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	length = int64(len(uploadContent))
	_, err = obj.PutObject(context.Background(), "bucket", "object", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = GetObject(context.Background(), obj, "bucket", "object", 0, length, &bytesBuffer, "", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if bytesBuffer.String() != "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed." {
		t.Errorf("%s: Invalid upload ID error mismatch.", instanceType)
	}
}

// Wrapper for calling testNonExistentBucketOperations for both Erasure and FS.
func TestNonExistentBucketOperations(t *testing.T) {
	ExecObjectLayerTest(t, testNonExistentBucketOperations)
}

// Tests validate that bucket operation on non-existent bucket fails.
func testNonExistentBucketOperations(obj ObjectLayer, instanceType string, t TestErrHandler) {
	var opts ObjectOptions
	_, err := obj.PutObject(context.Background(), "bucket1", "object", mustGetPutObjReader(t, bytes.NewBufferString("one"), int64(len("one")), "", ""), opts)
	if err == nil {
		t.Fatal("Expected error but found nil")
	}
	if err.Error() != "Bucket not found: bucket1" {
		t.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket1", err.Error())
	}
}

// Wrapper for calling testBucketRecreateFails for both Erasure and FS.
func TestBucketRecreateFails(t *testing.T) {
	ExecObjectLayerTest(t, testBucketRecreateFails)
}

// Tests validate that recreation of the bucket fails.
func testBucketRecreateFails(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "string", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucket(context.Background(), "string", MakeBucketOptions{})
	if err == nil {
		t.Fatalf("%s: Expected error but found nil.", instanceType)
	}

	if err.Error() != "Bucket exists: string" {
		t.Errorf("%s: Expected the error message to be `%s`, but instead found `%s`", instanceType, "Bucket exists: string", err.Error())
	}
}

func enableCompression(t *testing.T, encrypt bool, mimeTypes []string, extensions []string) {
	// Enable compression and exec...
	globalCompressConfigMu.Lock()
	globalCompressConfig.Enabled = true
	globalCompressConfig.MimeTypes = mimeTypes
	globalCompressConfig.Extensions = extensions
	globalCompressConfig.AllowEncrypted = encrypt
	globalCompressConfigMu.Unlock()
	if encrypt {
		globalAutoEncryption = encrypt
		KMS, err := kms.ParseSecretKey("my-minio-key:5lF+0pJM0OWwlQrvK2S/I7W9mO4a6rJJI7wzj7v09cw=")
		if err != nil {
			t.Fatal(err)
		}
		GlobalKMS = KMS
	}
}

func enableEncryption(t *testing.T) {
	// Exec with default settings...
	globalCompressConfigMu.Lock()
	globalCompressConfig.Enabled = false
	globalCompressConfigMu.Unlock()

	globalAutoEncryption = true
	KMS, err := kms.ParseSecretKey("my-minio-key:5lF+0pJM0OWwlQrvK2S/I7W9mO4a6rJJI7wzj7v09cw=")
	if err != nil {
		t.Fatal(err)
	}
	GlobalKMS = KMS
}

func resetCompressEncryption() {
	// Reset...
	globalCompressConfigMu.Lock()
	globalCompressConfig.Enabled = false
	globalCompressConfig.AllowEncrypted = false
	globalCompressConfigMu.Unlock()
	globalAutoEncryption = false
	GlobalKMS = nil
}

func execExtended(t *testing.T, fn func(t *testing.T, init func(), bucketOptions MakeBucketOptions)) {
	// Exec with default settings...
	resetCompressEncryption()
	t.Run("default", func(t *testing.T) {
		fn(t, nil, MakeBucketOptions{})
	})
	t.Run("default+versioned", func(t *testing.T) {
		fn(t, nil, MakeBucketOptions{VersioningEnabled: true})
	})

	t.Run("compressed", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableCompression(t, false, []string{"*"}, []string{"*"})
		}, MakeBucketOptions{})
	})
	t.Run("compressed+versioned", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableCompression(t, false, []string{"*"}, []string{"*"})
		}, MakeBucketOptions{
			VersioningEnabled: true,
		})
	})

	t.Run("encrypted", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableEncryption(t)
		}, MakeBucketOptions{})
	})
	t.Run("encrypted+versioned", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableEncryption(t)
		}, MakeBucketOptions{
			VersioningEnabled: true,
		})
	})

	t.Run("compressed+encrypted", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableCompression(t, true, []string{"*"}, []string{"*"})
		}, MakeBucketOptions{})
	})
	t.Run("compressed+encrypted+versioned", func(t *testing.T) {
		fn(t, func() {
			resetCompressEncryption()
			enableCompression(t, true, []string{"*"}, []string{"*"})
		}, MakeBucketOptions{
			VersioningEnabled: true,
		})
	})
}

// ExecExtendedObjectLayerTest will execute the tests with combinations of encrypted & compressed.
// This can be used to test functionality when reading and writing data.
func ExecExtendedObjectLayerTest(t *testing.T, objTest objTestType) {
	execExtended(t, func(t *testing.T, init func(), bucketOptions MakeBucketOptions) {
		ExecObjectLayerTest(t, objTest)
	})
}

// Wrapper for calling testPutObject for both Erasure and FS.
func TestPutObject(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testPutObject)
}

// Tests validate PutObject without prefix.
func testPutObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	content := []byte("testcontent")
	length := int64(len(content))
	readerEOF := newTestReaderEOF(content)
	readerNoEOF := newTestReaderNoEOF(content)
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer1 bytes.Buffer
	var opts ObjectOptions
	_, err = obj.PutObject(context.Background(), "bucket", "object", mustGetPutObjReader(t, readerEOF, length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = GetObject(context.Background(), obj, "bucket", "object", 0, length, &bytesBuffer1, "", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer1.Bytes()) != len(content) {
		t.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer1.Bytes()))
	}

	var bytesBuffer2 bytes.Buffer
	_, err = obj.PutObject(context.Background(), "bucket", "object", mustGetPutObjReader(t, readerNoEOF, length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = GetObject(context.Background(), obj, "bucket", "object", 0, length, &bytesBuffer2, "", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer2.Bytes()) != len(content) {
		t.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer2.Bytes()))
	}
}

// Wrapper for calling testPutObjectInSubdir for both Erasure and FS.
func TestPutObjectInSubdir(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testPutObjectInSubdir)
}

// Tests validate PutObject with subdirectory prefix.
func testPutObjectInSubdir(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var opts ObjectOptions
	uploadContent := `The specified multipart upload does not exist. The upload ID might be invalid, or the multipart
 upload might have been aborted or completed.`
	length := int64(len(uploadContent))
	_, err = obj.PutObject(context.Background(), "bucket", "dir1/dir2/object", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = GetObject(context.Background(), obj, "bucket", "dir1/dir2/object", 0, length, &bytesBuffer, "", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer.Bytes()) != len(uploadContent) {
		t.Errorf("%s: Expected length of downloaded data to be `%d`, but instead found `%d`",
			instanceType, len(uploadContent), len(bytesBuffer.Bytes()))
	}
}

// Wrapper for calling testListBuckets for both Erasure and FS.
func TestListBuckets(t *testing.T) {
	ExecObjectLayerTest(t, testListBuckets)
}

// Tests validate ListBuckets.
func testListBuckets(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// test empty list.
	buckets, err := obj.ListBuckets(context.Background(), BucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 0 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 0, len(buckets))
	}

	// add one and test exists.
	err = obj.MakeBucket(context.Background(), "bucket1", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets(context.Background(), BucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 1 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 1, len(buckets))
	}

	// add two and test exists.
	err = obj.MakeBucket(context.Background(), "bucket2", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets(context.Background(), BucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	// add three and test exists + prefix.
	err = obj.MakeBucket(context.Background(), "bucket22", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets(context.Background(), BucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 3 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 3, len(buckets))
	}
}

// Wrapper for calling testListBucketsOrder for both Erasure and FS.
func TestListBucketsOrder(t *testing.T) {
	ExecObjectLayerTest(t, testListBucketsOrder)
}

// Tests validate the order of result of ListBuckets.
func testListBucketsOrder(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time.
	// add one and test exists.
	err := obj.MakeBucket(context.Background(), "bucket1", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucket(context.Background(), "bucket2", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	buckets, err := obj.ListBuckets(context.Background(), BucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	if buckets[0].Name != "bucket1" {
		t.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket1", buckets[0].Name)
	}
	if buckets[1].Name != "bucket2" {
		t.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket2", buckets[1].Name)
	}
}

// Wrapper for calling testListObjectsTestsForNonExistentBucket for both Erasure and FS.
func TestListObjectsTestsForNonExistentBucket(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsTestsForNonExistentBucket)
}

// Tests validate that ListObjects operation on a non-existent bucket fails as expected.
func testListObjectsTestsForNonExistentBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	result, err := obj.ListObjects(context.Background(), "bucket", "", "", "", 1000)
	if err == nil {
		t.Fatalf("%s: Expected error but found nil.", instanceType)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("%s: Expected number of objects in the result to be `%d`, but instead found `%d`", instanceType, 0, len(result.Objects))
	}
	if result.IsTruncated {
		t.Fatalf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}
	if err.Error() != "Bucket not found: bucket" {
		t.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket", err.Error())
	}
}

// Wrapper for calling testNonExistentObjectInBucket for both Erasure and FS.
func TestNonExistentObjectInBucket(t *testing.T) {
	ExecObjectLayerTest(t, testNonExistentObjectInBucket)
}

// Tests validate that GetObject fails on a non-existent bucket as expected.
func testNonExistentObjectInBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	_, err = obj.GetObjectInfo(context.Background(), "bucket", "dir1", ObjectOptions{})
	if err == nil {
		t.Fatalf("%s: Expected error but found nil", instanceType)
	}
	if isErrObjectNotFound(err) {
		if err.Error() != "Object not found: bucket/dir1" {
			t.Errorf("%s: Expected the Error message to be `%s`, but instead found `%s`", instanceType, "Object not found: bucket/dir1", err.Error())
		}
	} else {
		if err.Error() != "fails" {
			t.Errorf("%s: Expected the Error message to be `%s`, but instead found it to be `%s`", instanceType, "fails", err.Error())
		}
	}
}

// Wrapper for calling testGetDirectoryReturnsObjectNotFound for both Erasure and FS.
func TestGetDirectoryReturnsObjectNotFound(t *testing.T) {
	ExecObjectLayerTest(t, testGetDirectoryReturnsObjectNotFound)
}

// Tests validate that GetObject on an existing directory fails as expected.
func testGetDirectoryReturnsObjectNotFound(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketName := "bucket"
	err := obj.MakeBucket(context.Background(), bucketName, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	content := "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
	length := int64(len(content))
	var opts ObjectOptions
	_, err = obj.PutObject(context.Background(), bucketName, "dir1/dir3/object", mustGetPutObjReader(t, bytes.NewBufferString(content), length, "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	testCases := []struct {
		dir string
		err error
	}{
		{
			dir: "dir1/",
			err: ObjectNotFound{Bucket: bucketName, Object: "dir1/"},
		},
		{
			dir: "dir1/dir3/",
			err: ObjectNotFound{Bucket: bucketName, Object: "dir1/dir3/"},
		},
	}

	for i, testCase := range testCases {
		_, expectedErr := obj.GetObjectInfo(context.Background(), bucketName, testCase.dir, opts)
		if expectedErr != nil && expectedErr.Error() != testCase.err.Error() {
			t.Errorf("Test %d, %s: Expected error %s, got %s", i+1, instanceType, testCase.err, expectedErr)
		}
	}
}

// Wrapper for calling testContentType for both Erasure and FS.
func TestContentType(t *testing.T) {
	ExecObjectLayerTest(t, testContentType)
}

// Test content-type.
func testContentType(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucket(context.Background(), "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	var opts ObjectOptions
	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	// Test empty.
	_, err = obj.PutObject(context.Background(), "bucket", "minio.png", mustGetPutObjReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	objInfo, err := obj.GetObjectInfo(context.Background(), "bucket", "minio.png", opts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	if objInfo.ContentType != "image/png" {
		t.Errorf("%s: Expected Content type to be `%s`, but instead found `%s`", instanceType, "image/png", objInfo.ContentType)
	}
}
