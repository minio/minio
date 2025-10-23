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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

func TestListObjectsVersionedFolders(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsVersionedFolders)
}

func testListObjectsVersionedFolders(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	t, _ := t1.(*testing.T)
	testBuckets := []string{
		// This bucket is used for testing ListObject operations.
		"test-bucket-folders",
		// This bucket has file delete marker.
		"test-bucket-files",
	}
	for _, bucket := range testBuckets {
		err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{
			VersioningEnabled: true,
		})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	var err error
	testObjects := []struct {
		parentBucket    string
		name            string
		content         string
		meta            map[string]string
		addDeleteMarker bool
	}{
		{testBuckets[0], "unique/folder/", "", nil, true},
		{testBuckets[0], "unique/folder/1.txt", "content", nil, false},
		{testBuckets[1], "unique/folder/1.txt", "content", nil, true},
	}
	for _, object := range testObjects {
		md5Bytes := md5.Sum([]byte(object.content))
		_, err = obj.PutObject(context.Background(), object.parentBucket, object.name, mustGetPutObjReader(t, bytes.NewBufferString(object.content),
			int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{
			Versioned:   globalBucketVersioningSys.PrefixEnabled(object.parentBucket, object.name),
			UserDefined: object.meta,
		})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
		if object.addDeleteMarker {
			oi, err := obj.DeleteObject(context.Background(), object.parentBucket, object.name, ObjectOptions{
				Versioned: globalBucketVersioningSys.PrefixEnabled(object.parentBucket, object.name),
			})
			if err != nil {
				t.Fatalf("%s : %s", instanceType, err.Error())
			}
			if oi.DeleteMarker != object.addDeleteMarker {
				t.Fatalf("Expected, marker %t : got %t", object.addDeleteMarker, oi.DeleteMarker)
			}
		}
	}

	// Formulating the result data set to be expected from ListObjects call inside the tests,
	// This will be used in testCases and used for asserting the correctness of ListObjects output in the tests.

	resultCases := []ListObjectsInfo{
		{
			IsTruncated: false,
			Prefixes:    []string{"unique/folder/"},
		},
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "unique/folder/1.txt"},
			},
		},
		{
			IsTruncated: false,
			Objects:     []ObjectInfo{},
		},
	}

	resultCasesV := []ListObjectVersionsInfo{
		{
			IsTruncated: false,
			Prefixes:    []string{"unique/folder/"},
		},
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{
					Name:         "unique/folder/",
					DeleteMarker: true,
				},
				{
					Name:         "unique/folder/",
					DeleteMarker: false,
				},
				{
					Name:         "unique/folder/1.txt",
					DeleteMarker: false,
				},
			},
		},
	}

	testCases := []struct {
		// Inputs to ListObjects.
		bucketName string
		prefix     string
		marker     string
		delimiter  string
		maxKeys    int
		versioned  bool
		// Expected output of ListObjects.
		resultL ListObjectsInfo
		resultV ListObjectVersionsInfo
		err     error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		{testBuckets[0], "unique/", "", "/", 1000, false, resultCases[0], ListObjectVersionsInfo{}, nil, true},
		{testBuckets[0], "unique/folder", "", "/", 1000, false, resultCases[0], ListObjectVersionsInfo{}, nil, true},
		{testBuckets[0], "unique/", "", "", 1000, false, resultCases[1], ListObjectVersionsInfo{}, nil, true},
		{testBuckets[1], "unique/", "", "/", 1000, false, resultCases[0], ListObjectVersionsInfo{}, nil, true},
		{testBuckets[1], "unique/folder/", "", "/", 1000, false, resultCases[2], ListObjectVersionsInfo{}, nil, true},
		{testBuckets[0], "unique/", "", "/", 1000, true, ListObjectsInfo{}, resultCasesV[0], nil, true},
		{testBuckets[0], "unique/", "", "", 1000, true, ListObjectsInfo{}, resultCasesV[1], nil, true},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s-Test%d", instanceType, i+1), func(t *testing.T) {
			var err error
			var resultL ListObjectsInfo
			var resultV ListObjectVersionsInfo
			if testCase.versioned {
				t.Log("ListObjectVersions, bucket:", testCase.bucketName, "prefix:",
					testCase.prefix, "marker:", testCase.marker, "delimiter:",
					testCase.delimiter, "maxkeys:", testCase.maxKeys)

				resultV, err = obj.ListObjectVersions(t.Context(), testCase.bucketName,
					testCase.prefix, testCase.marker, "", testCase.delimiter, testCase.maxKeys)
			} else {
				t.Log("ListObjects, bucket:", testCase.bucketName, "prefix:",
					testCase.prefix, "marker:", testCase.marker, "delimiter:",
					testCase.delimiter, "maxkeys:", testCase.maxKeys)

				resultL, err = obj.ListObjects(t.Context(), testCase.bucketName,
					testCase.prefix, testCase.marker, testCase.delimiter, testCase.maxKeys)
			}
			if err != nil && testCase.shouldPass {
				t.Errorf("Test %d: %s:  Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
			}
			if err == nil && !testCase.shouldPass {
				t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.err.Error())
			}
			// Failed as expected, but does it fail for the expected reason.
			if err != nil && !testCase.shouldPass {
				if !strings.Contains(err.Error(), testCase.err.Error()) {
					t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.err.Error(), err.Error())
				}
			}
			// Since there are cases for which ListObjects fails, this is
			// necessary. Test passes as expected, but the output values
			// are verified for correctness here.
			if err == nil && testCase.shouldPass {
				// The length of the expected ListObjectsResult.Objects
				// should match in both expected result from test cases
				// and in the output. On failure calling t.Fatalf,
				// otherwise it may lead to index out of range error in
				// assertion following this.
				if !testCase.versioned {
					if len(testCase.resultL.Objects) != len(resultL.Objects) {
						t.Logf("want: %v", objInfoNames(testCase.resultL.Objects))
						t.Logf("got: %v", objInfoNames(resultL.Objects))
						t.Errorf("Test %d: %s: Expected number of object in the result to be '%d', but found '%d' objects instead", i+1, instanceType, len(testCase.resultL.Objects), len(resultL.Objects))
					}
					for j := 0; j < len(testCase.resultL.Objects); j++ {
						if j >= len(resultL.Objects) {
							t.Errorf("Test %d: %s: Expected object name to be \"%s\", but not nothing instead", i+1, instanceType, testCase.resultL.Objects[j].Name)
							continue
						}
						if testCase.resultL.Objects[j].Name != resultL.Objects[j].Name {
							t.Errorf("Test %d: %s: Expected object name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.resultL.Objects[j].Name, resultL.Objects[j].Name)
						}
					}

					if len(testCase.resultL.Prefixes) != len(resultL.Prefixes) {
						t.Logf("want: %v", testCase.resultL.Prefixes)
						t.Logf("got: %v", resultL.Prefixes)
						t.Errorf("Test %d: %s: Expected number of prefixes in the result to be '%d', but found '%d' prefixes instead", i+1, instanceType, len(testCase.resultL.Prefixes), len(resultL.Prefixes))
					}
					for j := 0; j < len(testCase.resultL.Prefixes); j++ {
						if j >= len(resultL.Prefixes) {
							t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found no result", i+1, instanceType, testCase.resultL.Prefixes[j])
							continue
						}
						if testCase.resultL.Prefixes[j] != resultL.Prefixes[j] {
							t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.resultL.Prefixes[j], resultL.Prefixes[j])
						}
					}

					if testCase.resultL.IsTruncated != resultL.IsTruncated {
						// Allow an extra continuation token.
						if !resultL.IsTruncated || len(resultL.Objects) == 0 {
							t.Errorf("Test %d: %s: Expected IsTruncated flag to be %v, but instead found it to be %v", i+1, instanceType, testCase.resultL.IsTruncated, resultL.IsTruncated)
						}
					}

					if testCase.resultL.IsTruncated && resultL.NextMarker == "" {
						t.Errorf("Test %d: %s: Expected NextMarker to contain a string since listing is truncated, but instead found it to be empty", i+1, instanceType)
					}

					if !testCase.resultL.IsTruncated && resultL.NextMarker != "" {
						if !resultL.IsTruncated || len(resultL.Objects) == 0 {
							t.Errorf("Test %d: %s: Expected NextMarker to be empty since listing is not truncated, but instead found `%v`", i+1, instanceType, resultL.NextMarker)
						}
					}
				} else {
					if len(testCase.resultV.Objects) != len(resultV.Objects) {
						t.Logf("want: %v", objInfoNames(testCase.resultV.Objects))
						t.Logf("got: %v", objInfoNames(resultV.Objects))
						t.Errorf("Test %d: %s: Expected number of object in the result to be '%d', but found '%d' objects instead", i+1, instanceType, len(testCase.resultV.Objects), len(resultV.Objects))
					}
					for j := 0; j < len(testCase.resultV.Objects); j++ {
						if j >= len(resultV.Objects) {
							t.Errorf("Test %d: %s: Expected object name to be \"%s\", but not nothing instead", i+1, instanceType, testCase.resultV.Objects[j].Name)
							continue
						}
						if testCase.resultV.Objects[j].Name != resultV.Objects[j].Name {
							t.Errorf("Test %d: %s: Expected object name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.resultV.Objects[j].Name, resultV.Objects[j].Name)
						}
					}

					if len(testCase.resultV.Prefixes) != len(resultV.Prefixes) {
						t.Logf("want: %v", testCase.resultV.Prefixes)
						t.Logf("got: %v", resultV.Prefixes)
						t.Errorf("Test %d: %s: Expected number of prefixes in the result to be '%d', but found '%d' prefixes instead", i+1, instanceType, len(testCase.resultV.Prefixes), len(resultV.Prefixes))
					}
					for j := 0; j < len(testCase.resultV.Prefixes); j++ {
						if j >= len(resultV.Prefixes) {
							t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found no result", i+1, instanceType, testCase.resultV.Prefixes[j])
							continue
						}
						if testCase.resultV.Prefixes[j] != resultV.Prefixes[j] {
							t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.resultV.Prefixes[j], resultV.Prefixes[j])
						}
					}

					if testCase.resultV.IsTruncated != resultV.IsTruncated {
						// Allow an extra continuation token.
						if !resultV.IsTruncated || len(resultV.Objects) == 0 {
							t.Errorf("Test %d: %s: Expected IsTruncated flag to be %v, but instead found it to be %v", i+1, instanceType, testCase.resultV.IsTruncated, resultV.IsTruncated)
						}
					}

					if testCase.resultV.IsTruncated && resultV.NextMarker == "" {
						t.Errorf("Test %d: %s: Expected NextMarker to contain a string since listing is truncated, but instead found it to be empty", i+1, instanceType)
					}

					if !testCase.resultV.IsTruncated && resultV.NextMarker != "" {
						if !resultV.IsTruncated || len(resultV.Objects) == 0 {
							t.Errorf("Test %d: %s: Expected NextMarker to be empty since listing is not truncated, but instead found `%v`", i+1, instanceType, resultV.NextMarker)
						}
					}
				}
			}
		})
	}
}

// Wrapper for calling ListObjectsOnVersionedBuckets tests for both
// Erasure multiple disks and single node setup.
func TestListObjectsOnVersionedBuckets(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsOnVersionedBuckets)
}

// Wrapper for calling ListObjects tests for both Erasure multiple
// disks and single node setup.
func TestListObjects(t *testing.T) {
	ExecObjectLayerTest(t, testListObjects)
}

// Unit test for ListObjects on VersionedBucket.
func testListObjectsOnVersionedBuckets(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	_testListObjects(obj, instanceType, t1, true)
}

// Unit test for ListObjects.
func testListObjects(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	_testListObjects(obj, instanceType, t1, false)
}

func _testListObjects(obj ObjectLayer, instanceType string, t1 TestErrHandler, versioned bool) {
	t, _ := t1.(*testing.T)
	testBuckets := []string{
		// This bucket is used for testing ListObject operations.
		0: "test-bucket-list-object",
		// This bucket will be tested with empty directories
		1: "test-bucket-empty-dir",
		// Will not store any objects in this bucket,
		// Its to test ListObjects on an empty bucket.
		2: "empty-bucket",
		// Listing the case where the marker > last object.
		3: "test-bucket-single-object",
		// Listing uncommon delimiter.
		4: "test-bucket-delimiter",
		// Listing prefixes > maxKeys
		5: "test-bucket-max-keys-prefixes",
		// Listing custom delimiters
		6: "test-bucket-custom-delimiter",
	}
	for _, bucket := range testBuckets {
		err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{
			VersioningEnabled: versioned,
		})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	var err error
	testObjects := []struct {
		parentBucket string
		name         string
		content      string
		meta         map[string]string
	}{
		{testBuckets[0], "Asia-maps.png", "asis-maps", map[string]string{"content-type": "image/png"}},
		{testBuckets[0], "Asia/India/India-summer-photos-1", "contentstring", nil},
		{testBuckets[0], "Asia/India/Karnataka/Bangalore/Koramangala/pics", "contentstring", nil},
		{testBuckets[0], "newPrefix0", "newPrefix0", nil},
		{testBuckets[0], "newPrefix1", "newPrefix1", nil},
		{testBuckets[0], "newzen/zen/recurse/again/again/again/pics", "recurse", nil},
		{testBuckets[0], "obj0", "obj0", nil},
		{testBuckets[0], "obj1", "obj1", nil},
		{testBuckets[0], "obj2", "obj2", nil},
		{testBuckets[1], "obj1", "obj1", nil},
		{testBuckets[1], "obj2", "obj2", nil},
		{testBuckets[1], "temporary/0/", "", nil},
		{testBuckets[3], "A/B", "contentstring", nil},
		{testBuckets[4], "file1/receipt.json", "content", nil},
		{testBuckets[4], "file1/guidSplunk-aaaa/file", "content", nil},
		{testBuckets[5], "dir/day_id=2017-10-10/issue", "content", nil},
		{testBuckets[5], "dir/day_id=2017-10-11/issue", "content", nil},
		{testBuckets[5], "foo/201910/1122", "content", nil},
		{testBuckets[5], "foo/201910/1112", "content", nil},
		{testBuckets[5], "foo/201910/2112", "content", nil},
		{testBuckets[5], "foo/201910_txt", "content", nil},
		{testBuckets[5], "201910/foo/bar/xl.meta/1.txt", "content", nil},
		{testBuckets[6], "aaa", "content", nil},
		{testBuckets[6], "bbb_aaa", "content", nil},
		{testBuckets[6], "bbb_aaa", "content", nil},
		{testBuckets[6], "ccc", "content", nil},
	}
	for _, object := range testObjects {
		md5Bytes := md5.Sum([]byte(object.content))
		_, err = obj.PutObject(context.Background(), object.parentBucket, object.name,
			mustGetPutObjReader(t, bytes.NewBufferString(object.content),
				int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{
				Versioned:   globalBucketVersioningSys.PrefixEnabled(object.parentBucket, object.name),
				UserDefined: object.meta,
			})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	// Formulating the result data set to be expected from ListObjects call inside the tests,
	// This will be used in testCases and used for asserting the correctness of ListObjects output in the tests.

	resultCases := []ListObjectsInfo{
		// ListObjectsResult-0.
		// Testing for listing all objects in the bucket, (testCase 20,21,22).
		0: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-1.
		// Used for asserting the truncated case, (testCase 23).
		1: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
			},
		},
		// ListObjectsResult-2.
		// (TestCase 24).
		2: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
			},
		},
		// ListObjectsResult-3.
		// (TestCase 25).
		3: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},
		// ListObjectsResult-4.
		// Again used for truncated case.
		// (TestCase 26).
		4: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
			},
		},
		// ListObjectsResult-5.
		// Used for Asserting prefixes.
		// Used for test case with prefix "new", (testCase 27-29).
		5: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-6.
		// Used for Asserting prefixes.
		// Used for test case with prefix = "obj", (testCase 30).
		6: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-7.
		// Used for Asserting prefixes and truncation.
		// Used for test case with prefix = "new" and maxKeys = 1, (testCase 31).
		7: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
			},
		},
		// ListObjectsResult-8.
		// Used for Asserting prefixes.
		// Used for test case with prefix = "obj" and maxKeys = 2, (testCase 32).
		8: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "obj0"},
				{Name: "obj1"},
			},
		},
		// ListObjectsResult-9.
		// Used for asserting the case with marker, but without prefix.
		// marker is set to "newPrefix0" in the testCase, (testCase 33).
		9: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-10.
		// marker is set to "newPrefix1" in the testCase, (testCase 34).
		10: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-11.
		// marker is set to "obj0" in the testCase, (testCase 35).
		11: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-12.
		// Marker is set to "obj1" in the testCase, (testCase 36).
		12: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-13.
		// Marker is set to "man" in the testCase, (testCase37).
		13: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-14.
		// Marker is set to "Abc" in the testCase, (testCase 39).
		14: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-15.
		// Marker is set to "Asia/India/India-summer-photos-1" in the testCase, (testCase 40).
		15: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-16.
		// Marker is set to "Asia/India/Karnataka/Bangalore/Koramangala/pics" in the testCase, (testCase 41).
		16: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-17.
		// Used for asserting the case with marker, without prefix but with truncation.
		// Marker =  "newPrefix0" & maxKeys = 3 in the testCase, (testCase42).
		// Output truncated to 3 values.
		17: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
			},
		},
		// ListObjectsResult-18.
		// Marker = "newPrefix1" & maxkeys = 1 in the testCase, (testCase43).
		// Output truncated to 1 value.
		18: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-19.
		// Marker = "obj0" & maxKeys = 1 in the testCase, (testCase44).
		// Output truncated to 1 value.
		19: {
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "obj1"},
			},
		},
		// ListObjectsResult-20.
		// Marker = "obj0" & prefix = "obj" in the testCase, (testCase 45).
		20: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-21.
		// Marker = "obj1" & prefix = "obj" in the testCase, (testCase 46).
		21: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-22.
		// Marker = "newPrefix0" & prefix = "new" in the testCase,, (testCase 47).
		22: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-23.
		// Prefix is set to "Asia/India/" in the testCase, and delimiter is not set (testCase 55).
		23: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},

		// ListObjectsResult-24.
		// Prefix is set to "Asia" in the testCase, and delimiter is not set (testCase 56).
		24: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},

		// ListObjectsResult-25.
		// Prefix is set to "Asia" in the testCase, and delimiter is set (testCase 57).
		25: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
			},
			Prefixes: []string{"Asia/"},
		},
		// ListObjectsResult-26.
		// prefix = "new" and delimiter is set in the testCase.(testCase 58).
		26: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-27.
		// Prefix is set to "Asia/India/" in the testCase, and delimiter is set to forward slash '/' (testCase 59).
		27: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/India-summer-photos-1"},
			},
			Prefixes: []string{"Asia/India/Karnataka/"},
		},
		// ListObjectsResult-28.
		// Marker is set to "Asia/India/India-summer-photos-1" and delimiter set in the testCase, (testCase 60).
		28: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-29.
		// Marker is set to "Asia/India/Karnataka/Bangalore/Koramangala/pics" in the testCase and delimiter set, (testCase 61).
		29: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-30.
		// Prefix and Delimiter is set to '/', (testCase 62).
		30: {
			IsTruncated: false,
			Objects:     []ObjectInfo{},
		},
		// ListObjectsResult-31 Empty directory, recursive listing
		31: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
				{Name: "temporary/0/"},
			},
		},
		// ListObjectsResult-32 Empty directory, non recursive listing
		32: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"temporary/"},
		},
		// ListObjectsResult-33 Listing empty directory only
		33: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "temporary/0/"},
			},
		},
		// ListObjectsResult-34:
		//    * Listing with marker > last object should return empty
		//    * Listing an object with a trailing slash and '/' delimiter
		34: {
			IsTruncated: false,
			Objects:     []ObjectInfo{},
		},
		// ListObjectsResult-35 list with custom uncommon delimiter
		35: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "file1/receipt.json"},
			},
			Prefixes: []string{"file1/guidSplunk"},
		},
		// ListObjectsResult-36 list with nextmarker prefix and maxKeys set to 1.
		36: {
			IsTruncated: true,
			Prefixes:    []string{"dir/day_id=2017-10-10/"},
		},
		// ListObjectsResult-37 list with prefix match 2 levels deep
		37: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "foo/201910/1112"},
				{Name: "foo/201910/1122"},
			},
		},
		// ListObjectsResult-38 list with prefix match 1 level deep
		38: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "foo/201910/1112"},
				{Name: "foo/201910/1122"},
				{Name: "foo/201910/2112"},
				{Name: "foo/201910_txt"},
			},
		},
		// ListObjectsResult-39 list with prefix match 1 level deep
		39: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "201910/foo/bar/xl.meta/1.txt"},
			},
		},
		// ListObjectsResult-40
		40: {
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "aaa"},
				{Name: "ccc"},
			},
			Prefixes: []string{"bbb_"},
		},
	}

	testCases := []struct {
		// Inputs to ListObjects.
		bucketName string
		prefix     string
		marker     string
		delimiter  string
		maxKeys    int32
		// Expected output of ListObjects.
		result ListObjectsInfo
		err    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4 ).
		{".test", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Valid bucket names, but they do not exist (6-8).
		{"volatile-bucket-1", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// If marker is *after* the last possible object from the prefix it should return an empty list.
		{"test-bucket-list-object", "Asia", "europe-object", "", 0, ListObjectsInfo{}, nil, true},
		// If the marker is *before* the first possible object from the prefix it should return the first object.
		{"test-bucket-list-object", "Asia", "A", "", 1, resultCases[4], nil, true},
		// Setting a non-existing directory to be prefix (12-13).
		{"empty-bucket", "europe/france/", "", "", 1, ListObjectsInfo{}, nil, true},
		{"empty-bucket", "africa/tunisia/", "", "", 1, ListObjectsInfo{}, nil, true},
		// Testing on empty bucket, that is, bucket without any objects in it (14).
		{"empty-bucket", "", "", "", 0, ListObjectsInfo{}, nil, true},
		// Setting maxKeys to negative value (15-16).
		{"empty-bucket", "", "", "", -1, ListObjectsInfo{}, nil, true},
		{"empty-bucket", "", "", "", 1, ListObjectsInfo{}, nil, true},
		// Setting maxKeys to a very large value (17).
		{"empty-bucket", "", "", "", 111100000, ListObjectsInfo{}, nil, true},
		// Testing for all 10 objects in the bucket (18).
		{"test-bucket-list-object", "", "", "", 10, resultCases[0], nil, true},
		// Testing for negative value of maxKey, this should set maxKeys to listObjectsLimit (19).
		{"test-bucket-list-object", "", "", "", -1, resultCases[0], nil, true},
		// Testing for very large value of maxKey, this should set maxKeys to listObjectsLimit (20).
		{"test-bucket-list-object", "", "", "", 1234567890, resultCases[0], nil, true},
		// Testing for truncated value (21-24).
		{"test-bucket-list-object", "", "", "", 5, resultCases[1], nil, true},
		{"test-bucket-list-object", "", "", "", 4, resultCases[2], nil, true},
		{"test-bucket-list-object", "", "", "", 3, resultCases[3], nil, true},
		{"test-bucket-list-object", "", "", "", 1, resultCases[4], nil, true},
		// Testing with prefix (25-28).
		{"test-bucket-list-object", "new", "", "", 3, resultCases[5], nil, true},
		{"test-bucket-list-object", "new", "", "", 4, resultCases[5], nil, true},
		{"test-bucket-list-object", "new", "", "", 5, resultCases[5], nil, true},
		{"test-bucket-list-object", "obj", "", "", 3, resultCases[6], nil, true},
		{"test-bucket-list-object", "/obj", "", "", 0, ListObjectsInfo{}, nil, true},
		// Testing with prefix and truncation (29-30).
		{"test-bucket-list-object", "new", "", "", 1, resultCases[7], nil, true},
		{"test-bucket-list-object", "obj", "", "", 2, resultCases[8], nil, true},
		// Testing with marker, but without prefix and truncation (31-35).
		{"test-bucket-list-object", "", "newPrefix0", "", 6, resultCases[9], nil, true},
		{"test-bucket-list-object", "", "newPrefix1", "", 5, resultCases[10], nil, true},
		{"test-bucket-list-object", "", "obj0", "", 4, resultCases[11], nil, true},
		{"test-bucket-list-object", "", "obj1", "", 2, resultCases[12], nil, true},
		{"test-bucket-list-object", "", "man", "", 11, resultCases[13], nil, true},
		// Marker being set to a value which is greater than and all object names when sorted (36).
		// Expected to send an empty response in this case.
		{"test-bucket-list-object", "", "zen", "", 10, ListObjectsInfo{}, nil, true},
		// Marker being set to a value which is lesser than and all object names when sorted (37).
		// Expected to send all the objects in the bucket in this case.
		{"test-bucket-list-object", "", "Abc", "", 10, resultCases[14], nil, true},
		// Marker is to a hierarchical value (38-39).
		{"test-bucket-list-object", "", "Asia/India/India-summer-photos-1", "", 10, resultCases[15], nil, true},
		{"test-bucket-list-object", "", "Asia/India/Karnataka/Bangalore/Koramangala/pics", "", 10, resultCases[16], nil, true},
		// Testing with marker and truncation, but no prefix (40-42).
		{"test-bucket-list-object", "", "newPrefix0", "", 3, resultCases[17], nil, true},
		{"test-bucket-list-object", "", "newPrefix1", "", 1, resultCases[18], nil, true},
		{"test-bucket-list-object", "", "obj0", "", 1, resultCases[19], nil, true},
		// Testing with both marker and prefix, but without truncation (43-45).
		// The valid combination of marker and prefix should satisfy strings.HasPrefix(marker, prefix).
		{"test-bucket-list-object", "obj", "obj0", "", 2, resultCases[20], nil, true},
		{"test-bucket-list-object", "obj", "obj1", "", 1, resultCases[21], nil, true},
		{"test-bucket-list-object", "new", "newPrefix0", "", 2, resultCases[22], nil, true},
		// Testing with maxKeys set to 0 (46-52).
		// The parameters have to valid.
		{"test-bucket-list-object", "", "obj1", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "", "obj0", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "new", "", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "obj0", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "obj1", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "new", "newPrefix0", "", 0, ListObjectsInfo{}, nil, true},
		// Tests on hierarchical key names as prefix.
		// Without delimteter the code should recurse into the prefix Dir.
		// Tests with prefix, but without delimiter (53-54).
		{"test-bucket-list-object", "Asia/India/", "", "", 10, resultCases[23], nil, true},
		{"test-bucket-list-object", "Asia", "", "", 10, resultCases[24], nil, true},
		// Tests with prefix and delimiter (55-57).
		// With delimiter the code should not recurse into the sub-directories of prefix Dir.
		{"test-bucket-list-object", "Asia", "", SlashSeparator, 10, resultCases[25], nil, true},
		{"test-bucket-list-object", "new", "", SlashSeparator, 10, resultCases[26], nil, true},
		{"test-bucket-list-object", "Asia/India/", "", SlashSeparator, 10, resultCases[27], nil, true},
		// Test with marker set as hierarchical value and with delimiter. (58-59)
		{"test-bucket-list-object", "", "Asia/India/India-summer-photos-1", SlashSeparator, 10, resultCases[28], nil, true},
		{"test-bucket-list-object", "", "Asia/India/Karnataka/Bangalore/Koramangala/pics", SlashSeparator, 10, resultCases[29], nil, true},
		// Test with prefix and delimiter set to '/'. (60)
		{"test-bucket-list-object", SlashSeparator, "", SlashSeparator, 10, resultCases[30], nil, true},
		// Test with invalid prefix (61)
		{"test-bucket-list-object", "\\", "", SlashSeparator, 10, ListObjectsInfo{}, nil, true},
		// Test listing an empty directory in recursive mode (62)
		{"test-bucket-empty-dir", "", "", "", 10, resultCases[31], nil, true},
		// Test listing an empty directory in a non recursive mode (63)
		{"test-bucket-empty-dir", "", "", SlashSeparator, 10, resultCases[32], nil, true},
		// Test listing a directory which contains an empty directory (64)
		{"test-bucket-empty-dir", "", "temporary/", "", 10, resultCases[33], nil, true},
		// Test listing with marker > last object such that response should be empty (65)
		{"test-bucket-single-object", "", "A/C", "", 1000, resultCases[34], nil, true},
		// Test listing an object with a trailing slash and a slash delimiter (66)
		{"test-bucket-list-object", "Asia-maps.png/", "", "/", 1000, resultCases[34], nil, true},
		// Test listing an object with uncommon delimiter
		{testBuckets[4], "", "", "guidSplunk", 1000, resultCases[35], nil, true},
		// Test listing an object with uncommon delimiter and matching prefix
		{testBuckets[4], "file1/", "", "guidSplunk", 1000, resultCases[35], nil, true},
		// Test listing at prefix with expected prefix markers
		{testBuckets[5], "dir/", "", SlashSeparator, 1, resultCases[36], nil, true},
		// Test listing with prefix match
		{testBuckets[5], "foo/201910/11", "", "", 1000, resultCases[37], nil, true},
		{testBuckets[5], "foo/201910", "", "", 1000, resultCases[38], nil, true},
		// Test listing with prefix match with 'xl.meta'
		{testBuckets[5], "201910/foo/bar", "", "", 1000, resultCases[39], nil, true},
		// Test listing with custom prefix
		{testBuckets[6], "", "", "_", 1000, resultCases[40], nil, true},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s-Test%d", instanceType, i+1), func(t *testing.T) {
			t.Log("ListObjects, bucket:", testCase.bucketName, "prefix:", testCase.prefix, "marker:", testCase.marker, "delimiter:", testCase.delimiter, "maxkeys:", testCase.maxKeys)
			result, err := obj.ListObjects(t.Context(), testCase.bucketName,
				testCase.prefix, testCase.marker, testCase.delimiter, int(testCase.maxKeys))
			if err != nil && testCase.shouldPass {
				t.Errorf("Test %d: %s:  Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
			}
			if err == nil && !testCase.shouldPass {
				t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.err.Error())
			}
			// Failed as expected, but does it fail for the expected reason.
			if err != nil && !testCase.shouldPass {
				if !strings.Contains(err.Error(), testCase.err.Error()) {
					t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.err.Error(), err.Error())
				}
			}
			// Since there are cases for which ListObjects fails, this is
			// necessary. Test passes as expected, but the output values
			// are verified for correctness here.
			if err == nil && testCase.shouldPass {
				// The length of the expected ListObjectsResult.Objects
				// should match in both expected result from test cases
				// and in the output. On failure calling t.Fatalf,
				// otherwise it may lead to index out of range error in
				// assertion following this.
				if len(testCase.result.Objects) != len(result.Objects) {
					t.Logf("want: %v", objInfoNames(testCase.result.Objects))
					t.Logf("got: %v", objInfoNames(result.Objects))
					t.Errorf("Test %d: %s: Expected number of object in the result to be '%d', but found '%d' objects instead", i+1, instanceType, len(testCase.result.Objects), len(result.Objects))
				}
				for j := 0; j < len(testCase.result.Objects); j++ {
					if j >= len(result.Objects) {
						t.Errorf("Test %d: %s: Expected object name to be \"%s\", but not nothing instead", i+1, instanceType, testCase.result.Objects[j].Name)
						continue
					}
					if testCase.result.Objects[j].Name != result.Objects[j].Name {
						t.Errorf("Test %d: %s: Expected object name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.result.Objects[j].Name, result.Objects[j].Name)
					}
				}

				if len(testCase.result.Prefixes) != len(result.Prefixes) {
					t.Logf("want: %v", testCase.result.Prefixes)
					t.Logf("got: %v", result.Prefixes)
					t.Errorf("Test %d: %s: Expected number of prefixes in the result to be '%d', but found '%d' prefixes instead", i+1, instanceType, len(testCase.result.Prefixes), len(result.Prefixes))
				}
				for j := 0; j < len(testCase.result.Prefixes); j++ {
					if j >= len(result.Prefixes) {
						t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found no result", i+1, instanceType, testCase.result.Prefixes[j])
						continue
					}
					if testCase.result.Prefixes[j] != result.Prefixes[j] {
						t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.result.Prefixes[j], result.Prefixes[j])
					}
				}

				if testCase.result.IsTruncated != result.IsTruncated {
					// Allow an extra continuation token.
					if !result.IsTruncated || len(result.Objects) == 0 {
						t.Errorf("Test %d: %s: Expected IsTruncated flag to be %v, but instead found it to be %v", i+1, instanceType, testCase.result.IsTruncated, result.IsTruncated)
					}
				}

				if testCase.result.IsTruncated && result.NextMarker == "" {
					t.Errorf("Test %d: %s: Expected NextMarker to contain a string since listing is truncated, but instead found it to be empty", i+1, instanceType)
				}

				if !testCase.result.IsTruncated && result.NextMarker != "" {
					if !result.IsTruncated || len(result.Objects) == 0 {
						t.Errorf("Test %d: %s: Expected NextMarker to be empty since listing is not truncated, but instead found `%v`", i+1, instanceType, result.NextMarker)
					}
				}
			}
		})
	}
}

func objInfoNames(o []ObjectInfo) []string {
	res := make([]string, len(o))
	for i := range o {
		res[i] = o[i].Name
	}
	return res
}

func TestDeleteObjectVersionMarker(t *testing.T) {
	ExecObjectLayerTest(t, testDeleteObjectVersion)
}

func testDeleteObjectVersion(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	t, _ := t1.(*testing.T)

	testBuckets := []string{
		"bucket-suspended-version",
		"bucket-suspended-version-id",
	}
	for _, bucket := range testBuckets {
		err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{
			VersioningEnabled: true,
		})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
		meta, err := loadBucketMetadata(context.Background(), obj, bucket)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
		meta.VersioningConfigXML = []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Suspended</Status></VersioningConfiguration>`)
		if err := meta.Save(context.Background(), obj); err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
		globalBucketMetadataSys.Set(bucket, meta)
		globalNotificationSys.LoadBucketMetadata(context.Background(), bucket)
	}

	testObjects := []struct {
		parentBucket    string
		name            string
		content         string
		meta            map[string]string
		versionID       string
		expectDelMarker bool
	}{
		{testBuckets[0], "delete-file", "contentstring", nil, "", true},
		{testBuckets[1], "delete-file", "contentstring", nil, "null", false},
	}
	for _, object := range testObjects {
		md5Bytes := md5.Sum([]byte(object.content))
		_, err := obj.PutObject(context.Background(), object.parentBucket, object.name,
			mustGetPutObjReader(t, bytes.NewBufferString(object.content),
				int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{
				Versioned:        globalBucketVersioningSys.PrefixEnabled(object.parentBucket, object.name),
				VersionSuspended: globalBucketVersioningSys.PrefixSuspended(object.parentBucket, object.name),
				UserDefined:      object.meta,
			})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
		obj, err := obj.DeleteObject(context.Background(), object.parentBucket, object.name, ObjectOptions{
			Versioned:        globalBucketVersioningSys.PrefixEnabled(object.parentBucket, object.name),
			VersionSuspended: globalBucketVersioningSys.PrefixSuspended(object.parentBucket, object.name),
			VersionID:        object.versionID,
		})
		if err != nil {
			if object.versionID != "" {
				if !isErrVersionNotFound(err) {
					t.Fatalf("%s : %s", instanceType, err)
				}
			} else {
				if !isErrObjectNotFound(err) {
					t.Fatalf("%s : %s", instanceType, err)
				}
			}
		}
		if obj.DeleteMarker != object.expectDelMarker {
			t.Fatalf("%s : expected deleted marker %t, found %t", instanceType, object.expectDelMarker, obj.DeleteMarker)
		}
	}
}

// Wrapper for calling ListObjectVersions tests for both Erasure multiple disks and single node setup.
func TestListObjectVersions(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectVersions)
}

// Unit test for ListObjectVersions
func testListObjectVersions(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	t, _ := t1.(*testing.T)
	testBuckets := []string{
		// This bucket is used for testing ListObject operations.
		"test-bucket-list-object",
		// This bucket will be tested with empty directories
		"test-bucket-empty-dir",
		// Will not store any objects in this bucket,
		// Its to test ListObjects on an empty bucket.
		"empty-bucket",
		// Listing the case where the marker > last object.
		"test-bucket-single-object",
		// Listing uncommon delimiter.
		"test-bucket-delimiter",
		// Listing prefixes > maxKeys
		"test-bucket-max-keys-prefixes",
	}
	for _, bucket := range testBuckets {
		err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{VersioningEnabled: true})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	var err error
	testObjects := []struct {
		parentBucket string
		name         string
		content      string
		meta         map[string]string
	}{
		{testBuckets[0], "Asia-maps.png", "asis-maps", map[string]string{"content-type": "image/png"}},
		{testBuckets[0], "Asia/India/India-summer-photos-1", "contentstring", nil},
		{testBuckets[0], "Asia/India/Karnataka/Bangalore/Koramangala/pics", "contentstring", nil},
		{testBuckets[0], "newPrefix0", "newPrefix0", nil},
		{testBuckets[0], "newPrefix1", "newPrefix1", nil},
		{testBuckets[0], "newzen/zen/recurse/again/again/again/pics", "recurse", nil},
		{testBuckets[0], "obj0", "obj0", nil},
		{testBuckets[0], "obj1", "obj1", nil},
		{testBuckets[0], "obj2", "obj2", nil},
		{testBuckets[1], "obj1", "obj1", nil},
		{testBuckets[1], "obj2", "obj2", nil},
		{testBuckets[1], "temporary/0/", "", nil},
		{testBuckets[3], "A/B", "contentstring", nil},
		{testBuckets[4], "file1/receipt.json", "content", nil},
		{testBuckets[4], "file1/guidSplunk-aaaa/file", "content", nil},
		{testBuckets[5], "dir/day_id=2017-10-10/issue", "content", nil},
		{testBuckets[5], "dir/day_id=2017-10-11/issue", "content", nil},
	}

	for _, object := range testObjects {
		md5Bytes := md5.Sum([]byte(object.content))
		_, err = obj.PutObject(context.Background(), object.parentBucket, object.name, mustGetPutObjReader(t, bytes.NewBufferString(object.content),
			int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{UserDefined: object.meta})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	// Formulating the result data set to be expected from ListObjects call inside the tests,
	// This will be used in testCases and used for asserting the correctness of ListObjects output in the tests.

	resultCases := []ListObjectsInfo{
		// ListObjectsResult-0.
		// Testing for listing all objects in the bucket, (testCase 20,21,22).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-1.
		// Used for asserting the truncated case, (testCase 23).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
			},
		},
		// ListObjectsResult-2.
		// (TestCase 24).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
			},
		},
		// ListObjectsResult-3.
		// (TestCase 25).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},
		// ListObjectsResult-4.
		// Again used for truncated case.
		// (TestCase 26).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
			},
		},
		// ListObjectsResult-5.
		// Used for Asserting prefixes.
		// Used for test case with prefix "new", (testCase 27-29).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-6.
		// Used for Asserting prefixes.
		// Used for test case with prefix = "obj", (testCase 30).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-7.
		// Used for Asserting prefixes and truncation.
		// Used for test case with prefix = "new" and maxKeys = 1, (testCase 31).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
			},
		},
		// ListObjectsResult-8.
		// Used for Asserting prefixes.
		// Used for test case with prefix = "obj" and maxKeys = 2, (testCase 32).
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "obj0"},
				{Name: "obj1"},
			},
		},
		// ListObjectsResult-9.
		// Used for asserting the case with marker, but without prefix.
		// marker is set to "newPrefix0" in the testCase, (testCase 33).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-10.
		// marker is set to "newPrefix1" in the testCase, (testCase 34).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-11.
		// marker is set to "obj0" in the testCase, (testCase 35).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-12.
		// Marker is set to "obj1" in the testCase, (testCase 36).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-13.
		// Marker is set to "man" in the testCase, (testCase37).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-14.
		// Marker is set to "Abc" in the testCase, (testCase 39).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-15.
		// Marker is set to "Asia/India/India-summer-photos-1" in the testCase, (testCase 40).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-16.
		// Marker is set to "Asia/India/Karnataka/Bangalore/Koramangala/pics" in the testCase, (testCase 41).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-17.
		// Used for asserting the case with marker, without prefix but with truncation.
		// Marker =  "newPrefix0" & maxKeys = 3 in the testCase, (testCase42).
		// Output truncated to 3 values.
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
				{Name: "obj0"},
			},
		},
		// ListObjectsResult-18.
		// Marker = "newPrefix1" & maxkeys = 1 in the testCase, (testCase43).
		// Output truncated to 1 value.
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-19.
		// Marker = "obj0" & maxKeys = 1 in the testCase, (testCase44).
		// Output truncated to 1 value.
		{
			IsTruncated: true,
			Objects: []ObjectInfo{
				{Name: "obj1"},
			},
		},
		// ListObjectsResult-20.
		// Marker = "obj0" & prefix = "obj" in the testCase, (testCase 45).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-21.
		// Marker = "obj1" & prefix = "obj" in the testCase, (testCase 46).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj2"},
			},
		},
		// ListObjectsResult-22.
		// Marker = "newPrefix0" & prefix = "new" in the testCase,, (testCase 47).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix1"},
				{Name: "newzen/zen/recurse/again/again/again/pics"},
			},
		},
		// ListObjectsResult-23.
		// Prefix is set to "Asia/India/" in the testCase, and delimiter is not set (testCase 55).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},

		// ListObjectsResult-24.
		// Prefix is set to "Asia" in the testCase, and delimiter is not set (testCase 56).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
				{Name: "Asia/India/India-summer-photos-1"},
				{Name: "Asia/India/Karnataka/Bangalore/Koramangala/pics"},
			},
		},

		// ListObjectsResult-25.
		// Prefix is set to "Asia" in the testCase, and delimiter is set (testCase 57).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia-maps.png"},
			},
			Prefixes: []string{"Asia/"},
		},
		// ListObjectsResult-26.
		// prefix = "new" and delimiter is set in the testCase.(testCase 58).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-27.
		// Prefix is set to "Asia/India/" in the testCase, and delimiter is set to forward slash '/' (testCase 59).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "Asia/India/India-summer-photos-1"},
			},
			Prefixes: []string{"Asia/India/Karnataka/"},
		},
		// ListObjectsResult-28.
		// Marker is set to "Asia/India/India-summer-photos-1" and delimiter set in the testCase, (testCase 60).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-29.
		// Marker is set to "Asia/India/Karnataka/Bangalore/Koramangala/pics" in the testCase and delimiter set, (testCase 61).
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "newPrefix0"},
				{Name: "newPrefix1"},
				{Name: "obj0"},
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"newzen/"},
		},
		// ListObjectsResult-30.
		// Prefix and Delimiter is set to '/', (testCase 62).
		{
			IsTruncated: false,
			Objects:     []ObjectInfo{},
		},
		// ListObjectsResult-31 Empty directory, recursive listing
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
				{Name: "temporary/0/"},
			},
		},
		// ListObjectsResult-32 Empty directory, non recursive listing
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "obj1"},
				{Name: "obj2"},
			},
			Prefixes: []string{"temporary/"},
		},
		// ListObjectsResult-33 Listing empty directory only
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "temporary/0/"},
			},
		},
		// ListObjectsResult-34:
		//    * Listing with marker > last object should return empty
		//    * Listing an object with a trailing slash and '/' delimiter
		{
			IsTruncated: false,
			Objects:     []ObjectInfo{},
		},
		// ListObjectsResult-35 list with custom uncommon delimiter
		{
			IsTruncated: false,
			Objects: []ObjectInfo{
				{Name: "file1/receipt.json"},
			},
			Prefixes: []string{"file1/guidSplunk"},
		},
		// ListObjectsResult-36 list with nextmarker prefix and maxKeys set to 1.
		{
			IsTruncated: true,
			Prefixes:    []string{"dir/day_id=2017-10-10/"},
		},
	}

	testCases := []struct {
		// Inputs to ListObjects.
		bucketName string
		prefix     string
		marker     string
		delimiter  string
		maxKeys    int32
		// Expected output of ListObjects.
		result ListObjectsInfo
		err    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4).
		{".test", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", "", 0, ListObjectsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Valid bucket names, but they do not exist (6-8).
		{"volatile-bucket-1", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "", "", "", 1000, ListObjectsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// If marker is *after* the last possible object from the prefix it should return an empty list.
		{"test-bucket-list-object", "Asia", "europe-object", "", 0, ListObjectsInfo{}, nil, true},
		// Setting a non-existing directory to be prefix (10-11).
		{"empty-bucket", "europe/france/", "", "", 1, ListObjectsInfo{}, nil, true},
		{"empty-bucket", "africa/tunisia/", "", "", 1, ListObjectsInfo{}, nil, true},
		// Testing on empty bucket, that is, bucket without any objects in it (12).
		{"empty-bucket", "", "", "", 0, ListObjectsInfo{}, nil, true},
		// Setting maxKeys to negative value (13-14).
		{"empty-bucket", "", "", "", -1, ListObjectsInfo{}, nil, true},
		{"empty-bucket", "", "", "", 1, ListObjectsInfo{}, nil, true},
		// Setting maxKeys to a very large value (15).
		{"empty-bucket", "", "", "", 111100000, ListObjectsInfo{}, nil, true},
		// Testing for all 10 objects in the bucket (16).
		{"test-bucket-list-object", "", "", "", 10, resultCases[0], nil, true},
		// Testing for negative value of maxKey, this should set maxKeys to listObjectsLimit (17).
		{"test-bucket-list-object", "", "", "", -1, resultCases[0], nil, true},
		// Testing for very large value of maxKey, this should set maxKeys to listObjectsLimit (18).
		{"test-bucket-list-object", "", "", "", 1234567890, resultCases[0], nil, true},
		// Testing for truncated value (19-22).
		{"test-bucket-list-object", "", "", "", 5, resultCases[1], nil, true},
		{"test-bucket-list-object", "", "", "", 4, resultCases[2], nil, true},
		{"test-bucket-list-object", "", "", "", 3, resultCases[3], nil, true},
		{"test-bucket-list-object", "", "", "", 1, resultCases[4], nil, true},
		// Testing with prefix (23-26).
		{"test-bucket-list-object", "new", "", "", 3, resultCases[5], nil, true},
		{"test-bucket-list-object", "new", "", "", 4, resultCases[5], nil, true},
		{"test-bucket-list-object", "new", "", "", 5, resultCases[5], nil, true},
		{"test-bucket-list-object", "obj", "", "", 3, resultCases[6], nil, true},
		// Testing with prefix and truncation (27-28).
		{"test-bucket-list-object", "new", "", "", 1, resultCases[7], nil, true},
		{"test-bucket-list-object", "obj", "", "", 2, resultCases[8], nil, true},
		// Testing with marker, but without prefix and truncation (29-33).
		{"test-bucket-list-object", "", "newPrefix0", "", 6, resultCases[9], nil, true},
		{"test-bucket-list-object", "", "newPrefix1", "", 5, resultCases[10], nil, true},
		{"test-bucket-list-object", "", "obj0", "", 4, resultCases[11], nil, true},
		{"test-bucket-list-object", "", "obj1", "", 2, resultCases[12], nil, true},
		{"test-bucket-list-object", "", "man", "", 11, resultCases[13], nil, true},
		// Marker being set to a value which is greater than and all object names when sorted (34).
		// Expected to send an empty response in this case.
		{"test-bucket-list-object", "", "zen", "", 10, ListObjectsInfo{}, nil, true},
		// Marker being set to a value which is lesser than and all object names when sorted (35).
		// Expected to send all the objects in the bucket in this case.
		{"test-bucket-list-object", "", "Abc", "", 10, resultCases[14], nil, true},
		// Marker is to a hierarchical value (36-37).
		{"test-bucket-list-object", "", "Asia/India/India-summer-photos-1", "", 10, resultCases[15], nil, true},
		{"test-bucket-list-object", "", "Asia/India/Karnataka/Bangalore/Koramangala/pics", "", 10, resultCases[16], nil, true},
		// Testing with marker and truncation, but no prefix (38-40).
		{"test-bucket-list-object", "", "newPrefix0", "", 3, resultCases[17], nil, true},
		{"test-bucket-list-object", "", "newPrefix1", "", 1, resultCases[18], nil, true},
		{"test-bucket-list-object", "", "obj0", "", 1, resultCases[19], nil, true},
		// Testing with both marker and prefix, but without truncation (41-43).
		// The valid combination of marker and prefix should satisfy strings.HasPrefix(marker, prefix).
		{"test-bucket-list-object", "obj", "obj0", "", 2, resultCases[20], nil, true},
		{"test-bucket-list-object", "obj", "obj1", "", 1, resultCases[21], nil, true},
		{"test-bucket-list-object", "new", "newPrefix0", "", 2, resultCases[22], nil, true},
		// Testing with maxKeys set to 0 (44-50).
		// The parameters have to valid.
		{"test-bucket-list-object", "", "obj1", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "", "obj0", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "new", "", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "obj0", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "obj", "obj1", "", 0, ListObjectsInfo{}, nil, true},
		{"test-bucket-list-object", "new", "newPrefix0", "", 0, ListObjectsInfo{}, nil, true},
		// Tests on hierarchical key names as prefix.
		// Without delimteter the code should recurse into the prefix Dir.
		// Tests with prefix, but without delimiter (51-52).
		{"test-bucket-list-object", "Asia/India/", "", "", 10, resultCases[23], nil, true},
		{"test-bucket-list-object", "Asia", "", "", 10, resultCases[24], nil, true},
		// Tests with prefix and delimiter (53-55).
		// With delimiter the code should not recurse into the sub-directories of prefix Dir.
		{"test-bucket-list-object", "Asia", "", SlashSeparator, 10, resultCases[25], nil, true},
		{"test-bucket-list-object", "new", "", SlashSeparator, 10, resultCases[26], nil, true},
		{"test-bucket-list-object", "Asia/India/", "", SlashSeparator, 10, resultCases[27], nil, true},
		// Test with marker set as hierarchical value and with delimiter. (56-57)
		{"test-bucket-list-object", "", "Asia/India/India-summer-photos-1", SlashSeparator, 10, resultCases[28], nil, true},
		{"test-bucket-list-object", "", "Asia/India/Karnataka/Bangalore/Koramangala/pics", SlashSeparator, 10, resultCases[29], nil, true},
		// Test with prefix and delimiter set to '/'. (58)
		{"test-bucket-list-object", SlashSeparator, "", SlashSeparator, 10, resultCases[30], nil, true},
		// Test with invalid prefix (59)
		{"test-bucket-list-object", "\\", "", SlashSeparator, 10, ListObjectsInfo{}, nil, true},
		// Test listing an empty directory in recursive mode (60)
		{"test-bucket-empty-dir", "", "", "", 10, resultCases[31], nil, true},
		// Test listing an empty directory in a non recursive mode (61)
		{"test-bucket-empty-dir", "", "", SlashSeparator, 10, resultCases[32], nil, true},
		// Test listing a directory which contains an empty directory (62)
		{"test-bucket-empty-dir", "", "temporary/", "", 10, resultCases[33], nil, true},
		// Test listing with marker > last object such that response should be empty (63)
		{"test-bucket-single-object", "", "A/C", "", 1000, resultCases[34], nil, true},
		// Test listing an object with a trailing slash and a slash delimiter (64)
		{"test-bucket-list-object", "Asia-maps.png/", "", "/", 1000, resultCases[34], nil, true},
		// Test listing an object with uncommon delimiter
		{testBuckets[4], "", "", "guidSplunk", 1000, resultCases[35], nil, true},
		// Test listing an object with uncommon delimiter and matching prefix
		{testBuckets[4], "file1/", "", "guidSplunk", 1000, resultCases[35], nil, true},
		// Test listing at prefix with expected prefix markers
		{testBuckets[5], "dir/", "", SlashSeparator, 1, resultCases[36], nil, true},
		{"test-bucket-list-object", "Asia", "A", "", 1, resultCases[4], nil, true},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s-Test%d", instanceType, i+1), func(t *testing.T) {
			result, err := obj.ListObjectVersions(t.Context(), testCase.bucketName,
				testCase.prefix, testCase.marker, "", testCase.delimiter, int(testCase.maxKeys))
			if err != nil && testCase.shouldPass {
				t.Errorf("%s:  Expected to pass, but failed with: <ERROR> %s", instanceType, err.Error())
			}
			if err == nil && !testCase.shouldPass {
				t.Errorf("%s: Expected to fail with <ERROR> \"%s\", but passed instead", instanceType, testCase.err.Error())
			}
			// Failed as expected, but does it fail for the expected reason.
			if err != nil && !testCase.shouldPass {
				if !strings.Contains(err.Error(), testCase.err.Error()) {
					t.Errorf("%s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", instanceType, testCase.err.Error(), err.Error())
				}
			}
			// Since there are cases for which ListObjects fails, this is
			// necessary. Test passes as expected, but the output values
			// are verified for correctness here.
			if err == nil && testCase.shouldPass {
				// The length of the expected ListObjectsResult.Objects
				// should match in both expected result from test cases
				// and in the output. On failure calling t.Fatalf,
				// otherwise it may lead to index out of range error in
				// assertion following this.
				if len(testCase.result.Objects) != len(result.Objects) {
					t.Fatalf("%s: Expected number of object in the result to be '%d', but found '%d' objects instead", instanceType, len(testCase.result.Objects), len(result.Objects))
				}
				for j := 0; j < len(testCase.result.Objects); j++ {
					if testCase.result.Objects[j].Name != result.Objects[j].Name {
						t.Errorf("%s: Expected object name to be \"%s\", but found \"%s\" instead", instanceType, testCase.result.Objects[j].Name, result.Objects[j].Name)
					}
				}

				if len(testCase.result.Prefixes) != len(result.Prefixes) {
					t.Log(testCase, testCase.result.Prefixes, result.Prefixes)
					t.Fatalf("%s: Expected number of prefixes in the result to be '%d', but found '%d' prefixes instead", instanceType, len(testCase.result.Prefixes), len(result.Prefixes))
				}
				for j := 0; j < len(testCase.result.Prefixes); j++ {
					if testCase.result.Prefixes[j] != result.Prefixes[j] {
						t.Errorf("%s: Expected prefix name to be \"%s\", but found \"%s\" instead", instanceType, testCase.result.Prefixes[j], result.Prefixes[j])
					}
				}

				if testCase.result.IsTruncated != result.IsTruncated {
					// Allow an extra continuation token.
					if !result.IsTruncated || len(result.Objects) == 0 {
						t.Errorf("%s: Expected IsTruncated flag to be %v, but instead found it to be %v", instanceType, testCase.result.IsTruncated, result.IsTruncated)
					}
				}

				if testCase.result.IsTruncated && result.NextMarker == "" {
					t.Errorf("%s: Expected NextMarker to contain a string since listing is truncated, but instead found it to be empty", instanceType)
				}

				if !testCase.result.IsTruncated && result.NextMarker != "" {
					if !result.IsTruncated || len(result.Objects) == 0 {
						t.Errorf("%s: Expected NextMarker to be empty since listing is not truncated, but instead found `%v`", instanceType, result.NextMarker)
					}
				}
			}
		})
	}
}

// Wrapper for calling ListObjects continuation tests for both Erasure multiple disks and single node setup.
func TestListObjectsContinuation(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsContinuation)
}

// Unit test for ListObjects in general.
func testListObjectsContinuation(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	t, _ := t1.(*testing.T)
	testBuckets := []string{
		// This bucket is used for testing ListObject operations.
		"test-bucket-list-object-continuation-1",
		"test-bucket-list-object-continuation-2",
	}
	for _, bucket := range testBuckets {
		err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	var err error
	testObjects := []struct {
		parentBucket string
		name         string
		content      string
		meta         map[string]string
	}{
		{testBuckets[0], "a/1.txt", "contentstring", nil},
		{testBuckets[0], "a-1.txt", "contentstring", nil},
		{testBuckets[0], "a.txt", "contentstring", nil},
		{testBuckets[0], "apache2-doc/1.txt", "contentstring", nil},
		{testBuckets[0], "apache2/1.txt", "contentstring", nil},
		{testBuckets[0], "apache2/-sub/2.txt", "contentstring", nil},
		{testBuckets[1], "azerty/1.txt", "contentstring", nil},
		{testBuckets[1], "apache2-doc/1.txt", "contentstring", nil},
		{testBuckets[1], "apache2/1.txt", "contentstring", nil},
	}
	for _, object := range testObjects {
		md5Bytes := md5.Sum([]byte(object.content))
		_, err = obj.PutObject(context.Background(), object.parentBucket, object.name, mustGetPutObjReader(t, bytes.NewBufferString(object.content),
			int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{UserDefined: object.meta})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	// Formulating the result data set to be expected from ListObjects call inside the tests,
	// This will be used in testCases and used for asserting the correctness of ListObjects output in the tests.
	resultCases := []ListObjectsInfo{
		{
			Objects: []ObjectInfo{
				{Name: "a-1.txt"},
				{Name: "a.txt"},
				{Name: "a/1.txt"},
				{Name: "apache2-doc/1.txt"},
				{Name: "apache2/-sub/2.txt"},
				{Name: "apache2/1.txt"},
			},
		},
		{
			Objects: []ObjectInfo{
				{Name: "apache2-doc/1.txt"},
				{Name: "apache2/1.txt"},
			},
		},
		{
			Prefixes: []string{"apache2-doc/", "apache2/", "azerty/"},
		},
	}

	testCases := []struct {
		// Inputs to ListObjects.
		bucketName string
		prefix     string
		delimiter  string
		page       int
		// Expected output of ListObjects.
		result ListObjectsInfo
	}{
		{testBuckets[0], "", "", 1, resultCases[0]},
		{testBuckets[0], "a", "", 1, resultCases[0]},
		{testBuckets[1], "apache", "", 1, resultCases[1]},
		{testBuckets[1], "", "/", 1, resultCases[2]},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s-Test%d", instanceType, i+1), func(t *testing.T) {
			var foundObjects []ObjectInfo
			var foundPrefixes []string
			marker := ""
			for {
				result, err := obj.ListObjects(t.Context(), testCase.bucketName,
					testCase.prefix, marker, testCase.delimiter, testCase.page)
				if err != nil {
					t.Fatalf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
				}
				foundObjects = append(foundObjects, result.Objects...)
				foundPrefixes = append(foundPrefixes, result.Prefixes...)
				if !result.IsTruncated {
					break
				}
				marker = result.NextMarker
				if len(result.Objects) > 0 {
					// Discard marker, so it cannot resume listing.
					marker = result.Objects[len(result.Objects)-1].Name
				}
			}

			if len(testCase.result.Objects) != len(foundObjects) {
				t.Logf("want: %v", objInfoNames(testCase.result.Objects))
				t.Logf("got: %v", objInfoNames(foundObjects))
				t.Errorf("Test %d: %s: Expected number of objects in the result to be '%d', but found '%d' objects instead",
					i+1, instanceType, len(testCase.result.Objects), len(foundObjects))
			}
			for j := 0; j < len(testCase.result.Objects); j++ {
				if j >= len(foundObjects) {
					t.Errorf("Test %d: %s: Expected object name to be \"%s\", but not nothing instead", i+1, instanceType, testCase.result.Objects[j].Name)
					continue
				}
				if testCase.result.Objects[j].Name != foundObjects[j].Name {
					t.Errorf("Test %d: %s: Expected object name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.result.Objects[j].Name, foundObjects[j].Name)
				}
			}

			if len(testCase.result.Prefixes) != len(foundPrefixes) {
				t.Logf("want: %v", testCase.result.Prefixes)
				t.Logf("got: %v", foundPrefixes)
				t.Errorf("Test %d: %s: Expected number of prefixes in the result to be '%d', but found '%d' prefixes instead",
					i+1, instanceType, len(testCase.result.Prefixes), len(foundPrefixes))
			}
			for j := 0; j < len(testCase.result.Prefixes); j++ {
				if j >= len(foundPrefixes) {
					t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found no result", i+1, instanceType, testCase.result.Prefixes[j])
					continue
				}
				if testCase.result.Prefixes[j] != foundPrefixes[j] {
					t.Errorf("Test %d: %s: Expected prefix name to be \"%s\", but found \"%s\" instead", i+1, instanceType, testCase.result.Prefixes[j], foundPrefixes[j])
				}
			}
		})
	}
}

// Initialize FS backend for the benchmark.
func initFSObjectsB(disk string, t *testing.B) (obj ObjectLayer) {
	obj, _, err := initObjectLayer(context.Background(), mustGetPoolEndpoints(0, disk))
	if err != nil {
		t.Fatal(err)
	}

	newTestConfig(globalMinioDefaultRegion, obj)

	initAllSubsystems(GlobalContext)
	return obj
}

// BenchmarkListObjects - Run ListObject Repeatedly and benchmark.
func BenchmarkListObjects(b *testing.B) {
	// Make a temporary directory to use as the obj.
	directory := b.TempDir()

	// Create the obj.
	obj := initFSObjectsB(directory, b)

	bucket := "ls-benchmark-bucket"
	// Create a bucket.
	err := obj.MakeBucket(b.Context(), bucket, MakeBucketOptions{})
	if err != nil {
		b.Fatal(err)
	}

	// Insert objects to be listed and benchmarked later.
	for i := range 20000 {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject(b.Context(), bucket, key, mustGetPutObjReader(b, bytes.NewBufferString(key), int64(len(key)), "", ""), ObjectOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}

	// List the buckets over and over and over.
	for b.Loop() {
		_, err = obj.ListObjects(b.Context(), bucket, "", "obj9000", "", -1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestListObjectsWithILM(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsWithILM)
}

func testListObjectsWithILM(obj ObjectLayer, instanceType string, t1 TestErrHandler) {
	// Prepare lifecycle expiration workers
	es := newExpiryState(t1.Context(), obj, 0)
	globalExpiryState = es

	t, _ := t1.(*testing.T)

	objContent := "test-content"
	objMd5 := md5.Sum([]byte(objContent))

	uploads := []struct {
		bucket     string
		expired    int
		notExpired int
	}{
		{"test-list-ilm-nothing-expired", 0, 6},
		{"test-list-ilm-all-expired", 6, 0},
		{"test-list-ilm-all-half-expired", 3, 3},
	}

	oneWeekAgo := time.Now().Add(-7 * 24 * time.Hour)

	lifecycleBytes := []byte(`
<LifecycleConfiguration>
	<Rule>
		<Status>Enabled</Status>
		<Expiration>
			<Days>1</Days>
		</Expiration>
	</Rule>
</LifecycleConfiguration>
`)

	lifecycleConfig, err := lifecycle.ParseLifecycleConfig(bytes.NewReader(lifecycleBytes))
	if err != nil {
		t.Fatal(err)
	}

	for i, upload := range uploads {
		err := obj.MakeBucket(context.Background(), upload.bucket, MakeBucketOptions{})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}

		metadata, err := globalBucketMetadataSys.Get(upload.bucket)
		if err != nil {
			t.Fatal(err)
		}
		metadata.lifecycleConfig = lifecycleConfig
		globalBucketMetadataSys.Set(upload.bucket, metadata)
		defer globalBucketMetadataSys.Remove(upload.bucket)

		// Upload objects which modtime as one week ago, supposed to be expired by ILM
		for range upload.expired {
			_, err := obj.PutObject(context.Background(), upload.bucket, randString(32),
				mustGetPutObjReader(t,
					bytes.NewBufferString(objContent),
					int64(len(objContent)),
					hex.EncodeToString(objMd5[:]),
					""),
				ObjectOptions{MTime: oneWeekAgo},
			)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Upload objects which current time as modtime, not expired by ILM
		for range upload.notExpired {
			_, err := obj.PutObject(context.Background(), upload.bucket, randString(32),
				mustGetPutObjReader(t,
					bytes.NewBufferString(objContent),
					int64(len(objContent)),
					hex.EncodeToString(objMd5[:]),
					""),
				ObjectOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, maxKeys := range []int{1, 10, 49} {
			// Test ListObjects V2
			totalObjs, didRuns := 0, 0
			marker := ""
			for {
				didRuns++
				if didRuns > 1000 {
					t.Fatal("too many runs")
					return
				}
				result, err := obj.ListObjectsV2(context.Background(), upload.bucket, "", marker, "", maxKeys, false, "")
				if err != nil {
					t.Fatalf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i, instanceType, err.Error())
				}
				totalObjs += len(result.Objects)
				if !result.IsTruncated {
					break
				}
				if marker != "" && marker == result.NextContinuationToken {
					t.Fatalf("infinite loop marker: %s", result.NextContinuationToken)
				}
				marker = result.NextContinuationToken
			}

			if totalObjs != upload.notExpired {
				t.Fatalf("Test %d: %s: max-keys=%d, %d objects are expected to be seen, but %d found instead (didRuns=%d)",
					i+1, instanceType, maxKeys, upload.notExpired, totalObjs, didRuns)
			}
		}
	}
}
