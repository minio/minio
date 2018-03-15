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

package gcs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	"google.golang.org/api/googleapi"

	miniogo "github.com/minio/minio-go"
	minio "github.com/minio/minio/cmd"
)

func TestToGCSPageToken(t *testing.T) {
	testCases := []struct {
		Name  string
		Token string
	}{
		{
			Name:  "A",
			Token: "CgFB",
		},
		{
			Name:  "AAAAAAAAAA",
			Token: "CgpBQUFBQUFBQUFB",
		},
		{
			Name:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Token: "CmRBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB",
		},
		{
			Name:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Token: "CpEDQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE=",
		},
		{
			Name:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Token: "CpIDQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB",
		},
		{
			Name:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Token: "CpMDQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==",
		},
		{
			Name:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Token: "CvQDQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE=",
		},
	}

	for i, testCase := range testCases {
		if toGCSPageToken(testCase.Name) != testCase.Token {
			t.Errorf("Test %d: Expected %s, got %s", i+1, toGCSPageToken(testCase.Name), testCase.Token)
		}
	}

}

// TestIsValidGCSProjectIDFormat tests isValidGCSProjectIDFormat
func TestValidGCSProjectIDFormat(t *testing.T) {
	testCases := []struct {
		ProjectID string
		Valid     bool
	}{
		{"", false},
		{"a", false},
		{"Abc", false},
		{"1bcd", false},
		// 5 chars
		{"abcdb", false},
		// 6 chars
		{"abcdbz", true},
		// 30 chars
		{"project-id-1-project-id-more-1", true},
		// 31 chars
		{"project-id-1-project-id-more-11", false},
		{"storage.googleapis.com", false},
		{"http://storage.googleapis.com", false},
		{"http://localhost:9000", false},
		{"project-id-1", true},
		{"project-id-1988832", true},
		{"projectid1414", true},
	}

	for i, testCase := range testCases {
		valid := isValidGCSProjectIDFormat(testCase.ProjectID)
		if valid != testCase.Valid {
			t.Errorf("Test %d: Expected %v, got %v", i+1, valid, testCase.Valid)
		}
	}
}

// Test for isGCSMarker.
func TestIsGCSMarker(t *testing.T) {
	testCases := []struct {
		marker   string
		expected bool
	}{
		{
			marker:   "{minio}gcs123",
			expected: true,
		},
		{
			marker:   "{mini_no}tgcs123",
			expected: false,
		},
		{
			marker:   "{minioagainnotgcs123",
			expected: false,
		},
		{
			marker:   "obj1",
			expected: false,
		},
	}

	for i, tc := range testCases {
		if actual := isGCSMarker(tc.marker); actual != tc.expected {
			t.Errorf("Test %d: marker is %s, expected %v but got %v",
				i+1, tc.marker, tc.expected, actual)
		}
	}
}

// Test for gcsMultipartMetaName.
func TestGCSMultipartMetaName(t *testing.T) {
	uploadID := "a"
	expected := path.Join(gcsMinioMultipartPathV1, uploadID, gcsMinioMultipartMeta)
	got := gcsMultipartMetaName(uploadID)
	if expected != got {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

// Test for gcsMultipartDataName.
func TestGCSMultipartDataName(t *testing.T) {
	var (
		uploadID   = "a"
		etag       = "b"
		partNumber = 1
	)
	expected := path.Join(gcsMinioMultipartPathV1, uploadID, fmt.Sprintf("%05d.%s", partNumber, etag))
	got := gcsMultipartDataName(uploadID, partNumber, etag)
	if expected != got {
		t.Errorf("expected: %s, got: %s", expected, got)
	}
}

func TestFromMinioClientListBucketResultToV2Info(t *testing.T) {

	listBucketResult := miniogo.ListBucketResult{
		IsTruncated:    false,
		Marker:         "testMarker",
		NextMarker:     "testMarker2",
		CommonPrefixes: []miniogo.CommonPrefix{{Prefix: "one"}, {Prefix: "two"}},
		Contents:       []miniogo.ObjectInfo{{Key: "testobj", ContentType: ""}},
	}

	listBucketV2Info := minio.ListObjectsV2Info{
		Prefixes:              []string{"one", "two"},
		Objects:               []minio.ObjectInfo{{Name: "testobj", Bucket: "testbucket", UserDefined: map[string]string{"Content-Type": ""}}},
		IsTruncated:           false,
		ContinuationToken:     "testMarker",
		NextContinuationToken: "testMarker2",
	}

	if got := minio.FromMinioClientListBucketResultToV2Info("testbucket", listBucketResult); !reflect.DeepEqual(got, listBucketV2Info) {
		t.Errorf("fromMinioClientListBucketResultToV2Info() = %v, want %v", got, listBucketV2Info)
	}
}

// Test for gcsParseProjectID
func TestGCSParseProjectID(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(f.Name())
	defer f.Close()

	contents := `
{
  "type": "service_account",
  "project_id": "miniotesting"
}
`
	f.WriteString(contents)
	projectID, err := gcsParseProjectID(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if projectID != "miniotesting" {
		t.Errorf(`Expected projectID value to be "miniotesting"`)
	}

	if _, err = gcsParseProjectID("non-existent"); err == nil {
		t.Errorf(`Expected to fail but succeeded reading "non-existent"`)
	}

	f.WriteString(`,}`)

	if _, err := gcsParseProjectID(f.Name()); err == nil {
		t.Errorf(`Expected to fail reading corrupted credentials file`)
	}
}

func TestGCSToObjectError(t *testing.T) {
	testCases := []struct {
		params      []string
		gcsErr      error
		expectedErr error
	}{
		{
			[]string{}, nil, nil,
		},
		{
			[]string{}, fmt.Errorf("Not *Error"), fmt.Errorf("Not *Error"),
		},
		{
			[]string{"bucket"},
			fmt.Errorf("storage: bucket doesn't exist"),
			minio.BucketNotFound{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"},
			fmt.Errorf("storage: object doesn't exist"),
			minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket", "object", "uploadID"},
			fmt.Errorf("storage: object doesn't exist"),
			minio.InvalidUploadID{
				UploadID: "uploadID",
			},
		},
		{
			[]string{},
			fmt.Errorf("Unknown error"),
			fmt.Errorf("Unknown error"),
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Message: "No list of errors",
			},
			&googleapi.Error{
				Message: "No list of errors",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason:  "conflict",
					Message: "You already own this bucket. Please select another name.",
				}},
			},
			minio.BucketAlreadyOwnedByYou{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason:  "conflict",
					Message: "Sorry, that name is not available. Please try a different one.",
				}},
			},
			minio.BucketAlreadyExists{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "conflict",
				}},
			},
			minio.BucketNotEmpty{Bucket: "bucket"},
		},
		{
			[]string{"bucket"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "notFound",
				}},
			},
			minio.BucketNotFound{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "notFound",
				}},
			},
			minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "invalid",
				}},
			},
			minio.BucketNameInvalid{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "forbidden",
				}},
			},
			minio.PrefixAccessDenied{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "keyInvalid",
				}},
			},
			minio.PrefixAccessDenied{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "required",
				}},
			},
			minio.PrefixAccessDenied{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket", "object"},
			&googleapi.Error{
				Errors: []googleapi.ErrorItem{{
					Reason: "unknown",
				}},
			},
			fmt.Errorf("Unsupported error reason: unknown"),
		},
	}

	for i, testCase := range testCases {
		actualErr := gcsToObjectError(testCase.gcsErr, testCase.params...)
		if actualErr != nil {
			if actualErr.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, actualErr)
			}
		}
	}
}
