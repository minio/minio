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
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/textproto"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio/internal/config"
)

// Tests validate bucket LocationConstraint.
func TestIsValidLocationContraint(t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	// Corrupted XML
	malformedReq := &http.Request{
		Body:          ioutil.NopCloser(bytes.NewReader([]byte("<>"))),
		ContentLength: int64(len("<>")),
	}

	// Not an XML
	badRequest := &http.Request{
		Body:          ioutil.NopCloser(bytes.NewReader([]byte("garbage"))),
		ContentLength: int64(len("garbage")),
	}

	// generates the input request with XML bucket configuration set to the request body.
	createExpectedRequest := func(req *http.Request, location string) *http.Request {
		createBucketConfig := createBucketLocationConfiguration{}
		createBucketConfig.Location = location
		createBucketConfigBytes, _ := xml.Marshal(createBucketConfig)
		createBucketConfigBuffer := bytes.NewReader(createBucketConfigBytes)
		req.Body = ioutil.NopCloser(createBucketConfigBuffer)
		req.ContentLength = int64(createBucketConfigBuffer.Len())
		return req
	}

	testCases := []struct {
		request            *http.Request
		serverConfigRegion string
		expectedCode       APIErrorCode
	}{
		// Test case - 1.
		{createExpectedRequest(&http.Request{}, "eu-central-1"), globalMinioDefaultRegion, ErrNone},
		// Test case - 2.
		// In case of empty request body ErrNone is returned.
		{createExpectedRequest(&http.Request{}, ""), globalMinioDefaultRegion, ErrNone},
		// Test case - 3
		// In case of garbage request body ErrMalformedXML is returned.
		{badRequest, globalMinioDefaultRegion, ErrMalformedXML},
		// Test case - 4
		// In case of invalid XML request body ErrMalformedXML is returned.
		{malformedReq, globalMinioDefaultRegion, ErrMalformedXML},
	}

	for i, testCase := range testCases {
		config.SetRegion(globalServerConfig, testCase.serverConfigRegion)
		_, actualCode := parseLocationConstraint(testCase.request)
		if testCase.expectedCode != actualCode {
			t.Errorf("Test %d: Expected the APIErrCode to be %d, but instead found %d", i+1, testCase.expectedCode, actualCode)
		}
	}
}

// Test validate form field size.
func TestValidateFormFieldSize(t *testing.T) {
	testCases := []struct {
		header http.Header
		err    error
	}{
		// Empty header returns error as nil,
		{
			header: nil,
			err:    nil,
		},
		// Valid header returns error as nil.
		{
			header: http.Header{
				"Content-Type": []string{"image/png"},
			},
			err: nil,
		},
		// Invalid header value > maxFormFieldSize+1
		{
			header: http.Header{
				"Garbage": []string{strings.Repeat("a", int(maxFormFieldSize)+1)},
			},
			err: errSizeUnexpected,
		},
	}

	// Run validate form field size check under all test cases.
	for i, testCase := range testCases {
		err := validateFormFieldSize(context.Background(), testCase.header)
		if err != nil {
			if err.Error() != testCase.err.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.err, err)
			}
		}
	}
}

// Tests validate metadata extraction from http headers.
func TestExtractMetadataHeaders(t *testing.T) {
	testCases := []struct {
		header     http.Header
		metadata   map[string]string
		shouldFail bool
	}{
		// Validate if there a known 'content-type'.
		{
			header: http.Header{
				"Content-Type": []string{"image/png"},
			},
			metadata: map[string]string{
				"content-type": "image/png",
			},
			shouldFail: false,
		},
		// Validate if there are no keys to extract.
		{
			header: http.Header{
				"Test-1": []string{"123"},
			},
			metadata:   map[string]string{},
			shouldFail: false,
		},
		// Validate that there are all headers extracted
		{
			header: http.Header{
				"X-Amz-Meta-Appid":   []string{"amz-meta"},
				"X-Minio-Meta-Appid": []string{"minio-meta"},
			},
			metadata: map[string]string{
				"X-Amz-Meta-Appid":   "amz-meta",
				"X-Minio-Meta-Appid": "minio-meta",
			},
			shouldFail: false,
		},
		// Fail if header key is not in canonicalized form
		{
			header: http.Header{
				"x-amz-meta-appid": []string{"amz-meta"},
			},
			metadata: map[string]string{
				"x-amz-meta-appid": "amz-meta",
			},
			shouldFail: false,
		},
		// Support multiple values
		{
			header: http.Header{
				"x-amz-meta-key": []string{"amz-meta1", "amz-meta2"},
			},
			metadata: map[string]string{
				"x-amz-meta-key": "amz-meta1,amz-meta2",
			},
			shouldFail: false,
		},
		// Empty header input returns empty metadata.
		{
			header:     nil,
			metadata:   nil,
			shouldFail: true,
		},
	}

	// Validate if the extracting headers.
	for i, testCase := range testCases {
		metadata := make(map[string]string)
		err := extractMetadataFromMime(context.Background(), textproto.MIMEHeader(testCase.header), metadata)
		if err != nil && !testCase.shouldFail {
			t.Fatalf("Test %d failed to extract metadata: %v", i+1, err)
		}
		if err == nil && testCase.shouldFail {
			t.Fatalf("Test %d should fail, but it passed", i+1)
		}
		if err == nil && !reflect.DeepEqual(metadata, testCase.metadata) {
			t.Fatalf("Test %d failed: Expected \"%#v\", got \"%#v\"", i+1, testCase.metadata, metadata)
		}
	}
}

// Test getResource()
func TestGetResource(t *testing.T) {
	testCases := []struct {
		p                string
		host             string
		domains          []string
		expectedResource string
	}{
		{"/a/b/c", "test.mydomain.com", []string{"mydomain.com"}, "/test/a/b/c"},
		{"/a/b/c", "test.mydomain.com", []string{"notmydomain.com"}, "/a/b/c"},
		{"/a/b/c", "test.mydomain.com", nil, "/a/b/c"},
	}
	for i, test := range testCases {
		gotResource, err := getResource(test.p, test.host, test.domains)
		if err != nil {
			t.Fatal(err)
		}
		if gotResource != test.expectedResource {
			t.Fatalf("test %d: expected %s got %s", i+1, test.expectedResource, gotResource)
		}
	}
}
