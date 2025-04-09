// Copyright (c) 2015-2024 MinIO, Inc.
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
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	xhttp "github.com/minio/minio/internal/http"
)

// TestGetAndValidateAttributesOpts is currently minimal and covers a subset of getAndValidateAttributesOpts(),
// it is intended to be expanded when the function is worked on in the future.
func TestGetAndValidateAttributesOpts(t *testing.T) {
	globalBucketVersioningSys = &BucketVersioningSys{}
	bucket := minioMetaBucket
	ctx := t.Context()
	testCases := []struct {
		name            string
		headers         http.Header
		wantObjectAttrs map[string]struct{}
	}{
		{
			name:            "empty header",
			headers:         http.Header{},
			wantObjectAttrs: map[string]struct{}{},
		},
		{
			name: "single header line",
			headers: http.Header{
				xhttp.AmzObjectAttributes: []string{"test1,test2"},
			},
			wantObjectAttrs: map[string]struct{}{
				"test1": {}, "test2": {},
			},
		},
		{
			name: "multiple header lines with some duplicates",
			headers: http.Header{
				xhttp.AmzObjectAttributes: []string{"test1,test2", "test3,test4", "test4,test3"},
			},
			wantObjectAttrs: map[string]struct{}{
				"test1": {}, "test2": {}, "test3": {}, "test4": {},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header = testCase.headers

			opts, _ := getAndValidateAttributesOpts(ctx, rec, req, bucket, "testobject")

			if !reflect.DeepEqual(opts.ObjectAttributes, testCase.wantObjectAttrs) {
				t.Errorf("want opts %v, got %v", testCase.wantObjectAttrs, opts.ObjectAttributes)
			}
		})
	}
}
