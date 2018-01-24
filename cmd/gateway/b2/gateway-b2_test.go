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

package b2

import (
	"fmt"
	"testing"

	b2 "github.com/minio/blazer/base"
	"github.com/minio/minio/pkg/errors"

	minio "github.com/minio/minio/cmd"
)

// Test b2 object error.
func TestB2ObjectError(t *testing.T) {
	testCases := []struct {
		params      []string
		b2Err       error
		expectedErr error
	}{
		{
			[]string{}, nil, nil,
		},
		{
			[]string{}, fmt.Errorf("Not *Error"), fmt.Errorf("Not *Error"),
		},
		{
			[]string{}, errors.Trace(fmt.Errorf("Non B2 Error")), fmt.Errorf("Non B2 Error"),
		},
		{
			[]string{"bucket"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "duplicate_bucket_name",
			}), minio.BucketAlreadyOwnedByYou{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "bad_request",
			}), minio.BucketNotFound{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "bad_request",
			}), minio.ObjectNameInvalid{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "bad_bucket_id",
			}), minio.BucketNotFound{Bucket: "bucket"},
		},
		{
			[]string{"bucket", "object"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "file_not_present",
			}), minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket", "object"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "not_found",
			}), minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			},
		},
		{
			[]string{"bucket"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Code:       "cannot_delete_non_empty_bucket",
			}), minio.BucketNotEmpty{
				Bucket: "bucket",
			},
		},
		{
			[]string{"bucket", "object", "uploadID"}, errors.Trace(b2.Error{
				StatusCode: 1,
				Message:    "No active upload for",
			}), minio.InvalidUploadID{
				UploadID: "uploadID",
			},
		},
	}

	for i, testCase := range testCases {
		actualErr := b2ToObjectError(testCase.b2Err, testCase.params...)
		if actualErr != nil {
			if actualErr.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, actualErr)
			}
		}
	}
}
