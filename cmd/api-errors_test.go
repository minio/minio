/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"context"
	"errors"
	"testing"

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/pkg/hash"
)

var toAPIErrorTests = []struct {
	err     error
	errCode APIErrorCode
}{
	{err: hash.BadDigest{}, errCode: ErrBadDigest},
	{err: hash.SHA256Mismatch{}, errCode: ErrContentSHA256Mismatch},
	{err: IncompleteBody{}, errCode: ErrIncompleteBody},
	{err: ObjectExistsAsDirectory{}, errCode: ErrObjectExistsAsDirectory},
	{err: BucketNameInvalid{}, errCode: ErrInvalidBucketName},
	{err: BucketExists{}, errCode: ErrBucketAlreadyOwnedByYou},
	{err: ObjectNotFound{}, errCode: ErrNoSuchKey},
	{err: ObjectNameInvalid{}, errCode: ErrInvalidObjectName},
	{err: InvalidUploadID{}, errCode: ErrNoSuchUpload},
	{err: InvalidPart{}, errCode: ErrInvalidPart},
	{err: InsufficientReadQuorum{}, errCode: ErrReadQuorum},
	{err: InsufficientWriteQuorum{}, errCode: ErrWriteQuorum},
	{err: UnsupportedDelimiter{}, errCode: ErrNotImplemented},
	{err: InvalidMarkerPrefixCombination{}, errCode: ErrNotImplemented},
	{err: InvalidUploadIDKeyCombination{}, errCode: ErrNotImplemented},
	{err: MalformedUploadID{}, errCode: ErrNoSuchUpload},
	{err: PartTooSmall{}, errCode: ErrEntityTooSmall},
	{err: BucketNotEmpty{}, errCode: ErrBucketNotEmpty},
	{err: BucketNotFound{}, errCode: ErrNoSuchBucket},
	{err: StorageFull{}, errCode: ErrStorageFull},
	{err: NotImplemented{}, errCode: ErrNotImplemented},
	{err: errSignatureMismatch, errCode: ErrSignatureDoesNotMatch},

	// SSE-C errors
	{err: crypto.ErrInvalidCustomerAlgorithm, errCode: ErrInvalidSSECustomerAlgorithm},
	{err: crypto.ErrMissingCustomerKey, errCode: ErrMissingSSECustomerKey},
	{err: crypto.ErrInvalidCustomerKey, errCode: ErrAccessDenied},
	{err: crypto.ErrMissingCustomerKeyMD5, errCode: ErrMissingSSECustomerKeyMD5},
	{err: crypto.ErrCustomerKeyMD5Mismatch, errCode: ErrSSECustomerKeyMD5Mismatch},
	{err: errObjectTampered, errCode: ErrObjectTampered},

	{err: nil, errCode: ErrNone},
	{err: errors.New("Custom error"), errCode: ErrInternalError}, // Case where err type is unknown.
}

func TestAPIErrCode(t *testing.T) {
	ctx := context.Background()
	for i, testCase := range toAPIErrorTests {
		errCode := toAPIErrorCode(ctx, testCase.err)
		if errCode != testCase.errCode {
			t.Errorf("Test %d: Expected error code %d, got %d", i+1, testCase.errCode, errCode)
		}
	}
}
