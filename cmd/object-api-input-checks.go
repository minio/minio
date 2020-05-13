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
	"runtime"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	"github.com/minio/minio/cmd/logger"
)

// Checks on GetObject arguments, bucket and object.
func checkGetObjArgs(ctx context.Context, bucket, object string) error {
	return checkBucketAndObjectNames(ctx, bucket, object)
}

// Checks on DeleteObject arguments, bucket and object.
func checkDelObjArgs(ctx context.Context, bucket, object string) error {
	return checkBucketAndObjectNames(ctx, bucket, object)
}

// Checks bucket and object name validity, returns nil if both are valid.
func checkBucketAndObjectNames(ctx context.Context, bucket, object string) error {
	// Verify if bucket is valid.
	if !isMinioMetaBucketName(bucket) && s3utils.CheckValidBucketName(bucket) != nil {
		logger.LogIf(ctx, BucketNameInvalid{Bucket: bucket})
		return BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if len(object) == 0 {
		logger.LogIf(ctx, ObjectNameInvalid{Bucket: bucket, Object: object})
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !IsValidObjectPrefix(object) {
		logger.LogIf(ctx, ObjectNameInvalid{Bucket: bucket, Object: object})
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if runtime.GOOS == globalWindowsOSName && strings.Contains(object, "\\") {
		// Objects cannot be contain \ in Windows and is listed as `Characters to Avoid`.
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	return nil
}

// Checks for all ListObjects arguments validity.
func checkListObjsArgs(ctx context.Context, bucket, prefix, marker string, obj ObjectLayer) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}
	// Validates object prefix validity after bucket exists.
	if !IsValidObjectPrefix(prefix) {
		logger.LogIf(ctx, ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		})
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		}
	}
	// Verify if marker has prefix.
	if marker != "" && !HasPrefix(marker, prefix) {
		logger.LogIf(ctx, InvalidMarkerPrefixCombination{
			Marker: marker,
			Prefix: prefix,
		})
		return InvalidMarkerPrefixCombination{
			Marker: marker,
			Prefix: prefix,
		}
	}
	return nil
}

// Checks for all ListMultipartUploads arguments validity.
func checkListMultipartArgs(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, obj ObjectLayer) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, keyMarker, obj); err != nil {
		return err
	}
	if uploadIDMarker != "" {
		if HasSuffix(keyMarker, SlashSeparator) {

			logger.LogIf(ctx, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
			return InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		if _, err := uuid.Parse(uploadIDMarker); err != nil {
			logger.LogIf(ctx, err)
			return MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}
	return nil
}

// Checks for NewMultipartUpload arguments validity, also validates if bucket exists.
func checkNewMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for PutObjectPart arguments validity, also validates if bucket exists.
func checkPutObjectPartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for ListParts arguments validity, also validates if bucket exists.
func checkListPartsArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for CompleteMultipartUpload arguments validity, also validates if bucket exists.
func checkCompleteMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for AbortMultipartUpload arguments validity, also validates if bucket exists.
func checkAbortMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks Object arguments validity, also validates if bucket exists.
func checkObjectArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}

	if err := checkObjectNameForLengthAndSlash(bucket, object); err != nil {
		return err
	}

	// Validates object name validity after bucket exists.
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}

	return nil
}

// Checks for PutObject arguments validity, also validates if bucket exists.
func checkPutObjectArgs(ctx context.Context, bucket, object string, obj ObjectLayer, size int64) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}

	if err := checkObjectNameForLengthAndSlash(bucket, object); err != nil {
		return err
	}
	if len(object) == 0 ||
		(HasSuffix(object, SlashSeparator) && size != 0) ||
		!IsValidObjectPrefix(object) {
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	return nil
}

// Checks whether bucket exists and returns appropriate error if not.
func checkBucketExist(ctx context.Context, bucket string, obj ObjectLayer) error {
	_, err := obj.GetBucketInfo(ctx, bucket)
	if err != nil {
		return err
	}
	return nil
}
