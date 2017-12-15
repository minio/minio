/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"github.com/minio/minio/pkg/errors"
	"github.com/skyrings/skyring-common/tools/uuid"
)

// Checks on GetObject arguments, bucket and object.
func checkGetObjArgs(bucket, object string) error {
	return checkBucketAndObjectNames(bucket, object)
}

// Checks on DeleteObject arguments, bucket and object.
func checkDelObjArgs(bucket, object string) error {
	return checkBucketAndObjectNames(bucket, object)
}

// Checks bucket and object name validity, returns nil if both are valid.
func checkBucketAndObjectNames(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return errors.Trace(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		// Objects with "/" are invalid, verify to return a different error.
		if hasSuffix(object, slashSeparator) || hasPrefix(object, slashSeparator) {
			return errors.Trace(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return errors.Trace(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	return nil
}

// Checks for all ListObjects arguments validity.
func checkListObjsArgs(bucket, prefix, marker, delimiter string, obj ObjectLayer) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(bucket, obj); err != nil {
		return errors.Trace(err)
	}
	// Validates object prefix validity after bucket exists.
	if !IsValidObjectPrefix(prefix) {
		return errors.Trace(ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return errors.Trace(UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if marker != "" && !hasPrefix(marker, prefix) {
		return errors.Trace(InvalidMarkerPrefixCombination{
			Marker: marker,
			Prefix: prefix,
		})
	}
	return nil
}

// Checks for all ListMultipartUploads arguments validity.
func checkListMultipartArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, obj ObjectLayer) error {
	if err := checkListObjsArgs(bucket, prefix, keyMarker, delimiter, obj); err != nil {
		return err
	}
	if uploadIDMarker != "" {
		if hasSuffix(keyMarker, slashSeparator) {
			return errors.Trace(InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return errors.Trace(err)
		}
		if id.IsZero() {
			return errors.Trace(MalformedUploadID{
				UploadID: uploadIDMarker,
			})
		}
	}
	return nil
}

// Checks for NewMultipartUpload arguments validity, also validates if bucket exists.
func checkNewMultipartArgs(bucket, object string, obj ObjectLayer) error {
	return checkPutObjectArgs(bucket, object, obj)
}

// Checks for PutObjectPart arguments validity, also validates if bucket exists.
func checkPutObjectPartArgs(bucket, object string, obj ObjectLayer) error {
	return checkPutObjectArgs(bucket, object, obj)
}

// Checks for ListParts arguments validity, also validates if bucket exists.
func checkListPartsArgs(bucket, object string, obj ObjectLayer) error {
	return checkPutObjectArgs(bucket, object, obj)
}

// Checks for CompleteMultipartUpload arguments validity, also validates if bucket exists.
func checkCompleteMultipartArgs(bucket, object string, obj ObjectLayer) error {
	return checkPutObjectArgs(bucket, object, obj)
}

// Checks for AbortMultipartUpload arguments validity, also validates if bucket exists.
func checkAbortMultipartArgs(bucket, object string, obj ObjectLayer) error {
	return checkPutObjectArgs(bucket, object, obj)
}

// Checks for PutObject arguments validity, also validates if bucket exists.
func checkPutObjectArgs(bucket, object string, obj ObjectLayer) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(bucket, obj); err != nil {
		return errors.Trace(err)
	}
	// Validates object name validity after bucket exists.
	if !IsValidObjectName(object) {
		return errors.Trace(ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	return nil
}

// Checks whether bucket exists and returns appropriate error if not.
func checkBucketExist(bucket string, obj ObjectLayer) error {
	_, err := obj.GetBucketInfo(bucket)
	if err != nil {
		return errors.Cause(err)
	}
	return nil
}
