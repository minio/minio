package cmd

import (
	"strings"

	"github.com/skyrings/skyring-common/tools/uuid"
)

func (xl xlObjects) validateBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(eBucketNameInvalid(bucket))
	}
	// Verify if bucket exists.
	if !xl.isBucketExist(bucket) {
		return traceError(eBucketNotFound(bucket))
	}
	return nil
}

func (xl xlObjects) validateInputArgsWithPrefix(bucket, prefix string) error {
	if err := xl.validateBucket(bucket); err != nil {
		return err
	}
	if !IsValidObjectPrefix(prefix) {
		return traceError(eObjectNameInvalid(bucket, prefix))
	}
	return nil
}

func (xl xlObjects) validateInputArgs(bucket, object string) error {
	if err := xl.validateBucket(bucket); err != nil {
		return err
	}
	if !IsValidObjectName(object) {
		return traceError(eObjectNameInvalid(bucket, object))
	}
	return nil
}

func (xl xlObjects) validateListMpartUploadsArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter string) error {
	if err := xl.validateListObjsArgs(bucket, prefix, keyMarker, delimiter); err != nil {
		return err
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return traceError(eInvalidUploadIDKeyCombination(
				uploadIDMarker,
				keyMarker,
			))
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return traceError(err)
		}
		if id.IsZero() {
			return traceError(eMalformedUploadID(bucket, prefix, uploadIDMarker))
		}
	}
	return nil
}

func (xl xlObjects) validateListObjsArgs(bucket, prefix, marker, delimiter string) error {
	if err := xl.validateInputArgsWithPrefix(bucket, prefix); err != nil {
		return err
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return traceError(eUnsupportedDelimiter(delimiter))
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return traceError(eInvalidMarkerPrefixCombination(marker, prefix))
		}
	}
	return nil
}
