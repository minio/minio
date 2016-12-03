package cmd

import (
	"strings"

	"github.com/skyrings/skyring-common/tools/uuid"
)

func (fs fsObjects) validateBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(eBucketNameInvalid(bucket))
	}
	// Verify if bucket exists.
	if !fs.isBucketExist(bucket) {
		return traceError(eBucketNotFound(bucket))
	}
	return nil
}

func (fs fsObjects) validateInputArgsWithPrefix(bucket, prefix string) error {
	if err := fs.validateBucket(bucket); err != nil {
		return err
	}
	if !IsValidObjectPrefix(prefix) {
		return traceError(eObjectNameInvalid(bucket, prefix))
	}
	return nil
}

func (fs fsObjects) validateInputArgs(bucket, object string) error {
	if err := fs.validateBucket(bucket); err != nil {
		return err
	}
	if !IsValidObjectName(object) {
		return traceError(eObjectNameInvalid(bucket, object))
	}
	return nil
}

func (fs fsObjects) validateListMpartUploadsArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter string) error {
	if err := fs.validateListObjsArgs(bucket, prefix, keyMarker, delimiter); err != nil {
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

func (fs fsObjects) validateListObjsArgs(bucket, prefix, marker, delimiter string) error {
	if err := fs.validateInputArgsWithPrefix(bucket, prefix); err != nil {
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
