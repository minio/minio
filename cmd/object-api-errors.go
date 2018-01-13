/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"fmt"
	"io"

	"github.com/minio/minio/pkg/errors"
)

// Converts underlying storage error. Convenience function written to
// handle all cases where we have known types of errors returned by
// underlying storage layer.
func toObjectErr(err error, params ...string) error {
	e, ok := err.(*errors.Error)
	if ok {
		err = e.Cause
	}

	switch err {
	case errVolumeNotFound:
		if len(params) >= 1 {
			err = BucketNotFound{Bucket: params[0]}
		}
	case errVolumeNotEmpty:
		if len(params) >= 1 {
			err = BucketNotEmpty{Bucket: params[0]}
		}
	case errVolumeExists:
		if len(params) >= 1 {
			err = BucketExists{Bucket: params[0]}
		}
	case errDiskFull:
		err = StorageFull{}
	case errFileAccessDenied:
		if len(params) >= 2 {
			err = PrefixAccessDenied{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errIsNotRegular, errFileAccessDenied:
		if len(params) >= 2 {
			err = ObjectExistsAsDirectory{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileNotFound:
		if len(params) >= 2 {
			err = ObjectNotFound{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileNameTooLong:
		if len(params) >= 2 {
			err = ObjectNameInvalid{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errDataTooLarge:
		if len(params) >= 2 {
			err = ObjectTooLarge{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errDataTooSmall:
		if len(params) >= 2 {
			err = ObjectTooSmall{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errXLReadQuorum:
		err = InsufficientReadQuorum{}
	case errXLWriteQuorum:
		err = InsufficientWriteQuorum{}
	case io.ErrUnexpectedEOF, io.ErrShortWrite:
		err = IncompleteBody{}
	}
	if ok {
		e.Cause = err
		return e
	}
	return err
}

// SignatureDoesNotMatch - when content md5 does not match with what was sent from client.
type SignatureDoesNotMatch struct{}

func (e SignatureDoesNotMatch) Error() string {
	return "The request signature we calculated does not match the signature you provided. Check your key and signing method."
}

// StorageFull storage ran out of space.
type StorageFull struct{}

func (e StorageFull) Error() string {
	return "Storage reached its minimum free disk threshold."
}

// InsufficientReadQuorum storage cannot satisfy quorum for read operation.
type InsufficientReadQuorum struct{}

func (e InsufficientReadQuorum) Error() string {
	return "Storage resources are insufficient for the read operation."
}

// InsufficientWriteQuorum storage cannot satisfy quorum for write operation.
type InsufficientWriteQuorum struct{}

func (e InsufficientWriteQuorum) Error() string {
	return "Storage resources are insufficient for the write operation."
}

// GenericError - generic object layer error.
type GenericError struct {
	Bucket string
	Object string
}

// BucketNotFound bucket does not exist.
type BucketNotFound GenericError

func (e BucketNotFound) Error() string {
	return "Bucket not found: " + e.Bucket
}

// BucketAlreadyExists the requested bucket name is not available.
type BucketAlreadyExists GenericError

func (e BucketAlreadyExists) Error() string {
	return "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."
}

// BucketAlreadyOwnedByYou already owned by you.
type BucketAlreadyOwnedByYou GenericError

func (e BucketAlreadyOwnedByYou) Error() string {
	return "Bucket already owned by you: " + e.Bucket
}

// BucketNotEmpty bucket is not empty.
type BucketNotEmpty GenericError

func (e BucketNotEmpty) Error() string {
	return "Bucket not empty: " + e.Bucket
}

// ObjectNotFound object does not exist.
type ObjectNotFound GenericError

func (e ObjectNotFound) Error() string {
	return "Object not found: " + e.Bucket + "#" + e.Object
}

// ObjectExistsAsDirectory object already exists as a directory.
type ObjectExistsAsDirectory GenericError

func (e ObjectExistsAsDirectory) Error() string {
	return "Object exists on : " + e.Bucket + " as directory " + e.Object
}

//PrefixAccessDenied object access is denied.
type PrefixAccessDenied GenericError

func (e PrefixAccessDenied) Error() string {
	return "Prefix access is denied: " + e.Bucket + "/" + e.Object
}

// BucketExists bucket exists.
type BucketExists GenericError

func (e BucketExists) Error() string {
	return "Bucket exists: " + e.Bucket
}

// UnsupportedDelimiter - unsupported delimiter.
type UnsupportedDelimiter struct {
	Delimiter string
}

func (e UnsupportedDelimiter) Error() string {
	return fmt.Sprintf("delimiter '%s' is not supported. Only '/' is supported", e.Delimiter)
}

// InvalidUploadIDKeyCombination - invalid upload id and key marker combination.
type InvalidUploadIDKeyCombination struct {
	UploadIDMarker, KeyMarker string
}

func (e InvalidUploadIDKeyCombination) Error() string {
	return fmt.Sprintf("Invalid combination of uploadID marker '%s' and marker '%s'", e.UploadIDMarker, e.KeyMarker)
}

// InvalidMarkerPrefixCombination - invalid marker and prefix combination.
type InvalidMarkerPrefixCombination struct {
	Marker, Prefix string
}

func (e InvalidMarkerPrefixCombination) Error() string {
	return fmt.Sprintf("Invalid combination of marker '%s' and prefix '%s'", e.Marker, e.Prefix)
}

// BucketPolicyNotFound - no bucket policy found.
type BucketPolicyNotFound GenericError

func (e BucketPolicyNotFound) Error() string {
	return "No bucket policy found for bucket: " + e.Bucket
}

/// Bucket related errors.

// BucketNameInvalid - bucketname provided is invalid.
type BucketNameInvalid GenericError

// Return string an error formatted as the given text.
func (e BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + e.Bucket
}

/// Object related errors.

// ObjectNameInvalid - object name provided is invalid.
type ObjectNameInvalid GenericError

// Return string an error formatted as the given text.
func (e ObjectNameInvalid) Error() string {
	return "Object name invalid: " + e.Bucket + "#" + e.Object
}

// AllAccessDisabled All access to this object has been disabled
type AllAccessDisabled GenericError

// Return string an error formatted as the given text.
func (e AllAccessDisabled) Error() string {
	return "All access to this object has been disabled"
}

// IncompleteBody You did not provide the number of bytes specified by the Content-Length HTTP header.
type IncompleteBody GenericError

// Return string an error formatted as the given text.
func (e IncompleteBody) Error() string {
	return e.Bucket + "#" + e.Object + "has incomplete body"
}

// InvalidRange - invalid range typed error.
type InvalidRange struct {
	OffsetBegin  int64
	OffsetEnd    int64
	ResourceSize int64
}

func (e InvalidRange) Error() string {
	return fmt.Sprintf("The requested range \"bytes %d-%d/%d\" is not satisfiable.", e.OffsetBegin, e.OffsetEnd, e.ResourceSize)
}

// ObjectTooLarge error returned when the size of the object > max object size allowed (5G) per request.
type ObjectTooLarge GenericError

func (e ObjectTooLarge) Error() string {
	return "size of the object greater than what is allowed(5G)"
}

// ObjectTooSmall error returned when the size of the object < what is expected.
type ObjectTooSmall GenericError

func (e ObjectTooSmall) Error() string {
	return "size of the object less than what is expected"
}

// OperationTimedOut - a timeout occurred.
type OperationTimedOut struct {
	Path string
}

func (e OperationTimedOut) Error() string {
	return "Operation timed out: " + e.Path
}

/// Multipart related errors.

// MalformedUploadID malformed upload id.
type MalformedUploadID struct {
	UploadID string
}

func (e MalformedUploadID) Error() string {
	return "Malformed upload id " + e.UploadID
}

// InvalidUploadID invalid upload id.
type InvalidUploadID struct {
	UploadID string
}

func (e InvalidUploadID) Error() string {
	return "Invalid upload id " + e.UploadID
}

// InvalidPart One or more of the specified parts could not be found
type InvalidPart struct{}

func (e InvalidPart) Error() string {
	return "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag."
}

// PartsSizeUnequal - All parts except the last part should be of the same size
type PartsSizeUnequal struct{}

func (e PartsSizeUnequal) Error() string {
	return "All parts except the last part should be of the same size"
}

// PartTooSmall - error if part size is less than 5MB.
type PartTooSmall struct {
	PartSize   int64
	PartNumber int
	PartETag   string
}

func (e PartTooSmall) Error() string {
	return fmt.Sprintf("Part size for %d should be at least 5MB", e.PartNumber)
}

// PartTooBig returned if size of part is bigger than the allowed limit.
type PartTooBig struct{}

func (e PartTooBig) Error() string {
	return "Part size bigger than the allowed limit"
}

// InvalidETag error returned when the etag has changed on disk
type InvalidETag struct{}

func (e InvalidETag) Error() string {
	return "etag of the object has changed"
}

// NotImplemented If a feature is not implemented
type NotImplemented struct{}

func (e NotImplemented) Error() string {
	return "Not Implemented"
}

// PolicyNesting - policy nesting conflict.
type PolicyNesting struct{}

func (e PolicyNesting) Error() string {
	return "New bucket policy conflicts with an existing policy. Please try again with new prefix."
}

// PolicyNotFound - policy not found
type PolicyNotFound GenericError

func (e PolicyNotFound) Error() string {
	return "Policy not found"
}

// UnsupportedMetadata - unsupported metadata
type UnsupportedMetadata struct{}

func (e UnsupportedMetadata) Error() string {
	return "Unsupported headers in Metadata"
}

// isErrIncompleteBody - Check if error type is IncompleteBody.
func isErrIncompleteBody(err error) bool {
	err = errors.Cause(err)
	switch err.(type) {
	case IncompleteBody:
		return true
	}
	return false
}

// isErrBucketPolicyNotFound - Check if error type is BucketPolicyNotFound.
func isErrBucketPolicyNotFound(err error) bool {
	err = errors.Cause(err)
	switch err.(type) {
	case BucketPolicyNotFound:
		return true
	}
	return false
}

// isErrObjectNotFound - Check if error type is ObjectNotFound.
func isErrObjectNotFound(err error) bool {
	err = errors.Cause(err)
	switch err.(type) {
	case ObjectNotFound:
		return true
	}
	return false
}
