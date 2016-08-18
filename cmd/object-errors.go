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
	"errors"
	"fmt"
	"io"
)

// Converts underlying storage error. Convenience function written to
// handle all cases where we have known types of errors returned by
// underlying storage layer.
func toObjectErr(err error, params ...string) error {
	switch err {
	case errVolumeNotFound:
		if len(params) >= 1 {
			return BucketNotFound{Bucket: params[0]}
		}
	case errVolumeNotEmpty:
		if len(params) >= 1 {
			return BucketNotEmpty{Bucket: params[0]}
		}
	case errVolumeExists:
		if len(params) >= 1 {
			return BucketExists{Bucket: params[0]}
		}
	case errDiskFull:
		return StorageFull{}
	case errIsNotRegular, errFileAccessDenied:
		if len(params) >= 2 {
			return ObjectExistsAsDirectory{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileNotFound:
		if len(params) >= 2 {
			return ObjectNotFound{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileNameTooLong:
		if len(params) >= 2 {
			return ObjectNameInvalid{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errXLReadQuorum:
		return InsufficientReadQuorum{}
	case errXLWriteQuorum:
		return InsufficientWriteQuorum{}
	case io.ErrUnexpectedEOF, io.ErrShortWrite:
		return IncompleteBody{}
	}
	return err
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

// BucketExists bucket exists.
type BucketExists GenericError

func (e BucketExists) Error() string {
	return "Bucket exists: " + e.Bucket
}

// BadDigest - Content-MD5 you specified did not match what we received.
type BadDigest struct {
	ExpectedMD5   string
	CalculatedMD5 string
}

func (e BadDigest) Error() string {
	return "Bad digest: Expected " + e.ExpectedMD5 + " is not valid with what we calculated " + e.CalculatedMD5
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

// IncompleteBody You did not provide the number of bytes specified by the Content-Length HTTP header.
type IncompleteBody GenericError

// Return string an error formatted as the given text.
func (e IncompleteBody) Error() string {
	return e.Bucket + "#" + e.Object + "has incomplete body"
}

// errInvalidRange - returned when given range value is not valid.
var errInvalidRange = errors.New("Invalid range")

// InvalidRange - invalid range typed error.
type InvalidRange struct {
	offsetBegin  int64
	offsetEnd    int64
	resourceSize int64
}

func (e InvalidRange) Error() string {
	return fmt.Sprintf("The requested range \"bytes %d-%d/%d\" is not satisfiable.", e.offsetBegin, e.offsetEnd, e.resourceSize)
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
	return "One or more of the specified parts could not be found"
}

// PartTooSmall - error if part size is less than 5MB.
type PartTooSmall struct {
	PartSize   int64
	PartNumber int
	PartETag   string
}

func (e PartTooSmall) Error() string {
	return fmt.Sprintf("Part size for %d should be atleast 5MB", e.PartNumber)
}

// NotImplemented If a feature is not implemented
type NotImplemented struct{}

func (e NotImplemented) Error() string {
	return "Not Implemented"
}
