/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"fmt"
	"io"
	"path"
)

// Converts underlying storage error. Convenience function written to
// handle all cases where we have known types of errors returned by
// underlying storage layer.
func toObjectErr(err error, params ...string) error {
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
	case errTooManyOpenFiles:
		err = SlowDown{}
	case errFileAccessDenied:
		if len(params) >= 2 {
			err = PrefixAccessDenied{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileParentIsFile:
		if len(params) >= 2 {
			err = ParentIsObject{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errIsNotRegular:
		if len(params) >= 2 {
			err = ObjectExistsAsDirectory{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileVersionNotFound:
		switch len(params) {
		case 2:
			err = VersionNotFound{
				Bucket: params[0],
				Object: params[1],
			}
		case 3:
			err = VersionNotFound{
				Bucket:    params[0],
				Object:    params[1],
				VersionID: params[2],
			}
		}
	case errMethodNotAllowed:
		switch len(params) {
		case 2:
			err = MethodNotAllowed{
				Bucket: params[0],
				Object: params[1],
			}
		}
	case errFileNotFound:
		switch len(params) {
		case 2:
			err = ObjectNotFound{
				Bucket: params[0],
				Object: params[1],
			}
		case 3:
			err = InvalidUploadID{
				Bucket:   params[0],
				Object:   params[1],
				UploadID: params[2],
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
	case errErasureReadQuorum:
		err = InsufficientReadQuorum{}
	case errErasureWriteQuorum:
		err = InsufficientWriteQuorum{}
	case io.ErrUnexpectedEOF, io.ErrShortWrite:
		err = IncompleteBody{}
	case context.Canceled, context.DeadlineExceeded:
		err = IncompleteBody{}
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

// SlowDown  too many file descriptors open or backend busy .
type SlowDown struct{}

func (e SlowDown) Error() string {
	return "Please reduce your request rate"
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
	Bucket    string
	Object    string
	VersionID string
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

// VersionNotFound object does not exist.
type VersionNotFound GenericError

func (e VersionNotFound) Error() string {
	return "Version not found: " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
}

// ObjectNotFound object does not exist.
type ObjectNotFound GenericError

func (e ObjectNotFound) Error() string {
	return "Object not found: " + e.Bucket + "/" + e.Object
}

// MethodNotAllowed on the object
type MethodNotAllowed GenericError

func (e MethodNotAllowed) Error() string {
	return "Method not allowed: " + e.Bucket + "/" + e.Object
}

// ObjectAlreadyExists object already exists.
type ObjectAlreadyExists GenericError

func (e ObjectAlreadyExists) Error() string {
	return "Object: " + e.Bucket + "/" + e.Object + " already exists"
}

// ObjectExistsAsDirectory object already exists as a directory.
type ObjectExistsAsDirectory GenericError

func (e ObjectExistsAsDirectory) Error() string {
	return "Object exists on : " + e.Bucket + " as directory " + e.Object
}

//PrefixAccessDenied object access is denied.
type PrefixAccessDenied GenericError

func (e PrefixAccessDenied) Error() string {
	return "Prefix access is denied: " + e.Bucket + SlashSeparator + e.Object
}

// ParentIsObject object access is denied.
type ParentIsObject GenericError

func (e ParentIsObject) Error() string {
	return "Parent is object " + e.Bucket + SlashSeparator + path.Dir(e.Object)
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
	return "No bucket policy configuration found for bucket: " + e.Bucket
}

// BucketLifecycleNotFound - no bucket lifecycle found.
type BucketLifecycleNotFound GenericError

func (e BucketLifecycleNotFound) Error() string {
	return "No bucket lifecycle configuration found for bucket : " + e.Bucket
}

// BucketSSEConfigNotFound - no bucket encryption found
type BucketSSEConfigNotFound GenericError

func (e BucketSSEConfigNotFound) Error() string {
	return "No bucket encryption configuration found for bucket: " + e.Bucket
}

// BucketTaggingNotFound - no bucket tags found
type BucketTaggingNotFound GenericError

func (e BucketTaggingNotFound) Error() string {
	return "No bucket tags found for bucket: " + e.Bucket
}

// BucketObjectLockConfigNotFound - no bucket object lock config found
type BucketObjectLockConfigNotFound GenericError

func (e BucketObjectLockConfigNotFound) Error() string {
	return "No bucket object lock configuration found for bucket: " + e.Bucket
}

// BucketQuotaConfigNotFound - no bucket quota config found.
type BucketQuotaConfigNotFound GenericError

func (e BucketQuotaConfigNotFound) Error() string {
	return "No quota config found for bucket : " + e.Bucket
}

// BucketQuotaExceeded - bucket quota exceeded.
type BucketQuotaExceeded GenericError

func (e BucketQuotaExceeded) Error() string {
	return "Bucket quota exceeded for bucket: " + e.Bucket
}

/// Bucket related errors.

// BucketNameInvalid - bucketname provided is invalid.
type BucketNameInvalid GenericError

// Error returns string an error formatted as the given text.
func (e BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + e.Bucket
}

/// Object related errors.

// ObjectNameInvalid - object name provided is invalid.
type ObjectNameInvalid GenericError

// ObjectNameTooLong - object name too long.
type ObjectNameTooLong GenericError

// ObjectNamePrefixAsSlash - object name has a slash as prefix.
type ObjectNamePrefixAsSlash GenericError

// Error returns string an error formatted as the given text.
func (e ObjectNameInvalid) Error() string {
	return "Object name invalid: " + e.Bucket + "/" + e.Object
}

// Error returns string an error formatted as the given text.
func (e ObjectNameTooLong) Error() string {
	return "Object name too long: " + e.Bucket + "/" + e.Object
}

// Error returns string an error formatted as the given text.
func (e ObjectNamePrefixAsSlash) Error() string {
	return "Object name contains forward slash as pefix: " + e.Bucket + "/" + e.Object
}

// AllAccessDisabled All access to this object has been disabled
type AllAccessDisabled GenericError

// Error returns string an error formatted as the given text.
func (e AllAccessDisabled) Error() string {
	return "All access to this object has been disabled"
}

// IncompleteBody You did not provide the number of bytes specified by the Content-Length HTTP header.
type IncompleteBody GenericError

// Error returns string an error formatted as the given text.
func (e IncompleteBody) Error() string {
	return e.Bucket + "/" + e.Object + "has incomplete body"
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
}

func (e OperationTimedOut) Error() string {
	return "Operation timed out"
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
	Bucket   string
	Object   string
	UploadID string
}

func (e InvalidUploadID) Error() string {
	return "Invalid upload id " + e.UploadID
}

// InvalidPart One or more of the specified parts could not be found
type InvalidPart struct {
	PartNumber int
	ExpETag    string
	GotETag    string
}

func (e InvalidPart) Error() string {
	return fmt.Sprintf("Specified part could not be found. PartNumber %d, Expected %s, got %s",
		e.PartNumber, e.ExpETag, e.GotETag)
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
type NotImplemented struct {
	API string
}

func (e NotImplemented) Error() string {
	if e.API != "" {
		return e.API + " is Not Implemented"
	}
	return "Not Implemented"
}

// UnsupportedMetadata - unsupported metadata
type UnsupportedMetadata struct{}

func (e UnsupportedMetadata) Error() string {
	return "Unsupported headers in Metadata"
}

// BackendDown is returned for network errors or if the gateway's backend is down.
type BackendDown struct{}

func (e BackendDown) Error() string {
	return "Backend down"
}

// isErrBucketNotFound - Check if error type is BucketNotFound.
func isErrBucketNotFound(err error) bool {
	var bkNotFound BucketNotFound
	return errors.As(err, &bkNotFound)
}

// isErrObjectNotFound - Check if error type is ObjectNotFound.
func isErrObjectNotFound(err error) bool {
	var objNotFound ObjectNotFound
	return errors.As(err, &objNotFound)
}

// PreConditionFailed - Check if copy precondition failed
type PreConditionFailed struct{}

func (e PreConditionFailed) Error() string {
	return "At least one of the pre-conditions you specified did not hold"
}

func isErrPreconditionFailed(err error) bool {
	_, ok := err.(PreConditionFailed)
	return ok
}
