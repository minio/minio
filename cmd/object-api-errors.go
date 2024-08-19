// Copyright (c) 2015-2021 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
)

// Converts underlying storage error. Convenience function written to
// handle all cases where we have known types of errors returned by
// underlying storage layer.
func toObjectErr(oerr error, params ...string) error {
	if oerr == nil {
		return nil
	}

	// Unwarp the error first
	err := unwrapAll(oerr)

	if err == context.Canceled {
		return context.Canceled
	}

	switch err.Error() {
	case errVolumeNotFound.Error():
		apiErr := BucketNotFound{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		return apiErr
	case errVolumeNotEmpty.Error():
		apiErr := BucketNotEmpty{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		return apiErr
	case errVolumeExists.Error():
		apiErr := BucketExists{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		return apiErr
	case errDiskFull.Error():
		return StorageFull{}
	case errTooManyOpenFiles.Error():
		return SlowDown{}
	case errFileAccessDenied.Error():
		apiErr := PrefixAccessDenied{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errIsNotRegular.Error():
		apiErr := ObjectExistsAsDirectory{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errFileVersionNotFound.Error():
		apiErr := VersionNotFound{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		if len(params) >= 3 {
			apiErr.VersionID = params[2]
		}
		return apiErr
	case errMethodNotAllowed.Error():
		apiErr := MethodNotAllowed{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errFileNotFound.Error():
		apiErr := ObjectNotFound{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errUploadIDNotFound.Error():
		apiErr := InvalidUploadID{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		if len(params) >= 3 {
			apiErr.UploadID = params[2]
		}
		return apiErr
	case errFileNameTooLong.Error():
		apiErr := ObjectNameInvalid{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errDataTooLarge.Error():
		apiErr := ObjectTooLarge{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errDataTooSmall.Error():
		apiErr := ObjectTooSmall{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case errErasureReadQuorum.Error():
		apiErr := InsufficientReadQuorum{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		if v, ok := oerr.(InsufficientReadQuorum); ok {
			apiErr.Type = v.Type
		}
		return apiErr
	case errErasureWriteQuorum.Error():
		apiErr := InsufficientWriteQuorum{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
	case io.ErrUnexpectedEOF.Error(), io.ErrShortWrite.Error(), context.Canceled.Error(), context.DeadlineExceeded.Error():
		apiErr := IncompleteBody{}
		if len(params) >= 1 {
			apiErr.Bucket = params[0]
		}
		if len(params) >= 2 {
			apiErr.Object = decodeDirObject(params[1])
		}
		return apiErr
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
	return "Storage reached its minimum free drive threshold."
}

// SlowDown  too many file descriptors open or backend busy .
type SlowDown struct{}

func (e SlowDown) Error() string {
	return "Please reduce your request rate"
}

// RQErrType reason for read quorum error.
type RQErrType int

const (
	// RQInsufficientOnlineDrives - not enough online drives.
	RQInsufficientOnlineDrives RQErrType = 1 << iota
	// RQInconsistentMeta - inconsistent metadata.
	RQInconsistentMeta
)

func (t RQErrType) String() string {
	switch t {
	case RQInsufficientOnlineDrives:
		return "InsufficientOnlineDrives"
	case RQInconsistentMeta:
		return "InconsistentMeta"
	default:
		return "Unknown"
	}
}

// InsufficientReadQuorum storage cannot satisfy quorum for read operation.
type InsufficientReadQuorum struct {
	Bucket string
	Object string
	Err    error
	Type   RQErrType
}

func (e InsufficientReadQuorum) Error() string {
	return "Storage resources are insufficient for the read operation " + e.Bucket + "/" + e.Object
}

// Unwrap the error.
func (e InsufficientReadQuorum) Unwrap() error {
	return errErasureReadQuorum
}

// InsufficientWriteQuorum storage cannot satisfy quorum for write operation.
type InsufficientWriteQuorum GenericError

func (e InsufficientWriteQuorum) Error() string {
	return "Storage resources are insufficient for the write operation " + e.Bucket + "/" + e.Object
}

// Unwrap the error.
func (e InsufficientWriteQuorum) Unwrap() error {
	return errErasureWriteQuorum
}

// GenericError - generic object layer error.
type GenericError struct {
	Bucket    string
	Object    string
	VersionID string
	Err       error
}

// Unwrap the error to its underlying error.
func (e GenericError) Unwrap() error {
	return e.Err
}

// InvalidArgument incorrect input argument
type InvalidArgument GenericError

func (e InvalidArgument) Error() string {
	if e.Err != nil {
		return "Invalid arguments provided for " + e.Bucket + "/" + e.Object + ": (" + e.Err.Error() + ")"
	}
	return "Invalid arguments provided for " + e.Bucket + "/" + e.Object
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

// InvalidVersionID invalid version id
type InvalidVersionID GenericError

func (e InvalidVersionID) Error() string {
	return "Invalid version id: " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
}

// VersionNotFound version does not exist.
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

// ObjectLocked object is currently WORM protected.
type ObjectLocked GenericError

func (e ObjectLocked) Error() string {
	return "Object is WORM protected and cannot be overwritten: " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
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

// PrefixAccessDenied object access is denied.
type PrefixAccessDenied GenericError

func (e PrefixAccessDenied) Error() string {
	return "Prefix access is denied: " + e.Bucket + SlashSeparator + e.Object
}

// BucketExists bucket exists.
type BucketExists GenericError

func (e BucketExists) Error() string {
	return "Bucket exists: " + e.Bucket
}

// InvalidUploadIDKeyCombination - invalid upload id and key marker combination.
type InvalidUploadIDKeyCombination struct {
	UploadIDMarker, KeyMarker string
}

func (e InvalidUploadIDKeyCombination) Error() string {
	return fmt.Sprintf("Invalid combination of uploadID marker '%s' and marker '%s'", e.UploadIDMarker, e.KeyMarker)
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

// BucketReplicationConfigNotFound - no bucket replication config found
type BucketReplicationConfigNotFound GenericError

func (e BucketReplicationConfigNotFound) Error() string {
	return "The replication configuration was not found: " + e.Bucket
}

// BucketRemoteDestinationNotFound bucket does not exist.
type BucketRemoteDestinationNotFound GenericError

func (e BucketRemoteDestinationNotFound) Error() string {
	return "Destination bucket does not exist: " + e.Bucket
}

// BucketRemoteTargetNotFound remote target does not exist.
type BucketRemoteTargetNotFound GenericError

func (e BucketRemoteTargetNotFound) Error() string {
	return "Remote target not found: " + e.Bucket
}

// RemoteTargetConnectionErr remote target connection failure.
type RemoteTargetConnectionErr struct {
	Err       error
	Bucket    string
	Endpoint  string
	AccessKey string
}

func (e RemoteTargetConnectionErr) Error() string {
	if e.Bucket != "" {
		return fmt.Sprintf("Remote service endpoint offline, target bucket: %s or remote service credentials: %s invalid \n\t%s", e.Bucket, e.AccessKey, e.Err.Error())
	}
	return fmt.Sprintf("Remote service endpoint %s not available\n\t%s", e.Endpoint, e.Err.Error())
}

// BucketRemoteIdenticalToSource remote already exists for this target type.
type BucketRemoteIdenticalToSource struct {
	GenericError
	Endpoint string
}

func (e BucketRemoteIdenticalToSource) Error() string {
	return fmt.Sprintf("Remote service endpoint %s is self referential to current cluster", e.Endpoint)
}

// BucketRemoteAlreadyExists remote already exists for this target type.
type BucketRemoteAlreadyExists GenericError

func (e BucketRemoteAlreadyExists) Error() string {
	return "Remote already exists for this bucket: " + e.Bucket
}

// BucketRemoteLabelInUse remote already exists for this target label.
type BucketRemoteLabelInUse GenericError

func (e BucketRemoteLabelInUse) Error() string {
	return "Remote with this label already exists for this bucket: " + e.Bucket
}

// BucketRemoteArnTypeInvalid arn type for remote is not valid.
type BucketRemoteArnTypeInvalid GenericError

func (e BucketRemoteArnTypeInvalid) Error() string {
	return "Remote ARN type not valid: " + e.Bucket
}

// BucketRemoteArnInvalid arn needs to be specified.
type BucketRemoteArnInvalid GenericError

func (e BucketRemoteArnInvalid) Error() string {
	return "Remote ARN has invalid format: " + e.Bucket
}

// BucketRemoteRemoveDisallowed when replication configuration exists
type BucketRemoteRemoveDisallowed GenericError

func (e BucketRemoteRemoveDisallowed) Error() string {
	return "Replication configuration exists with this ARN:" + e.Bucket
}

// BucketRemoteTargetNotVersioned remote target does not have versioning enabled.
type BucketRemoteTargetNotVersioned GenericError

func (e BucketRemoteTargetNotVersioned) Error() string {
	return "Remote target does not have versioning enabled: " + e.Bucket
}

// BucketReplicationSourceNotVersioned replication source does not have versioning enabled.
type BucketReplicationSourceNotVersioned GenericError

func (e BucketReplicationSourceNotVersioned) Error() string {
	return "Replication source does not have versioning enabled: " + e.Bucket
}

// TransitionStorageClassNotFound remote tier not configured.
type TransitionStorageClassNotFound GenericError

func (e TransitionStorageClassNotFound) Error() string {
	return "Transition storage class not found "
}

// InvalidObjectState restore-object doesn't apply for the current state of the object.
type InvalidObjectState GenericError

func (e InvalidObjectState) Error() string {
	return "The operation is not valid for the current state of the object " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
}

// Bucket related errors.

// BucketNameInvalid - bucketname provided is invalid.
type BucketNameInvalid GenericError

// Error returns string an error formatted as the given text.
func (e BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + e.Bucket
}

// Object related errors.

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
	return "Object name contains forward slash as prefix: " + e.Bucket + "/" + e.Object
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
	return e.Bucket + "/" + e.Object + " has incomplete body"
}

// InvalidRange - invalid range typed error.
type InvalidRange struct {
	OffsetBegin  int64
	OffsetEnd    int64
	ResourceSize int64
}

func (e InvalidRange) Error() string {
	return fmt.Sprintf("The requested range 'bytes=%d-%d' is not satisfiable", e.OffsetBegin, e.OffsetEnd)
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
type OperationTimedOut struct{}

func (e OperationTimedOut) Error() string {
	return "Operation timed out"
}

// Multipart related errors.

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

// BackendDown is returned for network errors
type BackendDown struct {
	Err string
}

func (e BackendDown) Error() string {
	return e.Err
}

// NotImplemented If a feature is not implemented
type NotImplemented struct {
	Message string
}

func (e NotImplemented) Error() string {
	return e.Message
}

// UnsupportedMetadata - unsupported metadata
type UnsupportedMetadata struct{}

func (e UnsupportedMetadata) Error() string {
	return "Unsupported headers in Metadata"
}

// isErrBucketNotFound - Check if error type is BucketNotFound.
func isErrBucketNotFound(err error) bool {
	if errors.Is(err, errVolumeNotFound) {
		return true
	}

	var bkNotFound BucketNotFound
	return errors.As(err, &bkNotFound)
}

// isErrReadQuorum check if the error type is InsufficientReadQuorum
func isErrReadQuorum(err error) bool {
	var rquorum InsufficientReadQuorum
	return errors.As(err, &rquorum)
}

// isErrWriteQuorum check if the error type is InsufficientWriteQuorum
func isErrWriteQuorum(err error) bool {
	var rquorum InsufficientWriteQuorum
	return errors.As(err, &rquorum)
}

// isErrObjectNotFound - Check if error type is ObjectNotFound.
func isErrObjectNotFound(err error) bool {
	if errors.Is(err, errFileNotFound) {
		return true
	}

	var objNotFound ObjectNotFound
	return errors.As(err, &objNotFound)
}

// isErrVersionNotFound - Check if error type is VersionNotFound.
func isErrVersionNotFound(err error) bool {
	if errors.Is(err, errFileVersionNotFound) {
		return true
	}

	var versionNotFound VersionNotFound
	return errors.As(err, &versionNotFound)
}

// isErrSignatureDoesNotMatch - Check if error type is SignatureDoesNotMatch.
func isErrSignatureDoesNotMatch(err error) bool {
	var signatureDoesNotMatch SignatureDoesNotMatch
	return errors.As(err, &signatureDoesNotMatch)
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

// isErrMethodNotAllowed - Check if error type is MethodNotAllowed.
func isErrMethodNotAllowed(err error) bool {
	var methodNotAllowed MethodNotAllowed
	return errors.As(err, &methodNotAllowed)
}

func isErrInvalidRange(err error) bool {
	if errors.Is(err, errInvalidRange) {
		return true
	}
	_, ok := err.(InvalidRange)
	return ok
}

// ReplicationPermissionCheck - Check if error type is ReplicationPermissionCheck.
type ReplicationPermissionCheck struct{}

func (e ReplicationPermissionCheck) Error() string {
	return "Replication permission validation requests cannot be completed"
}

func isReplicationPermissionCheck(err error) bool {
	_, ok := err.(ReplicationPermissionCheck)
	return ok
}

// DataMovementOverwriteErr - captures the error when a data movement activity
// like rebalance incorrectly tries to overwrite an object.
type DataMovementOverwriteErr GenericError

func (de DataMovementOverwriteErr) Error() string {
	objInfoStr := fmt.Sprintf("bucket=%s object=%s", de.Bucket, de.Object)
	if de.VersionID != "" {
		objInfoStr = fmt.Sprintf("%s version-id=%s", objInfoStr, de.VersionID)
	}
	return fmt.Sprintf("invalid data movement operation, source and destination pool are the same for %s", objInfoStr)
}

func isDataMovementOverWriteErr(err error) bool {
	var de DataMovementOverwriteErr
	return errors.As(err, &de)
}
