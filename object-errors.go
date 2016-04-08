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

package main

import "fmt"

// InvalidArgument invalid argument
type InvalidArgument struct{}

func (e InvalidArgument) Error() string {
	return "Invalid argument"
}

// UnsupportedFilesystem unsupported filesystem type
type UnsupportedFilesystem struct {
	Type string
}

func (e UnsupportedFilesystem) Error() string {
	return "Unsupported filesystem: " + e.Type
}

// RootPathFull root path out of space
type RootPathFull struct {
	Path string
}

func (e RootPathFull) Error() string {
	return "Root path " + e.Path + " reached its minimum free disk threshold."
}

// BucketNotFound bucket does not exist
type BucketNotFound struct {
	Bucket string
}

func (e BucketNotFound) Error() string {
	return "Bucket not found: " + e.Bucket
}

// BucketNotEmpty bucket is not empty
type BucketNotEmpty struct {
	Bucket string
}

func (e BucketNotEmpty) Error() string {
	return "Bucket not empty: " + e.Bucket
}

// ObjectNotFound object does not exist
type ObjectNotFound struct {
	Bucket string
	Object string
}

func (e ObjectNotFound) Error() string {
	return "Object not found: " + e.Bucket + "#" + e.Object
}

// ObjectExistsAsPrefix object already exists with a requested prefix.
type ObjectExistsAsPrefix struct {
	Bucket string
	Prefix string
}

func (e ObjectExistsAsPrefix) Error() string {
	return "Object exists on : " + e.Bucket + " as prefix " + e.Prefix
}

// ObjectCorrupted object found to be corrupted
type ObjectCorrupted struct {
	Object string
}

func (e ObjectCorrupted) Error() string {
	return "Object found corrupted: " + e.Object
}

// BucketExists bucket exists
type BucketExists struct {
	Bucket string
}

func (e BucketExists) Error() string {
	return "Bucket exists: " + e.Bucket
}

// CorruptedBackend backend found to be corrupted
type CorruptedBackend struct {
	Backend string
}

func (e CorruptedBackend) Error() string {
	return "Corrupted backend: " + e.Backend
}

// NotImplemented function not implemented
type NotImplemented struct {
	Function string
}

func (e NotImplemented) Error() string {
	return "Not implemented: " + e.Function
}

// InvalidDisksArgument invalid number of disks per node
type InvalidDisksArgument struct{}

func (e InvalidDisksArgument) Error() string {
	return "Invalid number of disks per node"
}

// BadDigest - Content-MD5 you specified did not match what we received.
type BadDigest struct {
	ExpectedMD5   string
	CalculatedMD5 string
}

func (e BadDigest) Error() string {
	return "Bad digest expected " + e.ExpectedMD5 + " is not valid with what we calculated " + e.CalculatedMD5
}

// InternalError - generic internal error
type InternalError struct{}

// BackendError - generic disk backend error
type BackendError struct {
	Path string
}

// BackendCorrupted - path has corrupted data
type BackendCorrupted BackendError

// APINotImplemented - generic API not implemented error
type APINotImplemented struct {
	API string
}

// GenericBucketError - generic bucket error
type GenericBucketError struct {
	Bucket string
}

// BucketPolicyNotFound - no bucket policy found.
type BucketPolicyNotFound GenericBucketError

func (e BucketPolicyNotFound) Error() string {
	return "No bucket policy found for bucket: " + e.Bucket
}

// GenericObjectError - generic object error
type GenericObjectError struct {
	Bucket string
	Object string
}

// ImplementationError - generic implementation error
type ImplementationError struct {
	Bucket string
	Object string
	Err    error
}

/// Bucket related errors

// BucketNameInvalid - bucketname provided is invalid
type BucketNameInvalid GenericBucketError

/// Object related errors

// ObjectNameInvalid - object name provided is invalid
type ObjectNameInvalid GenericObjectError

// Return string an error formatted as the given text
func (e ImplementationError) Error() string {
	error := ""
	if e.Bucket != "" {
		error = error + "Bucket: " + e.Bucket + " "
	}
	if e.Object != "" {
		error = error + "Object: " + e.Object + " "
	}
	error = error + "Error: " + e.Err.Error()
	return error
}

// EmbedError - wrapper function for error object
func EmbedError(bucket, object string, err error) ImplementationError {
	return ImplementationError{
		Bucket: bucket,
		Object: object,
		Err:    err,
	}
}

// Return string an error formatted as the given text
func (e InternalError) Error() string {
	return "Internal error occured"
}

// Return string an error formatted as the given text
func (e APINotImplemented) Error() string {
	return "Api not implemented: " + e.API
}

// Return string an error formatted as the given text
func (e BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + e.Bucket
}

// Return string an error formatted as the given text
func (e ObjectNameInvalid) Error() string {
	return "Object name invalid: " + e.Bucket + "#" + e.Object
}

// IncompleteBody You did not provide the number of bytes specified by the Content-Length HTTP header
type IncompleteBody GenericObjectError

// Return string an error formatted as the given text
func (e IncompleteBody) Error() string {
	return e.Bucket + "#" + e.Object + "has incomplete body"
}

// Return string an error formatted as the given text
func (e BackendCorrupted) Error() string {
	return "Backend corrupted: " + e.Path
}

// OperationNotPermitted - operation not permitted
type OperationNotPermitted struct {
	Op     string
	Reason string
}

func (e OperationNotPermitted) Error() string {
	return "Operation " + e.Op + " not permitted for reason: " + e.Reason
}

// InvalidRange - invalid range
type InvalidRange struct {
	Start  int64
	Length int64
}

func (e InvalidRange) Error() string {
	return fmt.Sprintf("Invalid range start:%d length:%d", e.Start, e.Length)
}

/// Multipart related errors

// InvalidUploadID invalid upload id
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

// InvalidPartOrder parts are not ordered as Requested
type InvalidPartOrder struct {
	UploadID string
}

func (e InvalidPartOrder) Error() string {
	return "Invalid part order sent for " + e.UploadID
}

// MalformedXML invalid xml format
type MalformedXML struct{}

func (e MalformedXML) Error() string {
	return "Malformed XML"
}
