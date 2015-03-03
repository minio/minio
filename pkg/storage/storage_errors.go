/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package storage

type BackendError struct {
	Path string
}

type GenericError struct {
	Bucket string
	Path   string
}

type ObjectExists struct {
	Bucket string
	Key    string
}

type ApiNotImplemented struct {
	Api string
}

type ObjectNotFound GenericObjectError

type GenericBucketError struct {
	Bucket string
}

type GenericObjectError struct {
	Bucket string
	Object string
}

type ImplementationError struct {
	Bucket string
	Object string
	Err    error
}

type BackendCorrupted BackendError
type BucketPolicyNotFound GenericBucketError
type BucketNameInvalid GenericBucketError
type BucketExists GenericBucketError
type BucketNotFound GenericBucketError
type ObjectNameInvalid GenericObjectError

// Return string an error formatted as the given text
func (self ImplementationError) Error() string {
	error := ""
	if self.Bucket != "" {
		error = error + "Bucket: " + self.Bucket + " "
	}
	if self.Object != "" {
		error = error + "Object: " + self.Object + " "
	}
	error = error + "Error: " + self.Err.Error()
	return error
}

// Wrapper function for error object
func EmbedError(bucket, object string, err error) ImplementationError {
	return ImplementationError{
		Bucket: bucket,
		Object: object,
		Err:    err,
	}
}

// Return string an error formatted as the given text
func (self BucketPolicyNotFound) Error() string {
	return "Bucket policy not found for: " + self.Bucket
}

// Return string an error formatted as the given text
func (self ObjectNotFound) Error() string {
	return "Object not Found: " + self.Bucket + "#" + self.Object
}

// Return string an error formatted as the given text
func (self ApiNotImplemented) Error() string {
	return "Api not implemented: " + self.Api
}

// Return string an error formatted as the given text
func (self ObjectExists) Error() string {
	return "Object exists: " + self.Bucket + "#" + self.Key
}

// Return string an error formatted as the given text
func (self BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + self.Bucket
}

// Return string an error formatted as the given text
func (self BucketExists) Error() string {
	return "Bucket exists: " + self.Bucket
}

// Return string an error formatted as the given text
func (self BucketNotFound) Error() string {
	return "Bucket not Found: " + self.Bucket
}

// Return string an error formatted as the given text
func (self ObjectNameInvalid) Error() string {
	return "Object name invalid: " + self.Bucket + "#" + self.Object
}

// Return string an error formatted as the given text
func (self BackendCorrupted) Error() string {
	return "Backend corrupted: " + self.Path
}
