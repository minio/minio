/*
 * (C) 2017 David Gore <dvstate@gmail.com>
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

import(
 	"fmt"
)

// SiaServiceError is a custom error type used by Sia cache layer
type SiaServiceError struct {
	Code    string
	Message string
}

func (e SiaServiceError) Error() string {
	return fmt.Sprintf("Sia Error: %s",
		e.Message)
}

// Also: SiaErrorDaemon is a valid code

var siaErrorUnableToClearAnyCachedFiles = &SiaServiceError{
	Code:    "SiaErrorUnableToClearAnyCachedFiles",
	Message: "Unable to clear any files from cache.",
}
var siaErrorDeterminingCacheSize = &SiaServiceError{
	Code:    "SiaErrorDeterminingCacheSize",
	Message: "Unable to determine total size of files in cache.",
}
var siaErrorDatabaseDeleteError = &SiaServiceError{
	Code:    "SiaErrorDatabaseDeleteError",
	Message: "Failed to delete a record in the cache database.",
}
var siaErrorDatabaseCreateError = &SiaServiceError{
	Code:    "SiaErrorDatabaseCreateError",
	Message: "Failed to create a table in the cache database.",
}
var siaErrorDatabaseCantBeOpened = &SiaServiceError{
	Code:    "SiaErrorDatabaseCantBeOpened",
	Message: "The cache database could not be opened.",
}
var siaErrorDatabaseInsertError = &SiaServiceError{
	Code:    "SiaErrorDatabaseInsertError",
	Message: "Failed to insert a record in the cache database.",
}
var siaErrorDatabaseUpdateError = &SiaServiceError{
	Code:    "SiaErrorDatabaseUpdateError",
	Message: "Failed to update a record in the cache database.",
}
var siaErrorDatabaseSelectError = &SiaServiceError{
	Code:    "SiaErrorDatabaseSelectError",
	Message: "Failed to select records in the cache database.",
}
var siaErrorUnknown = &SiaServiceError{
	Code:    "SiaErrorUnknown",
	Message: "An unknown error has occurred.",
}
var siaErrorFailedToDeleteCachedFile = &SiaServiceError{
	Code:    "SiaFailedToDeleteCachedFile",
	Message: "Failed to delete cached file. Check permissions.",
}
var siaErrorObjectDoesNotExistInBucket = &SiaServiceError{
	Code:    "SiaErrorObjectDoesNotExistInBucket",
	Message: "Object does not exist in bucket.",
}
var siaErrorBucketNotEmpty = &SiaServiceError{
	Code:    "SiaErrorBucketNotEmpty",
	Message: "Bucket is not empty.",
}
var siaErrorObjectAlreadyExists = &SiaServiceError{
	Code:    "SiaErrorObjectAlreadyExists",
	Message: "Object does not exist in bucket.",
}
var siaErrorInvalidBucketPolicy = &SiaServiceError{
	Code:    "SiaErrorInvalidBucketPolicy",
	Message: "An invalid bucket policy has been specified.",
}