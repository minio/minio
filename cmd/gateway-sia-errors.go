/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

var siaErrorUnknown = &SiaServiceError{
	Code:    "SiaErrorUnknown",
	Message: "An unknown error has occurred.",
}
var siaErrorObjectDoesNotExistInBucket = &SiaServiceError{
	Code:    "SiaErrorObjectDoesNotExistInBucket",
	Message: "Object does not exist in bucket.",
}
var siaErrorObjectAlreadyExists = &SiaServiceError{
	Code:    "SiaErrorObjectAlreadyExists",
	Message: "Object already exists in bucket.",
}
var siaErrorInvalidObjectName = &SiaServiceError{
	Code:    "SiaErrorInvalidObjectName",
	Message: "Object name not suitable for Sia.",
}
var siaErrorNotImplemented = &SiaServiceError{
	Code:    "SiaErrorNotImplemented",
	Message: "Not Implemented",
}
