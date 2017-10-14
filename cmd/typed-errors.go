/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

import "errors"

// errInvalidArgument means that input argument is invalid.
var errInvalidArgument = errors.New("Invalid arguments specified")

// errSignatureMismatch means signature did not match.
var errSignatureMismatch = errors.New("Signature does not match")

// used when token used for authentication by the MinioBrowser has expired
var errInvalidToken = errors.New("Invalid token")

// used when we deal with data larger than expected
var errSizeUnexpected = errors.New("Data size larger than expected")

// used when we deal with data with unknown size
var errSizeUnspecified = errors.New("Data size is unspecified")

// When upload object size is greater than 5G in a single PUT/POST operation.
var errDataTooLarge = errors.New("Object size larger than allowed limit")

// When upload object size is less than what was expected.
var errDataTooSmall = errors.New("Object size smaller than expected")

// errServerNotInitialized - server not initialized.
var errServerNotInitialized = errors.New("Server not initialized, please try again")

// errServerVersionMismatch - server versions do not match.
var errServerVersionMismatch = errors.New("Server versions do not match")

// errServerTimeMismatch - server times are too far apart.
var errServerTimeMismatch = errors.New("Server times are too far apart")

// errInvalidBucketName - bucket name is reserved for Minio, usually
// returned for 'minio', '.minio.sys', buckets with capital letters.
var errInvalidBucketName = errors.New("The specified bucket is not valid")

// errInvalidRange - returned when given range value is not valid.
var errInvalidRange = errors.New("Invalid range")

// errInvalidRangeSource - returned when given range value exceeds
// the source object size.
var errInvalidRangeSource = errors.New("Range specified exceeds source object size")

// errOperationTimedOut - operation timed out.
var errOperationTimedOut = errors.New("Operation timed out")
