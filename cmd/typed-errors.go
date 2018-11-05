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

import (
	"errors"
)

// errInvalidArgument means that input argument is invalid.
var errInvalidArgument = errors.New("Invalid arguments specified")

// errMethodNotAllowed means that method is not allowed.
var errMethodNotAllowed = errors.New("Method not allowed")

// errSignatureMismatch means signature did not match.
var errSignatureMismatch = errors.New("Signature does not match")

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

// errRPCAPIVersionUnsupported - unsupported rpc API version.
var errRPCAPIVersionUnsupported = errors.New("Unsupported rpc API version")

// errServerTimeMismatch - server times are too far apart.
var errServerTimeMismatch = errors.New("Server times are too far apart")

// errOperationTimedOut
var errOperationTimedOut = errors.New("Operation timed out")

// errInvalidBucketName - bucket name is reserved for Minio, usually
// returned for 'minio', '.minio.sys', buckets with capital letters.
var errInvalidBucketName = errors.New("The specified bucket is not valid")

// errInvalidRange - returned when given range value is not valid.
var errInvalidRange = errors.New("Invalid range")

// errInvalidRangeSource - returned when given range value exceeds
// the source object size.
var errInvalidRangeSource = errors.New("Range specified exceeds source object size")

// error returned by disks which are to be initialized are waiting for the
// first server to initialize them in distributed set to initialize them.
var errNotFirstDisk = errors.New("Not first disk")

// error returned by first disk waiting to initialize other servers.
var errFirstDiskWait = errors.New("Waiting on other disks")

// error returned when a bucket already exists
var errBucketAlreadyExists = errors.New("Your previous request to create the named bucket succeeded and you already own it")

// error returned for a negative actual size.
var errInvalidDecompressedSize = errors.New("Invalid Decompressed Size")

// error returned in IAM subsystem when user doesn't exist.
var errNoSuchUser = errors.New("Specified user does not exist")

// error returned in IAM subsystem when policy doesn't exist.
var errNoSuchPolicy = errors.New("Specified canned policy does not exist")

// error returned when access is denied.
var errAccessDenied = errors.New("Do not have enough permissions to access this resource")
