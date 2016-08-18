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

// errCode represents the return status of shutdown functions
type errCode int

const (
	exitFailure errCode = -1
	exitSuccess errCode = 0
)

// errSyslogNotSupported - this message is only meaningful on windows
var errSyslogNotSupported = errors.New("Syslog logger not supported on windows")

// errInvalidArgument means that input argument is invalid.
var errInvalidArgument = errors.New("Invalid arguments specified")

// errSignatureMismatch means signature did not match.
var errSignatureMismatch = errors.New("Signature does not match")

// used when token used for authentication by the MinioBrowser has expired
var errInvalidToken = errors.New("Invalid token")

// If x-amz-content-sha256 header value mismatches with what we calculate.
var errContentSHA256Mismatch = errors.New("sha256 mismatch")

// used when we deal with data larger than expected
var errSizeUnexpected = errors.New("data size larger than expected")
