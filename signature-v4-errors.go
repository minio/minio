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

package main

import (
	"fmt"

	"github.com/minio/minio/pkg/probe"
)

type errFunc func(msg string, a ...string) *probe.Error

// generic error factory which wraps around probe.NewError()
func errFactory() errFunc {
	return func(msg string, a ...string) *probe.Error {
		return probe.NewError(fmt.Errorf("%s, Args: %s", msg, a)).Untrace()
	}
}

// Various signature v4 errors.
var (
	ErrPolicyAlreadyExpired  = errFactory()
	ErrInvalidRegion         = errFactory()
	ErrInvalidDateFormat     = errFactory()
	ErrInvalidService        = errFactory()
	ErrInvalidRequestVersion = errFactory()
	ErrMissingFields         = errFactory()
	ErrMissingCredTag        = errFactory()
	ErrCredMalformed         = errFactory()
	ErrMissingSignTag        = errFactory()
	ErrMissingSignHeadersTag = errFactory()
	ErrMissingDateHeader     = errFactory()
	ErrMalformedDate         = errFactory()
	ErrMalformedExpires      = errFactory()
	ErrAuthHeaderEmpty       = errFactory()
	ErrUnsuppSignAlgo        = errFactory()
	ErrExpiredPresignRequest = errFactory()
	ErrRegionISEmpty         = errFactory()
	ErrInvalidAccessKey      = errFactory()
)
