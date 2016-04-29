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
	"io"
)

// validates location constraint from the request body.
// the location value in the request body should match the Region in serverConfig.
// other values of location are not accepted.
// make bucket fails in such cases.
func isValidLocationContraint(reqBody io.Reader, serverRegion string) APIErrorCode {
	var locationContraint createBucketLocationConfiguration
	var errCode APIErrorCode
	errCode = ErrNone
	e := xmlDecoder(reqBody, &locationContraint)
	if e != nil {
		if e == io.EOF {
			// Do nothing.
			// failed due to empty body. The location will be set to default value from the serverConfig.
			// this is valid.
			errCode = ErrNone
		} else {
			// Failed due to malformed configuration.
			errCode = ErrMalformedXML
			//writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		}
	} else {
		// Region obtained from the body.
		// It should be equal to Region in serverConfig.
		// Else ErrInvalidRegion returned.
		// For empty value location will be to set to  default value from the serverConfig.
		if locationContraint.Location != "" && serverRegion != locationContraint.Location {
			//writeErrorResponse(w, r, ErrInvalidRegion, r.URL.Path)
			errCode = ErrInvalidRegion
		}
	}
	return errCode
}
