/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"context"
	"net/http"
)

// Represents additional fields necessary for ErrPartTooSmall S3 error.
type completeMultipartAPIError struct {
	// Proposed size represents uploaded size of the part.
	ProposedSize int64
	// Minimum size allowed epresents the minimum size allowed per
	// part. Defaults to 5MB.
	MinSizeAllowed int64
	// Part number of the part which is incorrect.
	PartNumber int
	// ETag of the part which is incorrect.
	PartETag string
	// Other default XML error responses.
	APIErrorResponse
}

// writeErrorResponsePartTooSmall - function is used specifically to
// construct a proper error response during CompleteMultipartUpload
// when one of the parts is < 5MB.
// The requirement comes due to the fact that generic ErrorResponse
// XML doesn't carry the additional fields required to send this
// error. So we construct a new type which lies well within the scope
// of this function.
func writePartSmallErrorResponse(w http.ResponseWriter, r *http.Request, err PartTooSmall) {

	apiError := getAPIError(toAPIErrorCode(context.Background(), err))
	// Generate complete multipart error response.
	errorResponse := getAPIErrorResponse(apiError, r.URL.Path, w.Header().Get(responseRequestIDKey))
	cmpErrResp := completeMultipartAPIError{err.PartSize, int64(5242880), err.PartNumber, err.PartETag, errorResponse}
	encodedErrorResponse := encodeResponse(cmpErrResp)

	// respond with 400 bad request.
	w.WriteHeader(apiError.HTTPStatusCode)
	// Write error body.
	w.Write(encodedErrorResponse)
	w.(http.Flusher).Flush()
}
