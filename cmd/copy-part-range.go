/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"net/url"
)

// Writes S3 compatible copy part range error.
func writeCopyPartErr(ctx context.Context, w http.ResponseWriter, err error, url *url.URL, browser bool) {
	switch err {
	case errInvalidRange:
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopyPartRange), url, browser)
		return
	case errInvalidRangeSource:
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopyPartRangeSource), url, browser)
		return
	default:
		apiErr := errorCodes.ToAPIErr(ErrInvalidCopyPartRangeSource)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, url, browser)
		return
	}
}

// Parses x-amz-copy-source-range for CopyObjectPart API. Its behavior
// is different from regular HTTP range header. It only supports the
// form `bytes=first-last` where first and last are zero-based byte
// offsets. See
// http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
// for full details. This function treats an empty rangeString as
// referring to the whole resource.
func parseCopyPartRangeSpec(rangeString string) (hrange *HTTPRangeSpec, err error) {
	hrange, err = parseRequestRangeSpec(rangeString)
	if err != nil {
		return nil, err
	}
	if hrange.IsSuffixLength || hrange.Start < 0 || hrange.End < 0 {
		return nil, errInvalidRange
	}
	return hrange, nil
}

// checkCopyPartRangeWithSize adds more check to the range string in case of
// copy object part. This API requires having specific start and end  range values
// e.g. 'bytes=3-10'. Other use cases will be rejected.
func checkCopyPartRangeWithSize(rs *HTTPRangeSpec, resourceSize int64) (err error) {
	if rs == nil {
		return nil
	}
	if rs.IsSuffixLength || rs.Start >= resourceSize || rs.End >= resourceSize {
		return errInvalidRangeSource
	}
	return nil
}
