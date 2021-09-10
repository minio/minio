// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"net/http"
	"net/url"
)

// Writes S3 compatible copy part range error.
func writeCopyPartErr(ctx context.Context, w http.ResponseWriter, err error, url *url.URL) {
	switch err {
	case errInvalidRange:
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopyPartRange), url)
		return
	case errInvalidRangeSource:
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopyPartRangeSource), url)
		return
	default:
		apiErr := errorCodes.ToAPIErr(ErrInvalidCopyPartRangeSource)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, url)
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
