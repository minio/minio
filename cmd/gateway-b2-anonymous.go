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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// mkRange converts offset, size into Range header equivalent.
func mkRange(offset, size int64) string {
	if offset == 0 && size == 0 {
		return ""
	}
	if size == 0 {
		return fmt.Sprintf("%s%d-", byteRangePrefix, offset)
	}
	return fmt.Sprintf("%s%d-%d", byteRangePrefix, offset, offset+size-1)
}

// AnonGetObject - performs a plain http GET request on a public resource,
// fails if the resource is not public.
func (l *b2Objects) AnonGetObject(bucket string, object string, startOffset int64, length int64, writer io.Writer) error {
	uri := fmt.Sprintf("%s/file/%s/%s", l.b2Client.DownloadURI, bucket, object)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return b2ToObjectError(traceError(err), bucket, object)
	}
	rng := mkRange(startOffset, length)
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := l.anonClient.Do(req)
	if err != nil {
		return b2ToObjectError(traceError(err), bucket, object)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return b2ToObjectError(traceError(errors.New(resp.Status)), bucket, object)
	}
	_, err = io.Copy(writer, resp.Body)
	return b2ToObjectError(traceError(err), bucket, object)
}

// Converts http Header into ObjectInfo. This function looks for all the
// standard Backblaze B2 headers to convert into ObjectInfo.
//
// Content-Length is converted to Size.
// X-Bz-Upload-Timestamp is converted to ModTime.
// X-Bz-Info-<header>:<value> is converted to <header>:<value>
// Content-Type is converted to ContentType.
// X-Bz-Content-Sha1 is converted to ETag.
func headerToObjectInfo(bucket, object string, header http.Header) (objInfo ObjectInfo, err error) {
	clen, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return objInfo, b2ToObjectError(traceError(err), bucket, object)
	}

	// Converting upload timestamp in milliseconds to a time.Time value for ObjectInfo.ModTime.
	timeStamp, err := strconv.ParseInt(header.Get("X-Bz-Upload-Timestamp"), 10, 64)
	if err != nil {
		return objInfo, b2ToObjectError(traceError(err), bucket, object)
	}

	// Populate user metadata by looking for all the X-Bz-Info-<name>
	// HTTP headers, ignore other headers since they have their own
	// designated meaning, for more details refer B2 API documentation.
	userMetadata := make(map[string]string)
	for key := range header {
		if strings.HasPrefix(key, "X-Bz-Info-") {
			var name string
			name, err = url.QueryUnescape(strings.TrimPrefix(key, "X-Bz-Info-"))
			if err != nil {
				return objInfo, b2ToObjectError(traceError(err), bucket, object)
			}
			var val string
			val, err = url.QueryUnescape(header.Get(key))
			if err != nil {
				return objInfo, b2ToObjectError(traceError(err), bucket, object)
			}
			userMetadata[name] = val
		}
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ContentType: header.Get("Content-Type"),
		ModTime:     time.Unix(0, 0).Add(time.Duration(timeStamp) * time.Millisecond),
		Size:        clen,
		ETag:        header.Get("X-Bz-File-Id"),
		UserDefined: userMetadata,
	}, nil
}

// AnonGetObjectInfo - performs a plain http HEAD request on a public resource,
// fails if the resource is not public.
func (l *b2Objects) AnonGetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	uri := fmt.Sprintf("%s/file/%s/%s", l.b2Client.DownloadURI, bucket, object)
	req, err := http.NewRequest("HEAD", uri, nil)
	if err != nil {
		return objInfo, b2ToObjectError(traceError(err), bucket, object)
	}
	resp, err := l.anonClient.Do(req)
	if err != nil {
		return objInfo, b2ToObjectError(traceError(err), bucket, object)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return objInfo, b2ToObjectError(traceError(errors.New(resp.Status)), bucket, object)
	}
	return headerToObjectInfo(bucket, object, resp.Header)
}
