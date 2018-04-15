/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2017 Minio, Inc.
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

package minio

import (
	"context"
	"net/http"

	"github.com/minio/minio-go/pkg/encrypt"
)

// CopyObject - copy a source object into a new object
func (c Client) CopyObject(dst DestinationInfo, src SourceInfo) error {
	header := make(http.Header)
	for k, v := range src.Headers {
		header[k] = v
	}
	if src.encryption != nil {
		encrypt.SSECopy(src.encryption).Marshal(header)
	}
	if dst.encryption != nil {
		dst.encryption.Marshal(header)
	}
	for k, v := range dst.getUserMetaHeadersMap(true) {
		header.Set(k, v)
	}

	resp, err := c.executeMethod(context.Background(), "PUT", requestMetadata{
		bucketName:   dst.bucket,
		objectName:   dst.object,
		customHeader: header,
	})
	if err != nil {
		return err
	}
	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp, dst.bucket, dst.object)
	}
	return nil
}
