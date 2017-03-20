/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

/// Multipart upload defaults.

// miniPartSize - minimum part size 64MiB per object after which
// putObject behaves internally as multipart.
const minPartSize = 1024 * 1024 * 64

// maxPartsCount - maximum number of parts for a single multipart session.
const maxPartsCount = 10000

// maxPartSize - maximum part size 5GiB for a single multipart upload
// operation.
const maxPartSize = 1024 * 1024 * 1024 * 5

// maxSinglePutObjectSize - maximum size 5GiB of object per PUT
// operation.
const maxSinglePutObjectSize = 1024 * 1024 * 1024 * 5

// maxMultipartPutObjectSize - maximum size 5TiB of object for
// Multipart operation.
const maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 1024 * 5

// optimalReadBufferSize - optimal buffer 5MiB used for reading
// through Read operation.
const optimalReadBufferSize = 1024 * 1024 * 5

// unsignedPayload - value to be set to X-Amz-Content-Sha256 header when
// we don't want to sign the request payload
const unsignedPayload = "UNSIGNED-PAYLOAD"

// Signature related constants.
const (
	signV4Algorithm   = "AWS4-HMAC-SHA256"
	iso8601DateFormat = "20060102T150405Z"
)
