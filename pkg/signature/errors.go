/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package signature

// MissingDateHeader date header missing
type MissingDateHeader struct{}

func (e MissingDateHeader) Error() string {
	return "Missing date header"
}

// MissingExpiresQuery expires query string missing
type MissingExpiresQuery struct{}

func (e MissingExpiresQuery) Error() string {
	return "Missing expires query string"
}

// ExpiredPresignedRequest request already expired
type ExpiredPresignedRequest struct{}

func (e ExpiredPresignedRequest) Error() string {
	return "Presigned request already expired"
}

// DoesNotMatch invalid signature
type DoesNotMatch struct {
	SignatureSent       string
	SignatureCalculated string
}

func (e DoesNotMatch) Error() string {
	return "The request signature we calculated does not match the signature you provided"
}
