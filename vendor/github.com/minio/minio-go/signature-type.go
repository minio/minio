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

// SignatureType is type of Authorization requested for a given HTTP request.
type SignatureType int

// Different types of supported signatures - default is Latest i.e SignatureV4.
const (
	Latest SignatureType = iota
	SignatureV4
	SignatureV2
)

// isV2 - is signature SignatureV2?
func (s SignatureType) isV2() bool {
	return s == SignatureV2
}

// isV4 - is signature SignatureV4?
func (s SignatureType) isV4() bool {
	return s == SignatureV4 || s == Latest
}
