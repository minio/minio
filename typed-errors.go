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

package main

import "errors"

// errMissingAuthHeader means that Authorization header
// has missing value or it is empty.
var errMissingAuthHeaderValue = errors.New("Missing auth header value")

// errInvalidAuthHeaderValue means that Authorization
// header is available but is malformed and not in
// accordance with signature v4.
var errInvalidAuthHeaderValue = errors.New("Invalid auth header value")

// errInvalidAuthHeaderPrefix means that Authorization header
// has a wrong prefix only supported value should be "AWS4-HMAC-SHA256".
var errInvalidAuthHeaderPrefix = errors.New("Invalid auth header prefix")

// errMissingFieldsAuthHeader means that Authorization
// header is available but has some missing fields.
var errMissingFieldsAuthHeader = errors.New("Missing fields in auth header")

// errMissingFieldsCredentialTag means that Authorization
// header credentials tag has some missing fields.
var errMissingFieldsCredentialTag = errors.New("Missing fields in crendential tag")

// errMissingFieldsSignedHeadersTag means that Authorization
// header signed headers tag has some missing fields.
var errMissingFieldsSignedHeadersTag = errors.New("Missing fields in signed headers tag")

// errMissingFieldsSignatureTag means that Authorization
// header signature tag has missing fields.
var errMissingFieldsSignatureTag = errors.New("Missing fields in signature tag")

// errCredentialTagMalformed means that Authorization header
// credential tag is malformed.
var errCredentialTagMalformed = errors.New("Invalid credential tag malformed")

// errInvalidRegion means that the region element from credential tag
// in Authorization header is invalid.
var errInvalidRegion = errors.New("Invalid region")

// errAccessKeyIDInvalid means that the accessKeyID element from
// credential tag in Authorization header is invalid.
var errAccessKeyIDInvalid = errors.New("AccessKeyID invalid")

// errUnsupportedAlgorithm means that the provided X-Amz-Algorithm is unsupported.
var errUnsupportedAlgorithm = errors.New("Unsupported Algorithm")

// errPolicyAlreadyExpired means that the client request carries an post policy
// header which is already expired.
var errPolicyAlreadyExpired = errors.New("Policy already expired")

// errPolicyMissingFields means that form values and policy header have some fields missing.
var errPolicyMissingFields = errors.New("Some fields are missing or do not match in policy")

// errMissingDateHeader means that date header is missing
var errMissingDateHeader = errors.New("Missing date header on the request")
