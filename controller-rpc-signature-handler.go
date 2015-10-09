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

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/probe"
)

type rpcSignatureHandler struct {
	handler http.Handler
}

// RPCSignatureHandler to validate authorization header for the incoming request.
func RPCSignatureHandler(h http.Handler) http.Handler {
	return signatureHandler{h}
}

type rpcSignature struct {
	AccessKeyID     string
	SecretAccessKey string
	Signature       string
	SignedHeaders   []string
	Request         *http.Request
}

// getCanonicalHeaders generate a list of request headers with their values
func (r *rpcSignature) getCanonicalHeaders(signedHeaders map[string][]string) string {
	var headers []string
	vals := make(map[string][]string)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(r.Request.Host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func (r *rpcSignature) getSignedHeaders(signedHeaders map[string][]string) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// extractSignedHeaders extract signed headers from Authorization header
func (r rpcSignature) extractSignedHeaders() map[string][]string {
	extractedSignedHeadersMap := make(map[string][]string)
	for _, header := range r.SignedHeaders {
		val, ok := r.Request.Header[http.CanonicalHeaderKey(header)]
		if !ok {
			// if not found continue, we will fail later
			continue
		}
		extractedSignedHeadersMap[header] = val
	}
	return extractedSignedHeadersMap
}

// getCanonicalRequest generate a canonical request of style
//
// canonicalRequest =
//  <HTTPMethod>\n
//  <CanonicalURI>\n
//  <CanonicalQueryString>\n
//  <CanonicalHeaders>\n
//  <SignedHeaders>\n
//  <HashedPayload>
//
func (r *rpcSignature) getCanonicalRequest() string {
	payload := r.Request.Header.Get(http.CanonicalHeaderKey("x-amz-content-sha256"))
	r.Request.URL.RawQuery = strings.Replace(r.Request.URL.Query().Encode(), "+", "%20", -1)
	encodedPath := getURLEncodedName(r.Request.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		r.Request.Method,
		encodedPath,
		r.Request.URL.RawQuery,
		r.getCanonicalHeaders(r.extractSignedHeaders()),
		r.getSignedHeaders(r.extractSignedHeaders()),
		payload,
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service
func (r rpcSignature) getScope(t time.Time) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"milkyway",
		"rpc",
		"rpc_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values
func (r rpcSignature) getStringToSign(canonicalRequest string, t time.Time) string {
	stringToSign := authHeaderPrefix + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + r.getScope(t) + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sha256.Sum256([]byte(canonicalRequest)))
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature
func (r rpcSignature) getSigningKey(t time.Time) []byte {
	secret := r.SecretAccessKey
	date := sumHMAC([]byte("MINIO"+secret), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("rpc"))
	signingKey := sumHMAC(service, []byte("rpc_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form
func (r rpcSignature) getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

func (r rpcSignature) DoesSignatureMatch(hashedPayload string) (bool, *probe.Error) {
	// set new calulated payload
	r.Request.Header.Set("X-Minio-Content-Sha256", hashedPayload)

	// Add date if not present throw error
	var date string
	if date = r.Request.Header.Get(http.CanonicalHeaderKey("x-minio-date")); date == "" {
		if date = r.Request.Header.Get("Date"); date == "" {
			return false, probe.NewError(errMissingDateHeader)
		}
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return false, probe.NewError(err)
	}
	canonicalRequest := r.getCanonicalRequest()
	stringToSign := r.getStringToSign(canonicalRequest, t)
	signingKey := r.getSigningKey(t)
	newSignature := r.getSignature(signingKey, stringToSign)

	if newSignature != r.Signature {
		return false, nil
	}
	return true, nil
}

func isRequestSignatureRPC(req *http.Request) bool {
	if _, ok := req.Header["Authorization"]; ok {
		return ok
	}
	return false
}

func (s rpcSignatureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var signature *rpcSignature
	if isRequestSignatureRPC(r) {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureRPC(r)
		if err != nil {
			switch err.ToGoError() {
			case errInvalidRegion:
				errorIf(err.Trace(), "Unknown region in authorization header.", nil)
				writeErrorResponse(w, r, AuthorizationHeaderMalformed, r.URL.Path)
				return
			case errAccessKeyIDInvalid:
				errorIf(err.Trace(), "Invalid access key id.", nil)
				writeErrorResponse(w, r, InvalidAccessKeyID, r.URL.Path)
				return
			default:
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, r, InternalError, r.URL.Path)
				return
			}
		}
		buffer := new(bytes.Buffer)
		if _, err := io.Copy(buffer, r.Body); err != nil {
			errorIf(probe.NewError(err), "Unable to read payload from request body.", nil)
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		value := sha256.Sum256(buffer.Bytes())
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(value[:]))
		if err != nil {
			errorIf(err.Trace(), "Unable to verify signature.", nil)
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		s.handler.ServeHTTP(w, r)
		return
	}
	writeErrorResponse(w, r, AccessDenied, r.URL.Path)
}
