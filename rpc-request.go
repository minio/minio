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
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/probe"
)

// rpcOperation RPC operation
type rpcOperation struct {
	Method  string
	Request interface{}
}

// rpcRequest rpc client request
type rpcRequest struct {
	req       *http.Request
	transport http.RoundTripper
}

// newRPCRequest initiate a new client RPC request
func newRPCRequest(config *AuthConfig, url string, op rpcOperation, transport http.RoundTripper) (*rpcRequest, *probe.Error) {
	t := time.Now().UTC()
	params, err := json.EncodeClientRequest(op.Method, op.Request)
	if err != nil {
		return nil, probe.NewError(err)
	}

	body := bytes.NewReader(params)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, probe.NewError(err)
	}
	req.Header.Set("x-minio-date", t.Format(iso8601Format))

	// save for subsequent use
	hash := func() string {
		sum256Bytes, _ := sum256Reader(body)
		return hex.EncodeToString(sum256Bytes)
	}

	hashedPayload := hash()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	var headers []string
	vals := make(map[string][]string)
	for k, vv := range req.Header {
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // ignored header
		}
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)

	var canonicalHeaders bytes.Buffer
	for _, k := range headers {
		canonicalHeaders.WriteString(k)
		canonicalHeaders.WriteByte(':')
		switch {
		case k == "host":
			canonicalHeaders.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					canonicalHeaders.WriteByte(',')
				}
				canonicalHeaders.WriteString(v)
			}
			canonicalHeaders.WriteByte('\n')
		}
	}

	signedHeaders := strings.Join(headers, ";")

	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)
	encodedPath := getURLEncodedName(req.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)

	//
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		encodedPath,
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		hashedPayload,
	}, "\n")

	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"milkyway",
		"rpc",
		"rpc_request",
	}, "/")

	stringToSign := rpcAuthHeaderPrefix + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	fmt.Println(config)
	date := sumHMAC([]byte("MINIO"+config.Users["admin"].SecretAccessKey), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("rpc"))
	signingKey := sumHMAC(service, []byte("rpc_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		rpcAuthHeaderPrefix + " Credential=" + config.Users["admin"].AccessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	rpcReq := &rpcRequest{}
	rpcReq.req = req
	if transport == nil {
		transport = http.DefaultTransport
	}
	rpcReq.transport = transport
	return rpcReq, nil
}

// Do - make a http connection
func (r rpcRequest) Do() (*http.Response, *probe.Error) {
	resp, err := r.transport.RoundTrip(r.req)
	if err != nil {
		if err, ok := probe.UnwrapError(err); ok {
			return nil, err.Trace()
		}
		return nil, probe.NewError(err)
	}
	return resp, nil
}

// Get - get value of requested header
func (r rpcRequest) Get(key string) string {
	return r.req.Header.Get(key)
}

// Set - set value of a header key
func (r *rpcRequest) Set(key, value string) {
	r.req.Header.Set(key, value)
}
