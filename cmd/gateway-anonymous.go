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
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

func anonErrToObjectErr(statusCode int, params ...string) error {
	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	switch statusCode {
	case http.StatusNotFound:
		if object != "" {
			return ObjectNotFound{bucket, object}
		}
		return BucketNotFound{Bucket: bucket}
	case http.StatusBadRequest:
		if object != "" {
			return ObjectNameInvalid{bucket, object}
		}
		return BucketNameInvalid{Bucket: bucket}
	case http.StatusForbidden:
		fallthrough
	case http.StatusUnauthorized:
		return AllAccessDisabled{bucket, object}
	}

	return errUnexpected
}

// newCustomHTTPTransport returns a new http configuration
// used while communicating with the cloud backends.
// This sets the value for MaxIdleConns from 2 (go default) to
// 100.
func newCustomHTTPTransport() http.RoundTripper {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{RootCAs: globalRootCAs},
		DisableCompression:    true,
	}
}
