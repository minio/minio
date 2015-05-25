/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio/pkg/api/config"
	"github.com/minio/minio/pkg/utils/crypto/keys"
)

type contentTypeHandler struct {
	handler http.Handler
}

type timeHandler struct {
	handler http.Handler
}

type validateAuthHandler struct {
	handler http.Handler
}

type resourceHandler struct {
	handler http.Handler
}

type auth struct {
	prefix        string
	credential    string
	signedheaders string
	signature     string
	accessKey     string
}

const (
	timeFormat = "20060102T150405Z"
)

const (
	authHeaderPrefix = "AWS4-HMAC-SHA256"
)

// strip auth from authorization header
func stripAuth(r *http.Request) (*auth, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, errors.New("Missing auth header")
	}
	a := new(auth)
	authFields := strings.Split(authHeader, ",")
	if len(authFields) != 3 {
		return nil, errors.New("Missing fields in Auth header")
	}
	authPrefixFields := strings.Fields(authFields[0])
	if len(authPrefixFields) != 2 {
		return nil, errors.New("Missing fields in Auth header")
	}
	if authPrefixFields[0] != authHeaderPrefix {
		return nil, errors.New("Missing fields is Auth header")
	}
	credentials := strings.Split(authPrefixFields[1], "=")
	if len(credentials) != 2 {
		return nil, errors.New("Missing fields in Auth header")
	}
	signedheaders := strings.Split(authFields[1], "=")
	if len(signedheaders) != 2 {
		return nil, errors.New("Missing fields in Auth header")
	}
	signature := strings.Split(authFields[2], "=")
	if len(signature) != 2 {
		return nil, errors.New("Missing fields in Auth header")
	}
	a.credential = credentials[1]
	a.signedheaders = signedheaders[1]
	a.signature = signature[1]
	a.accessKey = strings.Split(a.credential, "/")[0]
	if !keys.IsValidAccessKey(a.accessKey) {
		return nil, errors.New("Invalid access key")
	}
	return a, nil
}

func getDate(req *http.Request) (time.Time, error) {
	amzDate := req.Header.Get("X-Amz-Date")
	switch {
	case amzDate != "":
		if _, err := time.Parse(time.RFC1123, amzDate); err == nil {
			return time.Parse(time.RFC1123, amzDate)
		}
		if _, err := time.Parse(time.RFC1123Z, amzDate); err == nil {
			return time.Parse(time.RFC1123Z, amzDate)
		}
		if _, err := time.Parse(timeFormat, amzDate); err == nil {
			return time.Parse(timeFormat, amzDate)
		}
	}
	date := req.Header.Get("Date")
	switch {
	case date != "":
		if _, err := time.Parse(time.RFC1123, date); err == nil {
			return time.Parse(time.RFC1123, date)
		}
		if _, err := time.Parse(time.RFC1123Z, date); err == nil {
			return time.Parse(time.RFC1123Z, date)
		}
		if _, err := time.Parse(timeFormat, amzDate); err == nil {
			return time.Parse(timeFormat, amzDate)
		}
	}
	return time.Time{}, errors.New("invalid request")
}

func validContentTypeHandler(h http.Handler) http.Handler {
	return contentTypeHandler{h}
}

func (h contentTypeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	if acceptsContentType == unknownContentType {
		writeErrorResponse(w, r, NotAcceptable, acceptsContentType, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

func timeValidityHandler(h http.Handler) http.Handler {
	return timeHandler{h}
}

func (h timeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	// Verify if date headers are set, if not reject the request
	if r.Header.Get("Authorization") != "" {
		if r.Header.Get("X-Amz-Date") == "" && r.Header.Get("Date") == "" {
			// there is no way to knowing if this is a valid request, could be a attack reject such clients
			writeErrorResponse(w, r, RequestTimeTooSkewed, acceptsContentType, r.URL.Path)
			return
		}
		date, err := getDate(r)
		if err != nil {
			// there is no way to knowing if this is a valid request, could be a attack reject such clients
			writeErrorResponse(w, r, RequestTimeTooSkewed, acceptsContentType, r.URL.Path)
			return
		}
		duration := time.Since(date)
		minutes := time.Duration(5) * time.Minute
		if duration.Minutes() > minutes.Minutes() {
			writeErrorResponse(w, r, RequestTimeTooSkewed, acceptsContentType, r.URL.Path)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

// validate auth header handler is wrapper handler used for API request validation with authorization header.
// Current authorization layer supports S3's standard HMAC based signature request.
func validateAuthHeaderHandler(h http.Handler) http.Handler {
	return validateAuthHandler{h}
}

// validate auth header handler ServeHTTP() wrapper
func (h validateAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	_, err := stripAuth(r)
	switch err.(type) {
	case nil:
		var conf = config.Config{}
		if err := conf.SetupConfig(); err != nil {
			writeErrorResponse(w, r, InternalError, acceptsContentType, r.URL.Path)
			return
		}
		if err := conf.ReadConfig(); err != nil {
			writeErrorResponse(w, r, InternalError, acceptsContentType, r.URL.Path)
			return
		}
		// uncomment this when we have webcli
		// _, ok := conf.Users[auth.accessKey]
		//if !ok {
		//	writeErrorResponse(w, r, AccessDenied, acceptsContentType, r.URL.Path)
		//	return
		//}
		// Success
		h.handler.ServeHTTP(w, r)
	default:
		// control reaches here, we should just send the request up the stack - internally
		// individual calls will validate themselves against un-authenticated requests
		h.handler.ServeHTTP(w, r)
	}
}

// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating such a feature to have been not implemented.
func ignoreResourcesHandler(h http.Handler) http.Handler {
	return resourceHandler{h}
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	if ignoreNotImplementedObjectResources(r) || ignoreNotImplementedBucketResources(r) {
		error := getErrorCode(NotImplemented)
		errorResponse := getErrorResponse(error, "")
		encodeErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
		setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodeErrorResponse))
		w.WriteHeader(error.HTTPStatusCode)
		w.Write(encodeErrorResponse)
		return
	}
	h.handler.ServeHTTP(w, r)
}

//// helpers

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if notimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}
