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

	"github.com/minio-io/minio/pkg/api/config"
)

type timeHandler struct {
	handler http.Handler
}

type validateHandler struct {
	conf    config.Config
	handler http.Handler
}

type resourceHandler struct {
	handler http.Handler
}

// strip AccessKey from authorization header
func stripAccessKey(r *http.Request) string {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) < 2 {
		return ""
	}
	splits := strings.Split(fields[1], ":")
	if len(splits) < 2 {
		return ""
	}
	return splits[0]
}

func getDate(req *http.Request) (time.Time, error) {
	if req.Header.Get("x-amz-date") != "" {
		return time.Parse(http.TimeFormat, req.Header.Get("x-amz-date"))
	}
	if req.Header.Get("Date") != "" {
		return time.Parse(http.TimeFormat, req.Header.Get("Date"))
	}
	return time.Time{}, errors.New("invalid request")
}

func timeValidityHandler(h http.Handler) http.Handler {
	return timeHandler{h}
}

func (h timeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	if acceptsContentType == unknownContentType {
		writeErrorResponse(w, r, NotAcceptable, acceptsContentType, r.URL.Path)
		return
	}
	// Verify if date headers are set, if not reject the request
	if r.Header.Get("x-amz-date") == "" && r.Header.Get("Date") == "" {
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
	h.handler.ServeHTTP(w, r)
}

// Validate handler is wrapper handler used for API request validation with authorization header.
// Current authorization layer supports S3's standard HMAC based signature request.
func validateRequestHandler(conf config.Config, h http.Handler) http.Handler {
	return validateHandler{
		conf:    conf,
		handler: h,
	}
}

// Validate handler ServeHTTP() wrapper
func (h validateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptsContentType := getContentType(r)
	if acceptsContentType == unknownContentType {
		writeErrorResponse(w, r, NotAcceptable, acceptsContentType, r.URL.Path)
		return
	}
	// success
	h.handler.ServeHTTP(w, r)

	// Enable below only when webcli is ready

	/*
		switch true {
		case accessKey != "":
			if err := h.conf.ReadConfig(); err != nil {
				writeErrorResponse(w, r, InternalError, acceptsContentType, r.URL.Path)
				return
			}
			user, ok := h.conf.Users[accessKey]
			if !ok {
				writeErrorResponse(w, r, AccessDenied, acceptsContentType, r.URL.Path)
				return
			}
			ok, _ = ValidateRequest(user, r)
			if !ok {
				writeErrorResponse(w, r, AccessDenied, acceptsContentType, r.URL.Path)
				return
			}
			// Success
			h.handler.ServeHTTP(w, r)
		default:
			// Control reaches when no access key is found, ideally we would
			// like to throw back `403`. But for now with our tests lacking
			// this functionality it is better for us to be serving anonymous
			// requests as well.
			// We should remove this after adding tests to support signature request
			h.handler.ServeHTTP(w, r)
			// ## Uncommented below links of code after disabling anonymous requests
			// error := errorCodeError(AccessDenied)
			// errorResponse := getErrorResponse(error, "")
			// w.WriteHeader(error.HTTPStatusCode)
			// w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	*/
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
	if acceptsContentType == unknownContentType {
		writeErrorResponse(w, r, NotAcceptable, acceptsContentType, r.URL.Path)
		return
	}
	if ignoreUnImplementedObjectResources(r) || ignoreUnImplementedBucketResources(r) {
		error := getErrorCode(NotImplemented)
		errorResponse := getErrorResponse(error, "")
		setCommonHeaders(w, getContentTypeString(acceptsContentType))
		w.WriteHeader(error.HTTPStatusCode)
		w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		return
	}
	h.handler.ServeHTTP(w, r)
}

//// helpers

// Checks requests for unimplemented Bucket resources
func ignoreUnImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for unimplemented Object resources
func ignoreUnImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}
