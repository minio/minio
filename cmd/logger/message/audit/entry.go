/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package audit

import (
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/handlers"
)

// Version - represents the current version of audit log structure.
const Version = "1"

// Entry - audit entry logs.
type Entry struct {
	Version      string `json:"version"`
	DeploymentID string `json:"deploymentid,omitempty"`
	Time         string `json:"time"`
	API          struct {
		Name       string `json:"name,omitempty"`
		Bucket     string `json:"bucket,omitempty"`
		Object     string `json:"object,omitempty"`
		Status     string `json:"status,omitempty"`
		StatusCode int    `json:"statusCode,omitempty"`
	} `json:"api"`
	RemoteHost string                 `json:"remotehost,omitempty"`
	RequestID  string                 `json:"requestID,omitempty"`
	UserAgent  string                 `json:"userAgent,omitempty"`
	ReqClaims  map[string]interface{} `json:"requestClaims,omitempty"`
	ReqQuery   map[string]string      `json:"requestQuery,omitempty"`
	ReqHeader  map[string]string      `json:"requestHeader,omitempty"`
	RespHeader map[string]string      `json:"responseHeader,omitempty"`
}

// ToEntry - constructs an audit entry object.
func ToEntry(w http.ResponseWriter, r *http.Request, api string, statusCode int, reqClaims map[string]interface{}) Entry {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	reqQuery := make(map[string]string)
	for k, v := range r.URL.Query() {
		reqQuery[k] = strings.Join(v, ",")
	}
	reqHeader := make(map[string]string)
	for k, v := range r.Header {
		reqHeader[k] = strings.Join(v, ",")
	}
	respHeader := make(map[string]string)
	for k, v := range w.Header() {
		respHeader[k] = strings.Join(v, ",")
	}
	respHeader["Etag"] = strings.Trim(respHeader["Etag"], `"`)

	entry := Entry{
		Version:      Version,
		DeploymentID: w.Header().Get("x-minio-deployment-id"),
		RemoteHost:   handlers.GetSourceIP(r),
		RequestID:    w.Header().Get("x-amz-request-id"),
		UserAgent:    r.UserAgent(),
		Time:         time.Now().UTC().Format(time.RFC3339Nano),
		ReqQuery:     reqQuery,
		ReqHeader:    reqHeader,
		ReqClaims:    reqClaims,
		RespHeader:   respHeader,
	}

	entry.API.Name = api
	entry.API.Bucket = bucket
	entry.API.Object = object
	entry.API.Status = http.StatusText(statusCode)
	entry.API.StatusCode = statusCode

	return entry
}
