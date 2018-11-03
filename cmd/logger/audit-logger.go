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

package logger

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// Represents the current version of audit log structure.
const auditLogVersion = "1"

// AuditEntry - audit entry logs.
type AuditEntry struct {
	Version      string            `json:"version"`
	DeploymentID string            `json:"deploymentid,omitempty"`
	Time         string            `json:"time"`
	API          *api              `json:"api,omitempty"`
	RemoteHost   string            `json:"remotehost,omitempty"`
	RequestID    string            `json:"requestID,omitempty"`
	UserAgent    string            `json:"userAgent,omitempty"`
	ReqQuery     map[string]string `json:"requestQuery,omitempty"`
	ReqHeader    map[string]string `json:"requestHeader,omitempty"`
	RespHeader   map[string]string `json:"responseHeader,omitempty"`
}

// AuditTargets is the list of enabled audit loggers
var AuditTargets = []LoggingTarget{}

// AddAuditTarget adds a new audit logger target to the
// list of enabled loggers
func AddAuditTarget(t LoggingTarget) {
	AuditTargets = append(AuditTargets, t)
}

// AuditLog - logs audit logs to all targets.
func AuditLog(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if Disable {
		return
	}

	req := GetReqInfo(ctx)
	if req == nil {
		req = &ReqInfo{API: "SYSTEM"}
	}

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

	// Send audit logs only to http targets.
	for _, t := range AuditTargets {
		t.send(AuditEntry{
			Version:      auditLogVersion,
			DeploymentID: deploymentID,
			RemoteHost:   req.RemoteHost,
			RequestID:    req.RequestID,
			UserAgent:    req.UserAgent,
			Time:         time.Now().UTC().Format(time.RFC3339Nano),
			API: &api{
				Name: req.API,
				Args: &args{
					Bucket: req.BucketName,
					Object: req.ObjectName,
				},
			},
			ReqQuery:   reqQuery,
			ReqHeader:  reqHeader,
			RespHeader: respHeader,
		})
	}
}
