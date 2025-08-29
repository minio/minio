// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package audit

import (
	"net/http"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3/logger/audit"

	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
)

// Version - represents the current version of audit log structure.
const Version = "1"

// NewEntry - constructs an audit entry object with some fields filled
func NewEntry(deploymentID string) audit.Entry {
	return audit.Entry{
		Version:      Version,
		DeploymentID: deploymentID,
		Time:         time.Now().UTC(),
	}
}

// ToEntry - constructs an audit entry from a http request
func ToEntry(w http.ResponseWriter, r *http.Request, reqClaims map[string]any, deploymentID string) audit.Entry {
	entry := NewEntry(deploymentID)

	entry.RemoteHost = handlers.GetSourceIP(r)
	entry.UserAgent = r.UserAgent()
	entry.ReqClaims = reqClaims
	entry.ReqHost = r.Host
	entry.ReqPath = r.URL.Path

	q := r.URL.Query()
	reqQuery := make(map[string]string, len(q))
	for k, v := range q {
		reqQuery[k] = strings.Join(v, ",")
	}
	entry.ReqQuery = reqQuery

	reqHeader := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		reqHeader[k] = strings.Join(v, ",")
	}
	entry.ReqHeader = reqHeader

	wh := w.Header()
	entry.RequestID = wh.Get(xhttp.AmzRequestID)
	respHeader := make(map[string]string, len(wh))
	for k, v := range wh {
		respHeader[k] = strings.Join(v, ",")
	}
	entry.RespHeader = respHeader

	if etag := respHeader[xhttp.ETag]; etag != "" {
		respHeader[xhttp.ETag] = strings.Trim(etag, `"`)
	}

	return entry
}
