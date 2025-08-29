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

package logger

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/minio/madmin-go/v3/logger/audit"
	internalAudit "github.com/minio/minio/internal/logger/message/audit"
	"github.com/minio/minio/internal/mcontext"

	xhttp "github.com/minio/minio/internal/http"
)

const contextAuditKey = contextKeyType("audit-entry")

// SetAuditEntry sets Audit info in the context.
func SetAuditEntry(ctx context.Context, audit *audit.Entry) context.Context {
	if ctx == nil {
		LogIf(context.Background(), "audit", fmt.Errorf("context is nil"))
		return nil
	}
	return context.WithValue(ctx, contextAuditKey, audit)
}

// GetAuditEntry returns Audit entry if set.
func GetAuditEntry(ctx context.Context) *audit.Entry {
	if ctx != nil {
		r, ok := ctx.Value(contextAuditKey).(*audit.Entry)
		if ok {
			return r
		}
		r = &audit.Entry{
			Version:      internalAudit.Version,
			DeploymentID: xhttp.GlobalDeploymentID,
			Time:         time.Now().UTC(),
		}
		return r
	}
	return nil
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(ctx context.Context, w http.ResponseWriter, r *http.Request, reqClaims map[string]any, filterKeys ...string) {
	auditTgts := AuditTargets()
	if len(auditTgts) == 0 {
		return
	}

	var entry audit.Entry
	if w != nil && r != nil {
		reqInfo := GetReqInfo(ctx)
		if reqInfo == nil {
			return
		}
		reqInfo.RLock()
		defer reqInfo.RUnlock()

		entry = internalAudit.ToEntry(w, r, reqClaims, xhttp.GlobalDeploymentID)
		// indicates all requests for this API call are inbound
		entry.Trigger = "incoming"

		for _, filterKey := range filterKeys {
			delete(entry.ReqClaims, filterKey)
			delete(entry.ReqQuery, filterKey)
			delete(entry.ReqHeader, filterKey)
			delete(entry.RespHeader, filterKey)
		}

		var (
			statusCode      int
			timeToResponse  time.Duration
			timeToFirstByte time.Duration
			outputBytes     int64 = -1 // -1: unknown output bytes
			headerBytes     int64
		)

		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			statusCode = tc.ResponseRecorder.StatusCode
			outputBytes = int64(tc.ResponseRecorder.Size())
			headerBytes = int64(tc.ResponseRecorder.HeaderSize())
			timeToResponse = time.Now().UTC().Sub(tc.ResponseRecorder.StartTime)
			timeToFirstByte = tc.ResponseRecorder.TTFB()
		}

		entry.AccessKey = reqInfo.Cred.AccessKey
		entry.ParentUser = reqInfo.Cred.ParentUser

		entry.API.Name = reqInfo.API
		entry.API.Bucket = reqInfo.BucketName
		entry.API.Object = reqInfo.ObjectName
		entry.API.Objects = make([]audit.ObjectVersion, 0, len(reqInfo.Objects))
		for _, ov := range reqInfo.Objects {
			entry.API.Objects = append(entry.API.Objects, audit.ObjectVersion{
				ObjectName: ov.ObjectName,
				VersionID:  ov.VersionID,
			})
		}
		entry.API.Status = http.StatusText(statusCode)
		entry.API.StatusCode = statusCode
		entry.API.InputBytes = r.ContentLength
		entry.API.OutputBytes = outputBytes
		entry.API.HeaderBytes = headerBytes
		entry.API.TimeToResponse = strconv.FormatInt(timeToResponse.Nanoseconds(), 10) + "ns"
		entry.API.TimeToResponseInNS = strconv.FormatInt(timeToResponse.Nanoseconds(), 10)
		// We hold the lock, so we cannot call reqInfo.GetTagsMap().
		tags := make(map[string]any, len(reqInfo.tags))
		for _, t := range reqInfo.tags {
			tags[t.Key] = t.Val
		}
		entry.Tags = tags
		// ignore cases for ttfb when its zero.
		if timeToFirstByte != 0 {
			entry.API.TimeToFirstByte = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10) + "ns"
			entry.API.TimeToFirstByteInNS = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10)
		}
	} else {
		auditEntry := GetAuditEntry(ctx)
		if auditEntry != nil {
			entry = *auditEntry
		}
	}

	// Send audit logs only to http targets.
	for _, t := range auditTgts {
		if err := t.Send(ctx, entry); err != nil {
			LogOnceIf(ctx, "logging", fmt.Errorf("Unable to send audit event(s) to the target `%v`: %v", t, err), "send-audit-event-failure")
		}
	}
}
