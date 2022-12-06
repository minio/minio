// Copyright (c) 2015-2022 MinIO, Inc.
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

package mcontext

// Share a common context information between different
// packages in github.com/minio/minio

import (
	xhttp "github.com/minio/minio/internal/http"
)

// ContextTraceType represents the type of golang Context key
type ContextTraceType string

// ContextTraceKey is the key of TraceCtxt saved in a Golang context
const ContextTraceKey = ContextTraceType("ctx-trace-info")

// TraceCtxt holds related tracing data of a http request.
type TraceCtxt struct {
	RequestRecorder  *xhttp.RequestRecorder
	ResponseRecorder *xhttp.ResponseRecorder

	FuncName string
	AmzReqID string
}
