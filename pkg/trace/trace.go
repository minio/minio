/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package trace

import (
	"net/http"
	"time"
)

// Type indicates the type of the tracing Info
type Type int

const (
	// HTTP tracing (MinIO S3 & Internode)
	HTTP Type = iota
	// OS tracing (Golang os package calls)
	OS
	// Storage tracing (MinIO Storage Layer)
	Storage
)

// Info - represents a trace record, additionally
// also reports errors if any while listening on trace.
type Info struct {
	Time         time.Time    `json:"time"`
	ReqInfo      RequestInfo  `json:"request"`
	NodeName     string       `json:"nodename"`
	FuncName     string       `json:"funcname"`
	StorageStats StorageStats `json:"storageStats"`
	OSStats      OSStats      `json:"osStats"`
	RespInfo     ResponseInfo `json:"response"`
	CallStats    CallStats    `json:"stats"`
	TraceType    Type         `json:"type"`
}

// StorageStats statistics on MinIO Storage layer calls
type StorageStats struct {
	Path     string        `json:"path"`
	Duration time.Duration `json:"duration"`
}

// OSStats statistics on operating system specific calls.
type OSStats struct {
	Path     string        `json:"path"`
	Duration time.Duration `json:"duration"`
}

// CallStats records request stats
type CallStats struct {
	InputBytes      int           `json:"inputbytes"`
	OutputBytes     int           `json:"outputbytes"`
	Latency         time.Duration `json:"latency"`
	TimeToFirstByte time.Duration `json:"timetofirstbyte"`
}

// RequestInfo represents trace of http request
type RequestInfo struct {
	Time     time.Time   `json:"time"`
	Headers  http.Header `json:"headers,omitempty"`
	Proto    string      `json:"proto"`
	Method   string      `json:"method"`
	Path     string      `json:"path,omitempty"`
	RawQuery string      `json:"rawquery,omitempty"`
	Client   string      `json:"client"`
	Body     []byte      `json:"body,omitempty"`
}

// ResponseInfo represents trace of http request
type ResponseInfo struct {
	Time       time.Time   `json:"time"`
	Headers    http.Header `json:"headers,omitempty"`
	Body       []byte      `json:"body,omitempty"`
	StatusCode int         `json:"statuscode,omitempty"`
}
