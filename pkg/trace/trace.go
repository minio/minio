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
	TraceType Type `json:"type"`

	NodeName string    `json:"nodename"`
	FuncName string    `json:"funcname"`
	Time     time.Time `json:"time"`

	ReqInfo   RequestInfo  `json:"request"`
	RespInfo  ResponseInfo `json:"response"`
	CallStats CallStats    `json:"stats"`

	StorageStats StorageStats `json:"storageStats"`
	OSStats      OSStats      `json:"osStats"`
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
	Proto    string      `json:"proto"`
	Method   string      `json:"method"`
	Path     string      `json:"path,omitempty"`
	RawQuery string      `json:"rawquery,omitempty"`
	Headers  http.Header `json:"headers,omitempty"`
	Body     []byte      `json:"body,omitempty"`
	Client   string      `json:"client"`
}

// ResponseInfo represents trace of http request
type ResponseInfo struct {
	Time       time.Time   `json:"time"`
	Headers    http.Header `json:"headers,omitempty"`
	Body       []byte      `json:"body,omitempty"`
	StatusCode int         `json:"statuscode,omitempty"`
}
