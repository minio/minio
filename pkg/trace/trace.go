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

// Info - represents a trace record, additionally
// also reports errors if any while listening on trace.
type Info struct {
	NodeName  string       `json:"nodename"`
	FuncName  string       `json:"funcname"`
	ReqInfo   RequestInfo  `json:"request"`
	RespInfo  ResponseInfo `json:"response"`
	CallStats CallStats    `json:"stats"`
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
