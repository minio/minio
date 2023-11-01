// Copyright (c) 2015-2023 MinIO, Inc.
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

package grid

import (
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/pubsub"
)

// traceRequests adds request tracing to the connection.
func (c *Connection) traceRequests(p *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]) {
	c.trace = &tracer{
		Publisher: p,
		TraceType: madmin.TraceInternal,
		Prefix:    "grid.",
		Local:     c.Local,
		Remote:    c.Remote,
		Subroute:  "",
	}
}

// traceRequests adds request tracing to the connection.
func (c *tracer) subroute(subroute string) *tracer {
	if c == nil {
		return nil
	}
	c2 := *c
	c2.Subroute = subroute
	return &c2
}

type tracer struct {
	Publisher *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]
	TraceType madmin.TraceType
	Prefix    string
	Local     string
	Remote    string
	Subroute  string
}

func (c *muxClient) traceRoundtrip(t *tracer, h HandlerID, req []byte) ([]byte, error) {
	if t == nil || t.Publisher.NumSubscribers(t.TraceType) == 0 {
		return c.roundtrip(h, req)
	}
	start := time.Now()
	body := bytesOrLength(req)
	resp, err := c.roundtrip(h, req)
	end := time.Now()
	status := 200
	errString := ""
	if err != nil {
		errString = err.Error()
		if IsRemoteErr(err) == nil {
			status = 500
		} else {
			status = 400
		}
	}
	trace := madmin.TraceInfo{
		TraceType: t.TraceType,
		FuncName:  t.Prefix + h.String(),
		NodeName:  t.Local,
		Time:      start,
		Duration:  end.Sub(start),
		Path:      t.Subroute,
		Error:     errString,
		HTTP: &madmin.TraceHTTPStats{
			ReqInfo: madmin.TraceRequestInfo{
				Time:    start,
				Proto:   "grid",
				Method:  "REQ",
				Client:  t.Remote,
				Headers: nil,
				Path:    t.Subroute,
				Body:    []byte(body),
			},
			RespInfo: madmin.TraceResponseInfo{
				Time:       end,
				Headers:    nil,
				StatusCode: status,
				Body:       []byte(bytesOrLength(resp)),
			},
			CallStats: madmin.TraceCallStats{
				InputBytes:      len(req),
				OutputBytes:     len(resp),
				TimeToFirstByte: end.Sub(start),
			},
		},
	}
	t.Publisher.Publish(trace)
	return resp, err
}
