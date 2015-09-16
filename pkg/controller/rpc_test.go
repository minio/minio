/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"

	jsonrpc "github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/controller/rpc"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

var testRPCServer *httptest.Server

func (s *MySuite) SetUpSuite(c *C) {
	testRPCServer = httptest.NewServer(getRPCHandler())
}

func (s *MySuite) TearDownSuite(c *C) {
	testRPCServer.Close()
}

func (s *MySuite) TestMemStats(c *C) {
	op := rpc.Operation{
		Method:  "Server.MemStats",
		Request: rpc.Args{Request: ""},
	}
	req, err := rpc.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.MemStatsReply
	c.Assert(jsonrpc.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.MemStatsReply{})
}

func (s *MySuite) TestSysInfo(c *C) {
	op := rpc.Operation{
		Method:  "Server.SysInfo",
		Request: rpc.Args{Request: ""},
	}
	req, err := rpc.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.SysInfoReply
	c.Assert(jsonrpc.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.SysInfoReply{})
}

func (s *MySuite) TestAuth(c *C) {
	op := rpc.Operation{
		Method:  "Auth.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := rpc.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.AuthReply
	c.Assert(jsonrpc.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.AuthReply{})
	c.Assert(len(reply.AccessKeyID), Equals, 20)
	c.Assert(len(reply.SecretAccessKey), Equals, 40)
}
