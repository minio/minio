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

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	jsonrpc "github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/controller"
	"github.com/minio/minio/pkg/server/rpc"
	. "gopkg.in/check.v1"
)

func TestRPC(t *testing.T) { TestingT(t) }

type MyRPCSuite struct{}

var _ = Suite(&MyRPCSuite{})

var testRPCServer *httptest.Server

func (s *MyRPCSuite) SetUpSuite(c *C) {
	testRPCServer = httptest.NewServer(getRPCHandler())
}

func (s *MyRPCSuite) TearDownSuite(c *C) {
	testRPCServer.Close()
}

func (s *MyRPCSuite) TestMemStats(c *C) {
	op := controller.RPCOps{
		Method:  "MemStats.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := controller.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.MemStatsReply
	err = jsonrpc.DecodeClientResponse(resp.Body, &reply)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.MemStatsReply{})
}

func (s *MyRPCSuite) TestSysInfo(c *C) {
	op := controller.RPCOps{
		Method:  "SysInfo.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := controller.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.SysInfoReply
	err = jsonrpc.DecodeClientResponse(resp.Body, &reply)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.SysInfoReply{})
}

func (s *MyRPCSuite) TestAuth(c *C) {
	op := controller.RPCOps{
		Method:  "Auth.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := controller.NewRequest(testRPCServer.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply rpc.AuthReply
	err = jsonrpc.DecodeClientResponse(resp.Body, &reply)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), rpc.AuthReply{})
	c.Assert(len(reply.AccessKeyID), Equals, 20)
	c.Assert(len(reply.SecretAccessKey), Equals, 40)
}
