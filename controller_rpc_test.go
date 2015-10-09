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

package main

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"

	"github.com/gorilla/rpc/v2/json"
	. "gopkg.in/check.v1"
)

type ControllerRPCSuite struct {
	root   string
	url    *url.URL
	req    *http.Request
	body   io.ReadSeeker
	config *AuthConfig
}

var _ = Suite(&ControllerRPCSuite{})

var (
	testControllerRPC *httptest.Server
	testServerRPC     *httptest.Server
)

func (s *ControllerRPCSuite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(err, IsNil)
	s.root = root
	SetAuthConfigPath(root)

	secretAccessKey, perr := generateSecretAccessKey()
	c.Assert(perr, IsNil)

	authConf := &AuthConfig{}
	authConf.Users = make(map[string]*AuthUser)
	authConf.Users["admin"] = &AuthUser{
		Name:            "admin",
		AccessKeyID:     "admin",
		SecretAccessKey: string(secretAccessKey),
	}
	s.config = authConf

	SetAuthConfigPath(root)
	perr = SaveConfig(authConf)
	c.Assert(perr, IsNil)

	testControllerRPC = httptest.NewServer(getControllerRPCHandler(false))
	testServerRPC = httptest.NewUnstartedServer(getServerRPCHandler(false))
	testServerRPC.Config.Addr = ":9002"
	testServerRPC.Start()

	url, gerr := url.Parse(testServerRPC.URL)
	c.Assert(gerr, IsNil)
	s.url = url
}

func (s *ControllerRPCSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	testServerRPC.Close()
	testControllerRPC.Close()
}

func (s *ControllerRPCSuite) TestMemStats(c *C) {
	op := rpcOperation{
		Method:  "Controller.GetServerMemStats",
		Request: ControllerArgs{Host: s.url.Host},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply MemStatsRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), MemStatsRep{})
}

func (s *ControllerRPCSuite) TestDiskStats(c *C) {
	op := rpcOperation{
		Method:  "Controller.GetServerDiskStats",
		Request: ControllerArgs{Host: s.url.Host},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply MemStatsRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), DiskStatsRep{})
}

func (s *ControllerRPCSuite) TestSysInfo(c *C) {
	op := rpcOperation{
		Method:  "Controller.GetServerSysInfo",
		Request: ControllerArgs{Host: s.url.Host},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply SysInfoRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), SysInfoRep{})
}

func (s *ControllerRPCSuite) TestServerList(c *C) {
	op := rpcOperation{
		Method:  "Controller.ListServers",
		Request: ControllerArgs{Host: s.url.Host},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply ServerListRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), ServerListRep{List: []ServerRep{}})
}

func (s *ControllerRPCSuite) TestServerAdd(c *C) {
	op := rpcOperation{
		Method:  "Controller.AddServer",
		Request: ControllerArgs{Host: s.url.Host},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply DefaultRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), DefaultRep{nil, "Added"})
}

func (s *ControllerRPCSuite) TestAuth(c *C) {
	op := rpcOperation{
		Method:  "Controller.GenerateAuth",
		Request: AuthArgs{User: "newuser"},
	}
	req, err := newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err := req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var reply AuthRep
	c.Assert(json.DecodeClientResponse(resp.Body, &reply), IsNil)
	resp.Body.Close()
	c.Assert(reply, Not(DeepEquals), AuthRep{})
	c.Assert(len(reply.AccessKeyID), Equals, 20)
	c.Assert(len(reply.SecretAccessKey), Equals, 40)
	c.Assert(len(reply.Name), Not(Equals), 0)

	op = rpcOperation{
		Method:  "Controller.FetchAuth",
		Request: AuthArgs{User: "newuser"},
	}
	req, err = newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err = req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var newReply AuthRep
	c.Assert(json.DecodeClientResponse(resp.Body, &newReply), IsNil)
	resp.Body.Close()
	c.Assert(newReply, Not(DeepEquals), AuthRep{})
	c.Assert(reply.AccessKeyID, Equals, newReply.AccessKeyID)
	c.Assert(reply.SecretAccessKey, Equals, newReply.SecretAccessKey)
	c.Assert(len(reply.Name), Not(Equals), 0)

	op = rpcOperation{
		Method:  "Controller.ResetAuth",
		Request: AuthArgs{User: "newuser"},
	}
	req, err = newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err = req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	var resetReply AuthRep
	c.Assert(json.DecodeClientResponse(resp.Body, &resetReply), IsNil)
	resp.Body.Close()
	c.Assert(newReply, Not(DeepEquals), AuthRep{})
	c.Assert(reply.AccessKeyID, Not(Equals), resetReply.AccessKeyID)
	c.Assert(reply.SecretAccessKey, Not(Equals), resetReply.SecretAccessKey)
	c.Assert(len(reply.Name), Not(Equals), 0)

	// these operations should fail

	/// generating access for existing user fails
	op = rpcOperation{
		Method:  "Controller.GenerateAuth",
		Request: AuthArgs{User: "newuser"},
	}
	req, err = newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err = req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	/// null user provided invalid
	op = rpcOperation{
		Method:  "Controller.GenerateAuth",
		Request: AuthArgs{User: ""},
	}
	req, err = newRPCRequest(s.config, testControllerRPC.URL+"/rpc", op, http.DefaultTransport)
	c.Assert(err, IsNil)
	c.Assert(req.Get("Content-Type"), Equals, "application/json")
	resp, err = req.Do()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}
