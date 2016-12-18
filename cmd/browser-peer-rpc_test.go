/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"path"
	"testing"
)

// API suite container common to both FS and XL.
type TestRPCBrowserPeerSuite struct {
	serverType   string
	testServer   TestServer
	testAuthConf *authConfig
}

// Setting up the test suite and starting the Test server.
func (s *TestRPCBrowserPeerSuite) SetUpSuite(c *testing.T) {
	s.testServer = StartTestBrowserPeerRPCServer(c, s.serverType)
	s.testAuthConf = &authConfig{
		address:     s.testServer.Server.Listener.Addr().String(),
		accessKey:   s.testServer.AccessKey,
		secretKey:   s.testServer.SecretKey,
		path:        path.Join(reservedBucket, browserPeerPath),
		loginMethod: "BrowserPeer.LoginHandler",
	}
}

// No longer used with gocheck, but used in explicit teardown code in
// each test function. // Called implicitly by "gopkg.in/check.v1"
// after all tests are run.
func (s *TestRPCBrowserPeerSuite) TearDownSuite(c *testing.T) {
	s.testServer.Stop()
}

func TestBrowserPeerRPC(t *testing.T) {
	// setup code
	s := &TestRPCBrowserPeerSuite{serverType: "XL"}
	s.SetUpSuite(t)

	// run test
	s.testBrowserPeerRPC(t)

	// teardown code
	s.TearDownSuite(t)
}

// Tests for browser peer rpc.
func (s *TestRPCBrowserPeerSuite) testBrowserPeerRPC(t *testing.T) {
	// Construct RPC call arguments.
	creds := credential{
		AccessKeyID:     "abcd1",
		SecretAccessKey: "abcd1234",
	}

	// Validate for invalid token.
	args := SetAuthPeerArgs{Creds: creds}
	args.Token = "garbage"
	rclient := newRPCClient(s.testAuthConf.address, s.testAuthConf.path, false)
	defer rclient.Close()
	err := rclient.Call("BrowserPeer.SetAuthPeer", &args, &GenericReply{})
	if err != nil {
		if err.Error() != errInvalidToken.Error() {
			t.Fatal(err)
		}
	}

	// Validate for successful Peer update.
	args = SetAuthPeerArgs{Creds: creds}
	client := newAuthClient(s.testAuthConf)
	defer client.Close()
	err = client.Call("BrowserPeer.SetAuthPeer", &args, &GenericReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Validate for failure in login handler with previous credentials.
	rclient = newRPCClient(s.testAuthConf.address, s.testAuthConf.path, false)
	defer rclient.Close()
	rargs := &RPCLoginArgs{
		Username: s.testAuthConf.accessKey,
		Password: s.testAuthConf.secretKey,
	}
	rreply := &RPCLoginReply{}
	err = rclient.Call("BrowserPeer.LoginHandler", rargs, rreply)
	if err != nil {
		if err.Error() != errInvalidAccessKeyID.Error() {
			t.Fatal(err)
		}
	}

	// Validate for success in loing handled with valid credetnails.
	rargs = &RPCLoginArgs{
		Username: creds.AccessKeyID,
		Password: creds.SecretAccessKey,
	}
	rreply = &RPCLoginReply{}
	err = rclient.Call("BrowserPeer.LoginHandler", rargs, rreply)
	if err != nil {
		t.Fatal(err)
	}
	// Validate all the replied fields after successful login.
	if rreply.Token == "" {
		t.Fatalf("Generated token cannot be empty %s", errInvalidToken)
	}
	if rreply.Timestamp.IsZero() {
		t.Fatal("Time stamp returned cannot be zero")
	}
}
