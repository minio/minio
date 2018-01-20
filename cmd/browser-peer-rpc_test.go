/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

	"github.com/minio/minio/pkg/auth"
)

// API suite container common to both FS and XL.
type TestRPCBrowserPeerSuite struct {
	serverType   string
	testServer   TestServer
	testAuthConf authConfig
}

// Setting up the test suite and starting the Test server.
func (s *TestRPCBrowserPeerSuite) SetUpSuite(t *testing.T) {
	s.testServer = StartTestBrowserPeerRPCServer(t, s.serverType)
	s.testAuthConf = authConfig{
		serverAddr:      s.testServer.Server.Listener.Addr().String(),
		accessKey:       s.testServer.AccessKey,
		secretKey:       s.testServer.SecretKey,
		serviceEndpoint: path.Join(minioReservedBucketPath, browserPeerPath),
		serviceName:     "BrowserPeer",
	}
}

// TeatDownSuite - called implicitly by after all tests are run in
// browser peer rpc suite.
func (s *TestRPCBrowserPeerSuite) TearDownSuite(t *testing.T) {
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
	creds, err := auth.CreateCredentials("abcd1", "abcd1234")
	if err != nil {
		t.Fatalf("unable to create credential. %v", err)
	}

	// Validate for invalid token.
	args := SetAuthPeerArgs{Creds: creds}
	rclient := newAuthRPCClient(s.testAuthConf)
	defer rclient.Close()
	if err = rclient.Login(); err != nil {
		t.Fatal(err)
	}
	rclient.authToken = "garbage"
	if err = rclient.Call("BrowserPeer.SetAuthPeer", &args, &AuthRPCReply{}); err != nil {
		if err.Error() != errInvalidToken.Error() {
			t.Fatal(err)
		}
	}

	// Validate for successful Peer update.
	args = SetAuthPeerArgs{Creds: creds}
	client := newAuthRPCClient(s.testAuthConf)
	defer client.Close()
	err = client.Call("BrowserPeer.SetAuthPeer", &args, &AuthRPCReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Validate for failure in login handler with previous credentials.
	rclient = newAuthRPCClient(s.testAuthConf)
	defer rclient.Close()
	token, err := authenticateNode(creds.AccessKey, creds.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	rclient.authToken = token
	if err = rclient.Login(); err != nil {
		if err.Error() != errInvalidAccessKeyID.Error() {
			t.Fatal(err)
		}
	}

	token, err = authenticateNode(creds.AccessKey, creds.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	rclient.authToken = token
	if err = rclient.Login(); err != nil {
		t.Fatal(err)
	}
}
