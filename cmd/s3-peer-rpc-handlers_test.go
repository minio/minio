/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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
	"os"
	"path"
	"testing"
)

type TestRPCS3PeerSuite struct {
	testServer   TestServer
	testAuthConf authConfig
	disks        []string
}

// Set up the suite and start the test server.
func (s *TestRPCS3PeerSuite) SetUpSuite(t *testing.T) {
	s.testServer, s.disks = StartTestS3PeerRPCServer(t)
	s.testAuthConf = authConfig{
		serverAddr:      s.testServer.Server.Listener.Addr().String(),
		accessKey:       s.testServer.AccessKey,
		secretKey:       s.testServer.SecretKey,
		serviceEndpoint: path.Join(minioReservedBucketPath, s3Path),
		serviceName:     "S3",
	}
}

func (s *TestRPCS3PeerSuite) TearDownSuite(t *testing.T) {
	s.testServer.Stop()
	removeRoots(s.disks)
	os.RemoveAll(s.testServer.Root)
}

func TestS3PeerRPC(t *testing.T) {
	// setup
	s := &TestRPCS3PeerSuite{}
	s.SetUpSuite(t)

	// run test
	s.testS3PeerRPC(t)

	// teardown
	s.TearDownSuite(t)
}

// Test S3 RPC handlers
func (s *TestRPCS3PeerSuite) testS3PeerRPC(t *testing.T) {
	// Validate for invalid token.
	args := AuthRPCArgs{}
	rclient := newAuthRPCClient(s.testAuthConf)
	defer rclient.Close()

	if err := rclient.Login(); err != nil {
		t.Fatal(err)
	}

	rclient.authToken = "garbage"
	err := rclient.Call("S3.SetBucketNotificationPeer", &args, &AuthRPCReply{})
	if err != nil {
		if err.Error() != errInvalidToken.Error() {
			t.Fatal(err)
		}
	}

	// Check bucket notification call works.
	BNPArgs := SetBucketNotificationPeerArgs{Bucket: "bucket", NCfg: &notificationConfig{}}
	client := newAuthRPCClient(s.testAuthConf)
	defer client.Close()
	err = client.Call("S3.SetBucketNotificationPeer", &BNPArgs, &AuthRPCReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Check bucket listener update call works.
	BLPArgs := SetBucketListenerPeerArgs{Bucket: "bucket", LCfg: nil}
	err = client.Call("S3.SetBucketListenerPeer", &BLPArgs, &AuthRPCReply{})
	if err != nil {
		t.Fatal(err)
	}

	BPPArgs := SetBucketPolicyPeerArgs{Bucket: "bucket"}
	err = client.Call("S3.SetBucketPolicyPeer", &BPPArgs, &AuthRPCReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Check event send event call works.
	evArgs := EventArgs{Event: nil, Arn: "localhost:9000"}
	err = client.Call("S3.Event", &evArgs, &AuthRPCReply{})
	if err != nil {
		t.Fatal(err)
	}
}
