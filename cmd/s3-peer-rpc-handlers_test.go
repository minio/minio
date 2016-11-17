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
	"encoding/json"
	"path"
	"testing"
	"time"
)

type TestRPCS3PeerSuite struct {
	testServer   TestServer
	testAuthConf *authConfig
	disks        []string
}

// Set up the suite and start the test server.
func (s *TestRPCS3PeerSuite) SetUpSuite(t *testing.T) {
	s.testServer, s.disks = StartTestS3PeerRPCServer(t)
	s.testAuthConf = &authConfig{
		address:     s.testServer.Server.Listener.Addr().String(),
		accessKey:   s.testServer.AccessKey,
		secretKey:   s.testServer.SecretKey,
		path:        path.Join(reservedBucket, s3Path),
		loginMethod: "S3.LoginHandler",
	}
}

func (s *TestRPCS3PeerSuite) TearDownSuite(t *testing.T) {
	s.testServer.Stop()
	removeRoots(s.disks)
	removeAll(s.testServer.Root)
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
	args := GenericArgs{Token: "garbage", Timestamp: time.Now().UTC()}
	rclient := newClient(s.testAuthConf.address, s.testAuthConf.path, false)
	defer rclient.Close()
	err := rclient.Call("S3.SetBucketNotificationPeer", &args, &GenericReply{})
	if err != nil {
		if err.Error() != errInvalidToken.Error() {
			t.Fatal(err)
		}
	}

	// Check bucket notification call works.
	BNPArgs := SetBucketNotificationPeerArgs{Bucket: "bucket", NCfg: &notificationConfig{}}
	client := newAuthClient(s.testAuthConf)
	defer client.Close()
	err = client.Call("S3.SetBucketNotificationPeer", &BNPArgs, &GenericReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Check bucket listener update call works.
	BLPArgs := SetBucketListenerPeerArgs{Bucket: "bucket", LCfg: nil}
	err = client.Call("S3.SetBucketListenerPeer", &BLPArgs, &GenericReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Check bucket policy update call works.
	pCh := policyChange{IsRemove: true}
	pChBytes, err := json.Marshal(pCh)
	if err != nil {
		t.Fatal(err)
	}
	BPPArgs := SetBucketPolicyPeerArgs{Bucket: "bucket", PChBytes: pChBytes}
	err = client.Call("S3.SetBucketPolicyPeer", &BPPArgs, &GenericReply{})
	if err != nil {
		t.Fatal(err)
	}

	// Check event send event call works.
	evArgs := EventArgs{Event: nil, Arn: "localhost:9000"}
	err = client.Call("S3.Event", &evArgs, &GenericReply{})
	if err != nil {
		t.Fatal(err)
	}
}
