/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"fmt"
	"net/rpc"
	"path"
	"strconv"

	. "gopkg.in/check.v1"
)

// API suite container common to both FS and XL.
type TestRPCControllerSuite struct {
	serverType string
	testServer TestServer
	endPoint   string
	accessKey  string
	secretKey  string
}

// Init and run test on FS backend.
var _ = Suite(&TestRPCControllerSuite{serverType: "FS"})

// Init and run test on XL backend.
var _ = Suite(&TestRPCControllerSuite{serverType: "XL"})

// Setting up the test suite.
// Starting the Test server with temporary FS backend.
func (s *TestRPCControllerSuite) SetUpSuite(c *C) {
	s.testServer = StartTestRPCServer(c, s.serverType)
	s.endPoint = s.testServer.Server.Listener.Addr().String()
	s.accessKey = s.testServer.AccessKey
	s.secretKey = s.testServer.SecretKey
}

// Called implicitly by "gopkg.in/check.v1" after all tests are run.
func (s *TestRPCControllerSuite) TearDownSuite(c *C) {
	s.testServer.Stop()
}

// Tests to validate the correctness of lock instrumentation control RPC end point.
func (s *TestRPCControllerSuite) TestRPCControlLock(c *C) {
	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock()
	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()

	var client *rpc.Client
	var err error
	client, err = rpc.DialHTTPPath("tcp", s.endPoint, path.Join(reservedBucket, controlPath))
	if err != nil {
		c.Fatal("dialing", err)
	}

	defer client.Close()

	for i := 0; i < 5; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i))
	}
	// Synchronous calls
	args := &struct{}{}
	reply := &SystemLockState{}
	err = client.Call("Control.LockInfo", args, reply)
	if err != nil {
		c.Errorf("Add: expected no error but got string %q", err.Error())
	}
	fmt.Println("RPC test log")
	b, err := json.MarshalIndent(*reply, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}
