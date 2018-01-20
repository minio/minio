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
	"crypto/x509"
	"os"
	"path"
	"testing"
)

// Tests authorized RPC client.
func TestAuthRPCClient(t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()

	authCfg := authConfig{
		accessKey:       "123",
		secretKey:       "123",
		serverAddr:      "localhost:9000",
		serviceEndpoint: "/rpc/disk",
		secureConn:      false,
		serviceName:     "MyPackage",
	}
	authRPC := newAuthRPCClient(authCfg)
	if authRPC.ServerAddr() != authCfg.serverAddr {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.ServerAddr(), authCfg.serverAddr)
	}
	if authRPC.ServiceEndpoint() != authCfg.serviceEndpoint {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.ServiceEndpoint(), authCfg.serviceEndpoint)
	}
	authCfg = authConfig{
		accessKey:   "123",
		secretKey:   "123",
		secureConn:  false,
		serviceName: "MyPackage",
	}
	authRPC = newAuthRPCClient(authCfg)
	if authRPC.ServerAddr() != authCfg.serverAddr {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.ServerAddr(), authCfg.serverAddr)
	}
	if authRPC.ServiceEndpoint() != authCfg.serviceEndpoint {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.ServiceEndpoint(), authCfg.serviceEndpoint)
	}
}

// Test rpc dial test.
func TestRPCDial(t *testing.T) {
	prevRootCAs := globalRootCAs
	defer func() {
		globalRootCAs = prevRootCAs
	}()

	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	testServer := StartTestServer(t, "")
	defer testServer.Stop()

	cert, key, err := generateTLSCertKey("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	// Set global root CAs.
	globalRootCAs = x509.NewCertPool()
	globalRootCAs.AppendCertsFromPEM(cert)

	testServerTLS := StartTestTLSServer(t, "", cert, key)
	defer testServerTLS.Stop()

	adminEndpoint := path.Join(minioReservedBucketPath, adminPath)
	testCases := []struct {
		serverAddr     string
		serverEndpoint string
		success        bool
		secure         bool
	}{
		// Empty server addr should fail.
		{
			serverAddr:     "",
			serverEndpoint: adminEndpoint,
			success:        false,
		},
		// Unexpected server addr should fail.
		{
			serverAddr:     "example.com",
			serverEndpoint: adminEndpoint,
			success:        false,
		},
		// Server addr connects but fails for CONNECT call.
		{
			serverAddr:     "example.com:80",
			serverEndpoint: "/",
			success:        false,
		},
		// Successful connecting to insecure RPC server.
		{
			serverAddr:     testServer.Server.Listener.Addr().String(),
			serverEndpoint: adminEndpoint,
			success:        true,
		},
		// Successful connecting to secure RPC server.
		{
			serverAddr:     testServerTLS.Server.Listener.Addr().String(),
			serverEndpoint: adminEndpoint,
			success:        true,
			secure:         true,
		},
	}
	for i, testCase := range testCases {
		_, err = rpcDial(testCase.serverAddr, testCase.serverEndpoint, testCase.secure)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but found failure instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but found success instead", i+1)
		}
	}
}
