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

import "testing"

// Tests authorized RPC client.
func TestAuthRPCClient(t *testing.T) {
	authCfg := &authConfig{
		accessKey:   "123",
		secretKey:   "123",
		secureConn:  false,
		address:     "localhost:9000",
		path:        "/rpc/disk",
		loginMethod: "MyPackage.LoginHandler",
	}
	authRPC := newAuthClient(authCfg)
	if authRPC.Node() != authCfg.address {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.Node(), authCfg.address)
	}
	if authRPC.RPCPath() != authCfg.path {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.RPCPath(), authCfg.path)
	}
	authCfg = &authConfig{
		accessKey:   "123",
		secretKey:   "123",
		secureConn:  false,
		loginMethod: "MyPackage.LoginHandler",
	}
	authRPC = newAuthClient(authCfg)
	if authRPC.Node() != authCfg.address {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.Node(), authCfg.address)
	}
	if authRPC.RPCPath() != authCfg.path {
		t.Fatalf("Unexpected node value %s, but expected %s", authRPC.RPCPath(), authCfg.path)
	}
}
