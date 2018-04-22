/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/mux"
)

type ArithArgs struct {
	A, B int
}

type ArithReply struct {
	C int
}

type Arith struct {
	AuthRPCServer
}

// Some of Arith's methods have value args, some have pointer args. That's deliberate.
func (t *Arith) Add(args ArithArgs, reply *ArithReply) error {
	reply.C = args.A + args.B
	return nil
}

func TestGoHTTPRPC(t *testing.T) {
	newServer := newRPCServer()
	newServer.Register(&Arith{
		AuthRPCServer: AuthRPCServer{},
	})

	router := mux.NewRouter().SkipClean(true)
	router.Path("/foo").Handler(newServer)

	httpServer := httptest.NewServer(router)
	defer httpServer.Close()

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	creds := globalServerConfig.GetCredential()
	client := newAuthRPCClient(authConfig{
		serverAddr:      httpServer.Listener.Addr().String(),
		serviceName:     "Arith",
		serviceEndpoint: "/foo",
		accessKey:       creds.AccessKey,
		secretKey:       creds.SecretKey,
	})
	defer client.Close()

	if err = client.Login(); err != nil {
		t.Fatal(err)
	}

	// Synchronous calls
	args := &ArithArgs{7, 8}
	reply := new(ArithReply)
	if err = client.rpcClient.Call("Arith.Add", args, reply); err != nil {
		t.Errorf("Add: expected no error but got string %v", err)
	}

	if reply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
	}

	resp, err := http.Get(httpServer.URL + "/foo")
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("Expected %d, got %d", http.StatusMethodNotAllowed, resp.StatusCode)
	}
}
