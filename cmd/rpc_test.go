/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	xrpc "github.com/minio/minio/cmd/rpc"
	xnet "github.com/minio/minio/pkg/net"
)

func TestAuthArgsAuthenticate(t *testing.T) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	case1Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow(),
	}

	case2Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow().Add(15 * time.Minute),
	}

	case3Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow().Add(-16 * time.Minute),
	}

	case4Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  RPCVersion{99, 99, 99},
		RequestTime: UTCNow(),
	}

	case5Args := AuthArgs{
		Token:       "invalid-token",
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow(),
	}

	testCases := []struct {
		args      AuthArgs
		expectErr bool
	}{
		{case1Args, false},
		{case2Args, false},
		{case3Args, true},
		{case4Args, true},
		{case5Args, true},
	}

	for i, testCase := range testCases {
		err := testCase.args.Authenticate()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestAuthArgsSetAuthArgs(t *testing.T) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	case1Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow(),
	}

	case2Args := AuthArgs{
		Token:       newAuthToken(),
		RPCVersion:  globalRPCAPIVersion,
		RequestTime: UTCNow().Add(15 * time.Minute),
	}

	testCases := []struct {
		args           *AuthArgs
		authArgs       AuthArgs
		expectedResult *AuthArgs
	}{
		{&AuthArgs{}, case1Args, &case1Args},
		{&case2Args, case1Args, &case1Args},
	}

	for i, testCase := range testCases {
		testCase.args.SetAuthArgs(testCase.authArgs)
		result := testCase.args

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRPCClientArgsValidate(t *testing.T) {
	case1URL, err := xnet.ParseURL("http://localhost:12345/rpc")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case1Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case1URL,
		TLSConfig:        nil,
	}

	case2URL, err := xnet.ParseURL("https://localhost:12345/rpc")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case2Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case1URL,
		TLSConfig:        &tls.Config{},
	}

	case3Args := RPCClientArgs{
		NewAuthTokenFunc: nil,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case1URL,
		TLSConfig:        &tls.Config{},
	}

	case4Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceURL:       case1URL,
		TLSConfig:        &tls.Config{},
	}

	case5URL, err := xnet.ParseURL("ftp://localhost:12345/rpc")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case5Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case5URL,
		TLSConfig:        &tls.Config{},
	}

	case6URL, err := xnet.ParseURL("http://localhost:12345/rpc?location")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case6Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case6URL,
		TLSConfig:        &tls.Config{},
	}

	case7Args := RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       case2URL,
		TLSConfig:        nil,
	}

	testCases := []struct {
		args      RPCClientArgs
		expectErr bool
	}{
		{case1Args, false},
		{case2Args, false},
		// NewAuthTokenFunc must not be empty error.
		{case3Args, true},
		// ServiceName must not be empty.
		{case4Args, true},
		// unknown RPC URL error.
		{case5Args, true},
		// unknown RPC URL error.
		{case6Args, true},
		// tls configuration must not be empty for https url error.
		{case7Args, true},
	}

	for i, testCase := range testCases {
		err := testCase.args.validate()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

type Args struct {
	AuthArgs
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith struct{}

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func TestRPCClientCall(t *testing.T) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	url, err := xnet.ParseURL(httpServer.URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	rpcClient, err := NewRPCClient(RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       url,
	})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	var case1Result int
	case1ExpectedResult := 19 * 8

	testCases := []struct {
		serviceMethod string
		args          interface {
			SetAuthArgs(args AuthArgs)
		}
		result         interface{}
		changeConfig   bool
		expectedResult interface{}
		expectErr      bool
	}{
		{"Arith.Multiply", &Args{A: 19, B: 8}, &case1Result, false, &case1ExpectedResult, false},
		{"Arith.Divide", &Args{A: 19, B: 8}, &Quotient{}, false, &Quotient{2, 3}, false},
		{"Arith.Multiply", &Args{A: 19, B: 8}, &case1Result, true, &case1ExpectedResult, false},
		{"Arith.Divide", &Args{A: 19, B: 8}, &Quotient{}, true, &Quotient{2, 3}, false},
		{"Arith.Divide", &Args{A: 19, B: 0}, &Quotient{}, false, nil, true},
		{"Arith.Divide", &Args{A: 19, B: 8}, &case1Result, false, nil, true},
	}

	for i, testCase := range testCases {
		if testCase.changeConfig {
			globalServerConfig = newServerConfig()
		}

		err := rpcClient.Call(testCase.serviceMethod, testCase.args, testCase.result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(testCase.result, testCase.expectedResult) {
				t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, testCase.result)
			}
		}
	}
}

func TestRPCClientClose(t *testing.T) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	url, err := xnet.ParseURL("http://localhost:12345")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	rpcClient, err := NewRPCClient(RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       url,
	})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		rpcClient *RPCClient
		expectErr bool
	}{
		{rpcClient, false},
		// Double close.
		{rpcClient, false},
	}

	for i, testCase := range testCases {
		err := testCase.rpcClient.Close()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestRPCClientServiceURL(t *testing.T) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()
	globalServerConfig = newServerConfig()

	url, err := xnet.ParseURL("http://localhost:12345")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	rpcClient, err := NewRPCClient(RPCClientArgs{
		NewAuthTokenFunc: newAuthToken,
		RPCVersion:       globalRPCAPIVersion,
		ServiceName:      "Arith",
		ServiceURL:       url,
	})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case1Result, err := xnet.ParseURL("http://localhost:12345")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	testCases := []struct {
		rpcClient      *RPCClient
		expectedResult *xnet.URL
	}{
		{rpcClient, case1Result},
	}

	for i, testCase := range testCases {
		result := testCase.rpcClient.ServiceURL()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
