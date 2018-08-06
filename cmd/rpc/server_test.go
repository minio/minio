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

package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func gobEncode(e interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(e)
	return buf.Bytes(), err
}

type Args struct {
	A, B int
}

func (a *Args) Authenticate() (err error) {
	if a.A == 0 && a.B == 0 {
		err = errors.New("authenticated failed")
	}

	return
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

type mytype int

type Auth struct{}

func (a Auth) Authenticate() error {
	return nil
}

// exported method.
func (t mytype) Foo(a *Auth, b *int) error {
	return nil
}

// incompatible method because of unexported method.
func (t mytype) foo(a *Auth, b *int) error {
	return nil
}

// incompatible method because of first argument is not Authenticator.
func (t *mytype) Bar(a, b *int) error {
	return nil
}

// incompatible method because of error is not returned.
func (t mytype) IncompatFoo(a, b *int) {
}

// incompatible method because of second argument is not a pointer.
func (t *mytype) IncompatBar(a *int, b int) error {
	return nil
}

func TestIsExportedOrBuiltinType(t *testing.T) {
	var i int
	case1Type := reflect.TypeOf(i)

	var iptr *int
	case2Type := reflect.TypeOf(iptr)

	var a Arith
	case3Type := reflect.TypeOf(a)

	var aptr *Arith
	case4Type := reflect.TypeOf(aptr)

	var m mytype
	case5Type := reflect.TypeOf(m)

	var mptr *mytype
	case6Type := reflect.TypeOf(mptr)

	testCases := []struct {
		t              reflect.Type
		expectedResult bool
	}{
		{case1Type, true},
		{case2Type, true},
		{case3Type, true},
		{case4Type, true},
		// Type.Name() starts with lower case and Type.PkgPath() is not empty.
		{case5Type, false},
		// Type.Name() starts with lower case and Type.PkgPath() is not empty.
		{case6Type, false},
	}

	for i, testCase := range testCases {
		result := isExportedOrBuiltinType(testCase.t)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestGetMethodMap(t *testing.T) {
	var a Arith
	case1Type := reflect.TypeOf(a)

	var aptr *Arith
	case2Type := reflect.TypeOf(aptr)

	var m mytype
	case3Type := reflect.TypeOf(m)

	var mptr *mytype
	case4Type := reflect.TypeOf(mptr)

	testCases := []struct {
		t              reflect.Type
		expectedResult int
	}{
		// No methods exported.
		{case1Type, 0},
		// Multiply and Divide methods are exported.
		{case2Type, 2},
		// Foo method is exported.
		{case3Type, 1},
		// Foo method is exported.
		{case4Type, 1},
	}

	for i, testCase := range testCases {
		m := getMethodMap(testCase.t)
		result := len(m)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestServerRegisterName(t *testing.T) {
	case1Receiver := &Arith{}
	var case2Receiver mytype
	var case3Receiver *Arith
	i := 0
	var case4Receiver = &i
	var case5Receiver Arith

	testCases := []struct {
		name      string
		receiver  interface{}
		expectErr bool
	}{
		{"Arith", case1Receiver, false},
		{"arith", case1Receiver, false},
		{"Arith", case2Receiver, false},
		// nil receiver error.
		{"Arith", nil, true},
		// nil receiver error.
		{"Arith", case3Receiver, true},
		// rpc.Register: type Arith has no exported methods of suitable type error.
		{"Arith", case4Receiver, true},
		// rpc.Register: type Arith has no exported methods of suitable type (hint: pass a pointer to value of that type) error.
		{"Arith", case5Receiver, true},
	}

	for i, testCase := range testCases {
		err := NewServer().RegisterName(testCase.name, testCase.receiver)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestServerCall(t *testing.T) {
	server1 := NewServer()
	if err := server1.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	server2 := NewServer()
	if err := server2.RegisterName("arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case1ArgBytes, err := gobEncode(&Args{7, 8})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	reply := 7 * 8
	case1ExpectedResult, err := gobEncode(&reply)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case2ArgBytes, err := gobEncode(&Args{})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		server         *Server
		serviceMethod  string
		argBytes       []byte
		expectedResult []byte
		expectErr      bool
	}{
		{server1, "Arith.Multiply", case1ArgBytes, case1ExpectedResult, false},
		{server2, "arith.Multiply", case1ArgBytes, case1ExpectedResult, false},
		// invalid service/method request ill-formed error.
		{server1, "Multiply", nil, nil, true},
		// can't find service error.
		{server1, "arith.Multiply", nil, nil, true},
		// can't find method error.
		{server1, "Arith.Add", nil, nil, true},
		// gob decode error.
		{server1, "Arith.Multiply", []byte{10}, nil, true},
		// authentication error.
		{server1, "Arith.Multiply", case2ArgBytes, nil, true},
	}

	for i, testCase := range testCases {
		buf := bufPool.Get()
		defer bufPool.Put(buf)

		err := testCase.server.call(testCase.serviceMethod, testCase.argBytes, buf)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(buf.Bytes(), testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, buf.Bytes())
			}
		}
	}
}

func TestServerServeHTTP(t *testing.T) {
	server1 := NewServer()
	if err := server1.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	argBytes, err := gobEncode(&Args{7, 8})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	requestBodyData, err := gobEncode(CallRequest{Method: "Arith.Multiply", ArgBytes: argBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case1Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader(requestBodyData))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	reply := 7 * 8
	replyBytes, err := gobEncode(&reply)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case1Result, err := gobEncode(CallResponse{ReplyBytes: replyBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case2Request, err := http.NewRequest("GET", "http://localhost:12345/", bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case3Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader([]byte{10, 20}))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	requestBodyData, err = gobEncode(CallRequest{Method: "Arith.Add", ArgBytes: argBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case4Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader(requestBodyData))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case4Result, err := gobEncode(CallResponse{Error: "can't find method Add"})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		server         *Server
		httpRequest    *http.Request
		expectedCode   int
		expectedResult []byte
	}{
		{server1, case1Request, http.StatusOK, case1Result},
		{server1, case2Request, http.StatusMethodNotAllowed, nil},
		{server1, case3Request, http.StatusBadRequest, nil},
		{server1, case4Request, http.StatusOK, case4Result},
	}

	for i, testCase := range testCases {
		writer := httptest.NewRecorder()
		testCase.server.ServeHTTP(writer, testCase.httpRequest)
		if writer.Code != testCase.expectedCode {
			t.Fatalf("case %v: code: expected: %v, got: %v\n", i+1, testCase.expectedCode, writer.Code)
		}

		if testCase.expectedCode == http.StatusOK {
			result := writer.Body.Bytes()
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
