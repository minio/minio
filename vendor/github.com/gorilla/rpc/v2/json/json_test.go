// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package json

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gorilla/rpc/v2"
)

var (
	ErrResponseError     = errors.New("response error")
	ErrResponseJsonError = &Error{Data: map[string]interface{}{
		"stackstrace": map[string]interface{}{"0": "foo()"},
		"error":       "a message",
	}}
)

type Service1Request struct {
	A int
	B int
}

type Service1Response struct {
	Result int
}

type Service1 struct {
}

func (t *Service1) Multiply(r *http.Request, req *Service1Request, res *Service1Response) error {
	res.Result = req.A * req.B
	return nil
}

func (t *Service1) ResponseError(r *http.Request, req *Service1Request, res *Service1Response) error {
	return ErrResponseError
}

func (t *Service1) ResponseJsonError(r *http.Request, req *Service1Request, res *Service1Response) error {
	return ErrResponseJsonError
}

func execute(t *testing.T, s *rpc.Server, method string, req, res interface{}) error {
	if !s.HasMethod(method) {
		t.Fatal("Expected to be registered:", method)
	}

	buf, _ := EncodeClientRequest(method, req)
	body := bytes.NewBuffer(buf)
	r, _ := http.NewRequest("POST", "http://localhost:8080/", body)
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	return DecodeClientResponse(w.Body, res)
}

func executeRaw(t *testing.T, s *rpc.Server, req json.RawMessage) (int, *bytes.Buffer) {
	r, _ := http.NewRequest("POST", "http://localhost:8080/", bytes.NewBuffer(req))
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	return w.Code, w.Body
}

func field(name string, blob json.RawMessage) (v interface{}, ok bool) {
	var obj map[string]interface{}
	if err := json.Unmarshal(blob, &obj); err != nil {
		return nil, false
	}
	v, ok = obj[name]
	return
}

func TestService(t *testing.T) {
	s := rpc.NewServer()
	s.RegisterCodec(NewCodec(), "application/json")
	s.RegisterService(new(Service1), "")

	var res Service1Response
	if err := execute(t, s, "Service1.Multiply", &Service1Request{4, 2}, &res); err != nil {
		t.Error("Expected err to be nil, but got", err)
	}
	if res.Result != 8 {
		t.Error("Expected res.Result to be 8, but got", res.Result)
	}
	if err := execute(t, s, "Service1.ResponseError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get %q, but got nil", ErrResponseError)
	} else if err.Error() != ErrResponseError.Error() {
		t.Errorf("Expected to get %q, but got %q", ErrResponseError, err)
	}
	if code, res := executeRaw(t, s, json.RawMessage(`{"method":"Service1.Multiply","params":null,"id":5}`)); code != 400 {
		t.Error("Expected response code to be 400, but got", code)
	} else if v, ok := field("result", res.Bytes()); !ok || v != nil {
		t.Errorf("Expected ok to be true and v to be nil, but got %v and %v", ok, v)
	}
	if err := execute(t, s, "Service1.ResponseJsonError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get %q, but got nil", ErrResponseError)
	} else if jsonErr, ok := err.(*Error); !ok {
		t.Error("Expected err to be of a *json.Error type")
	} else if !reflect.DeepEqual(jsonErr.Data, ErrResponseJsonError.Data) {
		t.Errorf("Expected jsonErr to be %q, but got %q", ErrResponseJsonError, jsonErr)
	}
}

func TestClientNullResult(t *testing.T) {
	data := `{"jsonrpc": "2.0", "id": 8674665223082153551, "result": null}`
	reader := bytes.NewReader([]byte(data))

	var reply interface{}

	err := DecodeClientResponse(reader, &reply)
	if err == nil {
		t.Fatal(err)
	}
	if err.Error() != "Unexpected null result" {
		t.Fatalf("Unexpected error: %s", err)
	}
}
