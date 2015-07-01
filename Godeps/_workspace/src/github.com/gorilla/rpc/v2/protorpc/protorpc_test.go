// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package protorpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/rpc/v2"
)

var ErrResponseError = errors.New("response error")

type Service1Request struct {
	A int
	B int
}

type Service1BadRequest struct {
}

type Service1Response struct {
	Result       int
	ErrorMessage string `json:"error_message"`
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

func execute(t *testing.T, s *rpc.Server, method string, req, res interface{}) (int, error) {
	if !s.HasMethod(method) {
		t.Fatal("Expected to be registered:", method)
	}

	buf, _ := json.Marshal(req)
	body := bytes.NewBuffer(buf)
	r, _ := http.NewRequest("POST", "http://localhost:8080/"+method, body)
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	err := json.NewDecoder(w.Body).Decode(res)
	return w.Code, err
}

func TestService(t *testing.T) {
	s := rpc.NewServer()
	s.RegisterCodec(NewCodec(), "application/json")
	s.RegisterService(new(Service1), "")

	var res Service1Response
	if _, err := execute(t, s, "Service1.Multiply", &Service1Request{4, 2}, &res); err != nil {
		t.Error("Expected err to be nil, but got:", err)
	}
	if res.Result != 8 {
		t.Error("Expected res.Result to be 8, but got:", res.Result)
	}
	if res.ErrorMessage != "" {
		t.Error("Expected error_message to be empty, but got:", res.ErrorMessage)
	}
	if code, err := execute(t, s, "Service1.ResponseError", &Service1Request{4, 2}, &res); err != nil || code != 400 {
		t.Errorf("Expected code to be 400 and error to be nil, but got %v (%v)", code, err)
	}
	if res.ErrorMessage == "" {
		t.Errorf("Expected error_message to be %q, but got %q", ErrResponseError, res.ErrorMessage)
	}
	if code, _ := execute(t, s, "Service1.Multiply", nil, &res); code != 400 {
		t.Error("Expected http response code 400, but got", code)
	}
}
