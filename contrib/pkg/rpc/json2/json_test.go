// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Modified to be used with MinIO, uses jsoniter and
// supports tracing JSON-rpc calls.

package json2

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/minio/minio/contrib/pkg/rpc"
)

// ResponseRecorder is an implementation of http.ResponseWriter that
// records its mutations for later inspection in tests.
type ResponseRecorder struct {
	Code      int           // the HTTP response code from WriteHeader
	HeaderMap http.Header   // the HTTP response headers
	Body      *bytes.Buffer // if non-nil, the bytes.Buffer to append written data to
	Flushed   bool
}

// NewRecorder returns an initialized ResponseRecorder.
func NewRecorder() *ResponseRecorder {
	return &ResponseRecorder{
		HeaderMap: make(http.Header),
		Body:      new(bytes.Buffer),
	}
}

// DefaultRemoteAddr is the default remote address to return in RemoteAddr if
// an explicit DefaultRemoteAddr isn't set on ResponseRecorder.
const DefaultRemoteAddr = "1.2.3.4"

// Header returns the response headers.
func (rw *ResponseRecorder) Header() http.Header {
	return rw.HeaderMap
}

// Write always succeeds and writes to rw.Body, if not nil.
func (rw *ResponseRecorder) Write(buf []byte) (int, error) {
	if rw.Body != nil {
		rw.Body.Write(buf)
	}
	if rw.Code == 0 {
		rw.Code = http.StatusOK
	}
	return len(buf), nil
}

// WriteHeader sets rw.Code.
func (rw *ResponseRecorder) WriteHeader(code int) {
	rw.Code = code
}

// Flush sets rw.Flushed to true.
func (rw *ResponseRecorder) Flush() {
	rw.Flushed = true
}

// ----------------------------------------------------------------------------

var ErrResponseError = errors.New("response error")
var ErrMappedResponseError = errors.New("mapped response error")

type Service1Request struct {
	A int
	B int
}

type Service1NoParamsRequest struct {
	V  string `json:"jsonrpc"`
	M  string `json:"method"`
	ID uint64 `json:"id"`
}

type Service1ParamsArrayRequest struct {
	V string `json:"jsonrpc"`
	P []struct {
		T string
	} `json:"params"`
	M  string `json:"method"`
	ID uint64 `json:"id"`
}

type Service1Response struct {
	Result int
}

type Service1 struct {
}

const Service1DefaultResponse = 9999

func (t *Service1) Multiply(r *http.Request, req *Service1Request, res *Service1Response) error {
	if req.A == 0 && req.B == 0 {
		// Sentinel value for test with no params.
		res.Result = Service1DefaultResponse
	} else {
		res.Result = req.A * req.B
	}
	return nil
}

func (t *Service1) ResponseError(r *http.Request, req *Service1Request, res *Service1Response) error {
	return ErrResponseError
}

func (t *Service1) MappedResponseError(r *http.Request, req *Service1Request, res *Service1Response) error {
	return ErrMappedResponseError
}

func execute(t *testing.T, s *rpc.Server, method string, req, res interface{}) error {
	if !s.HasMethod(method) {
		t.Fatal("Expected to be registered:", method)
	}

	buf, _ := EncodeClientRequest(method, req)
	body := bytes.NewBuffer(buf)
	r, _ := http.NewRequest("POST", "http://localhost:8080/", body)
	r.Header.Set("Content-Type", "application/json")

	w := NewRecorder()
	s.ServeHTTP(w, r)

	return DecodeClientResponse(w.Body, res)
}

func executeRaw(t *testing.T, s *rpc.Server, req interface{}, res interface{}) error {
	j, _ := json.Marshal(req)
	r, _ := http.NewRequest("POST", "http://localhost:8080/", bytes.NewBuffer(j))
	r.Header.Set("Content-Type", "application/json")

	w := NewRecorder()
	s.ServeHTTP(w, r)

	return DecodeClientResponse(w.Body, res)
}

func executeInvalidJSON(t *testing.T, s *rpc.Server, res interface{}) error {
	r, _ := http.NewRequest("POST", "http://localhost:8080/", strings.NewReader(`not even a json`))
	r.Header.Set("Content-Type", "application/json")

	w := NewRecorder()
	s.ServeHTTP(w, r)

	return DecodeClientResponse(w.Body, res)
}

func TestService(t *testing.T) {
	s := rpc.NewServer()
	s.RegisterCodec(NewCodec(), "application/json")
	s.RegisterService(new(Service1), "")

	var res Service1Response
	if err := execute(t, s, "Service1.Multiply", &Service1Request{4, 2}, &res); err != nil {
		t.Error("Expected err to be nil, but got:", err)
	}
	if res.Result != 8 {
		t.Errorf("Wrong response: %v.", res.Result)
	}

	if err := execute(t, s, "Service1.ResponseError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get %q, but got nil", ErrResponseError)
	} else if err.Error() != ErrResponseError.Error() {
		t.Errorf("Expected to get %q, but got %q", ErrResponseError, err)
	}

	// No parameters.
	res = Service1Response{}
	if err := executeRaw(t, s, &Service1NoParamsRequest{"2.0", "Service1.Multiply", 1}, &res); err != nil {
		t.Error(err)
	}
	if res.Result != Service1DefaultResponse {
		t.Errorf("Wrong response: got %v, want %v", res.Result, Service1DefaultResponse)
	}

	// Parameters as by-position.
	res = Service1Response{}
	req := Service1ParamsArrayRequest{
		V: "2.0",
		P: []struct {
			T string
		}{{
			T: "test",
		}},
		M:  "Service1.Multiply",
		ID: 1,
	}
	if err := executeRaw(t, s, &req, &res); err != nil {
		t.Error(err)
	}
	if res.Result != Service1DefaultResponse {
		t.Errorf("Wrong response: got %v, want %v", res.Result, Service1DefaultResponse)
	}

	res = Service1Response{}
	if err := executeInvalidJSON(t, s, &res); err == nil {
		t.Error("Expected to receive an E_PARSE error, but got nil")
	} else if jsonRpcErr, ok := err.(*Error); !ok {
		t.Errorf("Expected to receive an Error, but got %T: %s", err, err)
	} else if jsonRpcErr.Code != E_PARSE {
		t.Errorf("Expected to receive an E_PARSE JSON-RPC error (%d) but got %d", E_PARSE, jsonRpcErr.Code)
	}
}

func TestServiceWithErrorMapper(t *testing.T) {
	const mappedErrorCode = 100

	// errorMapper maps ErrMappedResponseError to an Error with mappedErrorCode Code, everything else is returned as-is
	errorMapper := func(err error) error {
		if err == ErrMappedResponseError {
			return &Error{
				Code:    mappedErrorCode,
				Message: err.Error(),
			}
		}
		// Map everything else to E_SERVER
		return &Error{
			Code:    E_SERVER,
			Message: err.Error(),
		}
	}

	s := rpc.NewServer()
	s.RegisterCodec(NewCustomCodecWithErrorMapper(rpc.DefaultEncoderSelector, errorMapper), "application/json")
	s.RegisterService(new(Service1), "")

	var res Service1Response
	if err := execute(t, s, "Service1.MappedResponseError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get a JSON-RPC error, but got nil")
	} else if jsonRpcErr, ok := err.(*Error); !ok {
		t.Errorf("Expected to get an *Error, but got %T: %s", err, err)
	} else if jsonRpcErr.Code != mappedErrorCode {
		t.Errorf("Expected to get Code %d, but got %d", mappedErrorCode, jsonRpcErr.Code)
	} else if jsonRpcErr.Message != ErrMappedResponseError.Error() {
		t.Errorf("Expected to get Message %q, but got %q", ErrMappedResponseError.Error(), jsonRpcErr.Message)
	}

	// Unmapped error behaves as usual
	if err := execute(t, s, "Service1.ResponseError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get a JSON-RPC error, but got nil")
	} else if jsonRpcErr, ok := err.(*Error); !ok {
		t.Errorf("Expected to get an *Error, but got %T: %s", err, err)
	} else if jsonRpcErr.Code != E_SERVER {
		t.Errorf("Expected to get Code %d, but got %d", E_SERVER, jsonRpcErr.Code)
	} else if jsonRpcErr.Message != ErrResponseError.Error() {
		t.Errorf("Expected to get Message %q, but got %q", ErrResponseError.Error(), jsonRpcErr.Message)
	}

	// Malformed request without method: our framework tries to return an error: we shouldn't map that one
	malformedRequest := struct {
		V  string `json:"jsonrpc"`
		ID string `json:"id"`
	}{
		V:  "3.0",
		ID: "any",
	}
	if err := executeRaw(t, s, &malformedRequest, &res); err == nil {
		t.Errorf("Expected to get a JSON-RPC error, but got nil")
	} else if jsonRpcErr, ok := err.(*Error); !ok {
		t.Errorf("Expected to get an *Error, but got %T: %s", err, err)
	} else if jsonRpcErr.Code != E_INVALID_REQ {
		t.Errorf("Expected to get an E_INVALID_REQ error (%d), but got %d", E_INVALID_REQ, jsonRpcErr.Code)
	}
}

func TestDecodeNullResult(t *testing.T) {
	data := `{"jsonrpc": "2.0", "id": 12345, "result": null}`
	reader := bytes.NewReader([]byte(data))
	var result interface{}

	err := DecodeClientResponse(reader, &result)

	if err != ErrNullResult {
		t.Error("Expected err no be ErrNullResult, but got:", err)
	}

	if result != nil {
		t.Error("Expected result to be nil, but got:", result)
	}
}
