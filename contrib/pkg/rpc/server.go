// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Modified to be used with MinIO, uses jsoniter and
// supports tracing JSON-rpc calls.

package rpc

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

var nilErrorValue = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())

// ----------------------------------------------------------------------------
// Codec
// ----------------------------------------------------------------------------

// Codec creates a CodecRequest to process each request.
type Codec interface {
	NewRequest(*http.Request) CodecRequest
}

// CodecRequest decodes a request and encodes a response using a specific
// serialization scheme.
type CodecRequest interface {
	// Reads the request and returns the RPC method name.
	Method() (string, error)
	// Reads the request filling the RPC method args.
	ReadRequest(interface{}) error
	// Writes the response using the RPC method reply.
	WriteResponse(http.ResponseWriter, interface{})
	// Writes an error produced by the server.
	WriteError(w http.ResponseWriter, status int, err error)
}

// ----------------------------------------------------------------------------
// Server
// ----------------------------------------------------------------------------

// NewServer returns a new RPC server.
func NewServer() *Server {
	return &Server{
		codecs:   make(map[string]Codec),
		services: new(serviceMap),
	}
}

// RequestInfo contains all the information we pass to before/after functions
type RequestInfo struct {
	Args           reflect.Value
	Method         string
	Error          error
	ResponseWriter http.ResponseWriter
	Request        *http.Request
	StatusCode     int
}

// Server serves registered RPC services using registered codecs.
type Server struct {
	codecs        map[string]Codec
	services      *serviceMap
	interceptFunc func(i *RequestInfo) *http.Request
	beforeFunc    func(i *RequestInfo)
	afterFunc     func(i *RequestInfo)
	validateFunc  reflect.Value
}

// RegisterCodec adds a new codec to the server.
//
// Codecs are defined to process a given serialization scheme, e.g., JSON or
// XML. A codec is chosen based on the "Content-Type" header from the request,
// excluding the charset definition.
func (s *Server) RegisterCodec(codec Codec, contentType string) {
	s.codecs[strings.ToLower(contentType)] = codec
}

// RegisterInterceptFunc registers the specified function as the function
// that will be called before every request. The function is allowed to intercept
// the request e.g. add values to the context.
//
// Note: Only one function can be registered, subsequent calls to this
// method will overwrite all the previous functions.
func (s *Server) RegisterInterceptFunc(f func(i *RequestInfo) *http.Request) {
	s.interceptFunc = f
}

// RegisterBeforeFunc registers the specified function as the function
// that will be called before every request.
//
// Note: Only one function can be registered, subsequent calls to this
// method will overwrite all the previous functions.
func (s *Server) RegisterBeforeFunc(f func(i *RequestInfo)) {
	s.beforeFunc = f
}

// RegisterValidateRequestFunc registers the specified function as the function
// that will be called after the BeforeFunc (if registered) and before invoking
// the actual Service method. If this function returns a non-nil error, the method
// won't be invoked and this error will be considered as the method result.
// The first argument is information about the request, useful for accessing to http.Request.Context()
// The second argument of this function is the already-unmarshalled *args parameter of the method.
func (s *Server) RegisterValidateRequestFunc(f func(r *RequestInfo, i interface{}) error) {
	s.validateFunc = reflect.ValueOf(f)
}

// RegisterAfterFunc registers the specified function as the function
// that will be called after every request
//
// Note: Only one function can be registered, subsequent calls to this
// method will overwrite all the previous functions.
func (s *Server) RegisterAfterFunc(f func(i *RequestInfo)) {
	s.afterFunc = f
}

// RegisterService adds a new service to the server.
//
// The name parameter is optional: if empty it will be inferred from
// the receiver type name.
//
// Methods from the receiver will be extracted if these rules are satisfied:
//
//    - The receiver is exported (begins with an upper case letter) or local
//      (defined in the package registering the service).
//    - The method name is exported.
//    - The method has three arguments: *http.Request, *args, *reply.
//    - All three arguments are pointers.
//    - The second and third arguments are exported or local.
//    - The method has return type error.
//
// All other methods are ignored.
func (s *Server) RegisterService(receiver interface{}, name string) error {
	return s.services.register(receiver, name)
}

// HasMethod returns true if the given method is registered.
//
// The method uses a dotted notation as in "Service.Method".
func (s *Server) HasMethod(method string) bool {
	if _, _, err := s.services.get(method); err == nil {
		return true
	}
	return false
}

// ServeHTTP
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		err := fmt.Errorf("rpc: POST method required, received %s", r.Method)
		WriteError(w, http.StatusMethodNotAllowed, err.Error())
		// Call the registered After Function
		if s.afterFunc != nil {
			s.afterFunc(&RequestInfo{
				ResponseWriter: w,
				Request:        r,
				Method:         "Unknown." + r.Method,
				StatusCode:     http.StatusMethodNotAllowed,
			})
		}
		return
	}
	contentType := r.Header.Get("Content-Type")
	idx := strings.Index(contentType, ";")
	if idx != -1 {
		contentType = contentType[:idx]
	}
	var codec Codec
	if contentType == "" && len(s.codecs) == 1 {
		// If Content-Type is not set and only one codec has been registered,
		// then default to that codec.
		for _, c := range s.codecs {
			codec = c
		}
	} else if codec = s.codecs[strings.ToLower(contentType)]; codec == nil {
		err := fmt.Errorf("rpc: unrecognized Content-Type: %s", contentType)
		WriteError(w, http.StatusUnsupportedMediaType, err.Error())
		// Call the registered After Function
		if s.afterFunc != nil {
			s.afterFunc(&RequestInfo{
				ResponseWriter: w,
				Request:        r,
				Method:         "Unknown." + r.Method,
				Error:          err,
				StatusCode:     http.StatusUnsupportedMediaType,
			})
		}
		return
	}
	// Create a new codec request.
	codecReq := codec.NewRequest(r)
	// Get service method to be called.
	method, errMethod := codecReq.Method()
	if errMethod != nil {
		codecReq.WriteError(w, http.StatusBadRequest, errMethod)
		if s.afterFunc != nil {
			s.afterFunc(&RequestInfo{
				ResponseWriter: w,
				Request:        r,
				Method:         "Unknown." + r.Method,
				Error:          errMethod,
				StatusCode:     http.StatusBadRequest,
			})
		}
		return
	}
	serviceSpec, methodSpec, errGet := s.services.get(method)
	if errGet != nil {
		codecReq.WriteError(w, http.StatusBadRequest, errGet)
		if s.afterFunc != nil {
			s.afterFunc(&RequestInfo{
				ResponseWriter: w,
				Request:        r,
				Method:         method,
				Error:          errGet,
				StatusCode:     http.StatusBadRequest,
			})
		}
		return
	}
	// Decode the args.
	args := reflect.New(methodSpec.argsType)
	if errRead := codecReq.ReadRequest(args.Interface()); errRead != nil {
		codecReq.WriteError(w, http.StatusBadRequest, errRead)
		if s.afterFunc != nil {
			s.afterFunc(&RequestInfo{
				ResponseWriter: w,
				Request:        r,
				Method:         method,
				Error:          errRead,
				StatusCode:     http.StatusBadRequest,
			})
		}
		return
	}

	// Call the registered Intercept Function
	if s.interceptFunc != nil {
		req := s.interceptFunc(&RequestInfo{
			Request: r,
			Method:  method,
		})
		if req != nil {
			r = req
		}
	}

	requestInfo := &RequestInfo{
		Request: r,
		Method:  method,
	}

	// Call the registered Before Function
	if s.beforeFunc != nil {
		s.beforeFunc(requestInfo)
	}

	// Prepare the reply, we need it even if validation fails
	reply := reflect.New(methodSpec.replyType)
	errValue := []reflect.Value{nilErrorValue}

	// Call the registered Validator Function
	if s.validateFunc.IsValid() {
		errValue = s.validateFunc.Call([]reflect.Value{reflect.ValueOf(requestInfo), args})
	}

	// If still no errors after validation, call the method
	if errValue[0].IsNil() {
		errValue = methodSpec.method.Func.Call([]reflect.Value{
			serviceSpec.rcvr,
			reflect.ValueOf(r),
			args,
			reply,
		})
	}

	// Extract the result to error if needed.
	var errResult error
	statusCode := http.StatusOK
	errInter := errValue[0].Interface()
	if errInter != nil {
		statusCode = http.StatusBadRequest
		errResult = errInter.(error)
	}

	// Prevents Internet Explorer from MIME-sniffing a response away
	// from the declared content-type
	w.Header().Set("x-content-type-options", "nosniff")

	// Encode the response.
	if errResult == nil {
		codecReq.WriteResponse(w, reply.Interface())
	} else {
		codecReq.WriteError(w, statusCode, errResult)
	}

	// Call the registered After Function
	if s.afterFunc != nil {
		s.afterFunc(&RequestInfo{
			Args:           args,
			ResponseWriter: w,
			Request:        r,
			Method:         method,
			Error:          errResult,
			StatusCode:     statusCode,
		})
	}
}

func WriteError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprint(w, msg)
}
