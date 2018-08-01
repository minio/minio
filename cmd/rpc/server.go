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
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Authenticator - validator of first argument of any RPC call.
type Authenticator interface {
	// Method to validate first argument of any RPC call.
	Authenticate() error
}

// reflect.Type of error interface.
var errorType = reflect.TypeOf((*error)(nil)).Elem()

// reflect.Type of io.Reader interface.
var readerType = reflect.TypeOf((*io.Reader)(nil)).Elem()

// reflect.Type of io.ReadCloser interface.
var readCloserType = reflect.TypeOf((*io.ReadCloser)(nil)).Elem()

// reflect.Type of Authenticator interface.
var authenticatorType = reflect.TypeOf((*Authenticator)(nil)).Elem()

// Returns whether given type is exported or builin type or not.
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	rune, _ := utf8.DecodeRuneInString(t.Name())
	return unicode.IsUpper(rune) || t.PkgPath() == ""
}

// Returns whether given method is like
// (func *T) Method(Args, *Reply) error
func isRPCMethod(method reflect.Method) bool {
	// Methods must have three arguments (receiver, args, reply)
	if method.Type.NumIn() != 3 {
		return false
	}

	// First argument must be exported.
	if !isExportedOrBuiltinType(method.Type.In(1)) {
		return false
	}

	// First argument must be Authenticator.
	if !method.Type.In(1).Implements(authenticatorType) {
		return false
	}

	// Second argument must be exported or builtin type.
	if !isExportedOrBuiltinType(method.Type.In(2)) {
		return false
	}

	// Second argument must be a pointer.
	if method.Type.In(2).Kind() != reflect.Ptr {
		return false
	}

	// Method must return one value.
	if method.Type.NumOut() != 1 {
		return false
	}

	// The return type of the method must be error.
	if method.Type.Out(0) != errorType {
		return false
	}

	return true
}

// Returns whether given method is like
// (func *T) Method(Args, io.Reader, *Reply) (io.ReadCloser, error)
func isRPCMethodStream(method reflect.Method) bool {
	// Methods must have four arguments (receiver, args, reader, reply)
	if method.Type.NumIn() != 4 {
		return false
	}

	// First argument must be exported.
	if !isExportedOrBuiltinType(method.Type.In(1)) {
		return false
	}

	// First argument must be Authenticator.
	if !method.Type.In(1).Implements(authenticatorType) {
		return false
	}

	// Second argument must be io.Reader type.
	if !method.Type.In(2).Implements(readerType) {
		return false
	}

	// Third argument must be exported or builtin type.
	if !isExportedOrBuiltinType(method.Type.In(3)) {
		return false
	}

	// Third argument must be a pointer.
	if method.Type.In(3).Kind() != reflect.Ptr {
		return false
	}

	// Method must return two values (io.ReadCloser, error).
	if method.Type.NumOut() != 2 {
		return false
	}

	// First return type must be io.ReadCloser.
	if method.Type.Out(0) != readCloserType {
		return false
	}

	// Second return type must be error.
	if method.Type.Out(1) != errorType {
		return false
	}

	return true
}

// Makes method name map from given type.
func getMethodMap(receiverType reflect.Type) map[string]reflect.Method {
	methodMap := make(map[string]reflect.Method)
	for i := 0; i < receiverType.NumMethod(); i++ {
		// Method.PkgPath is empty for this package.
		method := receiverType.Method(i)

		if isRPCMethod(method) || isRPCMethodStream(method) {
			methodMap[method.Name] = method
		}
	}

	return methodMap
}

// Server - HTTP based RPC server.
type Server struct {
	serviceName   string
	receiverValue reflect.Value
	methodMap     map[string]reflect.Method
}

// RegisterName - registers receiver with given name to handle RPC requests.
func (server *Server) RegisterName(name string, receiver interface{}) error {
	server.serviceName = name

	server.receiverValue = reflect.ValueOf(receiver)
	if !reflect.Indirect(server.receiverValue).IsValid() {
		return fmt.Errorf("nil receiver")
	}

	receiverName := reflect.Indirect(server.receiverValue).Type().Name()
	receiverType := reflect.TypeOf(receiver)
	server.methodMap = getMethodMap(receiverType)
	if len(server.methodMap) == 0 {
		str := "rpc.Register: type " + receiverName + " has no exported methods of suitable type"

		// To help the user, see if a pointer receiver would work.
		if len(getMethodMap(reflect.PtrTo(receiverType))) != 0 {
			str += " (hint: pass a pointer to value of that type)"
		}

		return errors.New(str)
	}

	return nil
}

// call - call service method in receiver.
func (server *Server) call(serviceMethod string, argBytes []byte, reader io.Reader, replyWriter io.Writer) (readCloser io.ReadCloser, err error) {
	tokens := strings.SplitN(serviceMethod, ".", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("invalid service/method request ill-formed %v", serviceMethod)
	}

	serviceName := tokens[0]
	if serviceName != server.serviceName {
		return nil, fmt.Errorf("can't find service %v", serviceName)
	}

	methodName := tokens[1]
	method, found := server.methodMap[methodName]
	if !found {
		return nil, fmt.Errorf("can't find method %v", methodName)
	}

	var argv reflect.Value

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if method.Type.In(1).Kind() == reflect.Ptr {
		argv = reflect.New(method.Type.In(1).Elem())
	} else {
		argv = reflect.New(method.Type.In(1))
		argIsValue = true
	}

	if err = gob.NewDecoder(bytes.NewReader(argBytes)).Decode(argv.Interface()); err != nil {
		return nil, err
	}

	if argIsValue {
		argv = argv.Elem()
	}

	// call Authenticate() method.
	authMethod, ok := method.Type.In(1).MethodByName("Authenticate")
	if !ok {
		panic("Authenticate() method not found. This should not happen.")
	}
	returnValues := authMethod.Func.Call([]reflect.Value{argv})
	errInter := returnValues[0].Interface()
	if errInter != nil {
		err = errInter.(error)
	}
	if err != nil {
		return nil, err
	}

	// Reply is second argument for non-stream methods and third argument for stream methods.
	replyIndex := 2
	if method.Type.NumIn() != 3 {
		replyIndex = 3
	}

	replyv := reflect.New(method.Type.In(replyIndex).Elem())

	switch method.Type.In(replyIndex).Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(method.Type.In(replyIndex).Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(method.Type.In(replyIndex).Elem(), 0, 0))
	}

	var readCloserInter interface{}
	if method.Type.NumIn() == 3 {
		// Call non-stream methods.
		returnValues = method.Func.Call([]reflect.Value{server.receiverValue, argv, replyv})
		errInter = returnValues[0].Interface()
	} else {
		// Call stream methods.
		readerv := reflect.ValueOf(reader)
		returnValues = method.Func.Call([]reflect.Value{server.receiverValue, argv, readerv, replyv})
		readCloserInter = returnValues[0].Interface()
		errInter = returnValues[1].Interface()
	}

	if errInter != nil {
		err = errInter.(error)
	}
	if err != nil {
		return nil, err
	}

	if err = gob.NewEncoder(replyWriter).Encode(replyv.Interface()); err != nil {
		return nil, err
	}

	if readCloserInter != nil {
		readCloser = readCloserInter.(io.ReadCloser)
	}

	return readCloser, nil
}

// CallRequest - RPC call request parameters.
type CallRequest struct {
	Method   string
	ArgBytes []byte
}

// CallResponse - RPC call response parameters.
type CallResponse struct {
	Error      string
	ReplyBytes []byte
}

// ServeHTTP - handles RPC on HTTP request.
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var callRequest CallRequest
	if err := gob.NewDecoder(newGobReader(req.Body)).Decode(&callRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var replyBody io.ReadCloser
	defer func() {
		if replyBody != nil {
			replyBody.Close()
		}
	}()

	var callResponse CallResponse
	var err error
	replyBuf := bufPool.Get()
	defer bufPool.Put(replyBuf)
	replyBody, err = server.call(callRequest.Method, callRequest.ArgBytes, req.Body, replyBuf)
	if err != nil {
		callResponse.Error = err.Error()
	}
	callResponse.ReplyBytes = replyBuf.Bytes()

	responseBuf := bufPool.Get()
	if err = gob.NewEncoder(responseBuf).Encode(callResponse); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		bufPool.Put(responseBuf)
		return
	}

	_, err = io.Copy(w, responseBuf)
	// FIXME: add error log here.
	bufPool.Put(responseBuf)

	if replyBody != nil {
		_, err = io.Copy(w, replyBody)
		// FIXME: add error log here.
		replyBody.Close()
	}
}

// NewServer - returns new RPC server.
func NewServer() *Server {
	return &Server{}
}
