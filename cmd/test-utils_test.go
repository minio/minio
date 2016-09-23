/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	router "github.com/gorilla/mux"
)

// Tests should initNSLock only once.
func init() {
	// Initialize name space lock.
	isDist := false
	initNSLock(isDist)
}

func prepareFS() (ObjectLayer, string, error) {
	fsDirs, err := getRandomDisks(1)
	if err != nil {
		return nil, "", err
	}
	obj, err := getSingleNodeObjectLayer(fsDirs[0])
	if err != nil {
		removeRoots(fsDirs)
		return nil, "", err
	}
	return obj, fsDirs[0], nil
}

func prepareXL() (ObjectLayer, []string, error) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		return nil, nil, err
	}
	obj, err := getXLObjectLayer(fsDirs, nil)
	if err != nil {
		removeRoots(fsDirs)
		return nil, nil, err
	}
	return obj, fsDirs, nil
}

// TestErrHandler - Golang Testing.T and Testing.B, and gocheck.C satisfy this interface.
// This makes it easy to run the TestServer from any of the tests.
// Using this interface, functionalities to be used in tests can be made generalized, and can be integrated in benchmarks/unit tests/go check suite tests.
type TestErrHandler interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

const (
	// singleNodeTestStr is the string which is used as notation for Single node ObjectLayer in the unit tests.
	singleNodeTestStr string = "FS"
	// xLTestStr is the string which is used as notation for XL ObjectLayer in the unit tests.
	xLTestStr string = "XL"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// Random number state.
// We generate random temporary file names so that there's a good
// chance the file doesn't exist yet.
var randN uint32
var randmu sync.Mutex

// reseed - returns a new seed every time the function is called.
func reseed() uint32 {
	return uint32(time.Now().UnixNano() + int64(os.Getpid()))
}

// nextSuffix - provides a new unique suffix every time the function is called.
func nextSuffix() string {
	randmu.Lock()
	r := randN
	// Initial seed required, generate one.
	if r == 0 {
		r = reseed()
	}
	// constants from Numerical Recipes
	r = r*1664525 + 1013904223
	randN = r
	randmu.Unlock()
	return strconv.Itoa(int(1e9 + r%1e9))[1:]
}

// isSameType - compares two object types via reflect.TypeOf
func isSameType(obj1, obj2 interface{}) bool {
	return reflect.TypeOf(obj1) == reflect.TypeOf(obj2)
}

// TestServer encapsulates an instantiation of a Minio instance with a temporary backend.
// Example usage:
//   s := StartTestServer(t,"XL")
//   defer s.Stop()
type TestServer struct {
	Root      string
	Disks     []string
	AccessKey string
	SecretKey string
	Server    *httptest.Server
	Obj       ObjectLayer
}

// Starts the test server and returns the TestServer instance.
func StartTestServer(t TestErrHandler, instanceType string) TestServer {
	// create an instance of TestServer.
	testServer := TestServer{}
	// create temporary backend for the test server.
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}

	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Test Server needs to start before formatting of disks.
	// Get credential.
	credentials := serverConfig.GetCredential()

	testServer.Root = root
	testServer.Disks = disks
	testServer.AccessKey = credentials.AccessKeyID
	testServer.SecretKey = credentials.SecretAccessKey
	// Run TestServer.
	testServer.Server = httptest.NewServer(configureServerHandler(serverCmdConfig{disks: disks}))

	objLayer, err := makeTestBackend(disks, instanceType)
	if err != nil {
		t.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	testServer.Obj = objLayer
	objLayerMutex.Lock()
	globalObjectAPI = objLayer
	objLayerMutex.Unlock()
	return testServer
}

// Initializes control RPC end points.
// The object Layer will be a temp back used for testing purpose.
func initTestControlRPCEndPoint(objectLayer ObjectLayer) http.Handler {
	// Initialize Web.

	controllerHandlers := &controllerAPIHandlers{
		ObjectAPI: func() ObjectLayer { return objectLayer },
	}

	// Initialize router.
	muxRouter := router.NewRouter()
	registerControllerRPCRouter(muxRouter, controllerHandlers)
	return muxRouter
}

// StartTestRPCServer - Creates a temp XL/FS backend and initializes control RPC end points,
// then starts a test server with those control RPC end points registered.
func StartTestRPCServer(t TestErrHandler, instanceType string) TestServer {
	// create temporary backend for the test server.
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	// create an instance of TestServer.
	testRPCServer := TestServer{}
	// create temporary backend for the test server.
	objLayer, err := makeTestBackend(disks, instanceType)

	if err != nil {
		t.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}

	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Get credential.
	credentials := serverConfig.GetCredential()

	testRPCServer.Root = root
	testRPCServer.Disks = disks
	testRPCServer.AccessKey = credentials.AccessKeyID
	testRPCServer.SecretKey = credentials.SecretAccessKey
	testRPCServer.Obj = objLayer
	// Run TestServer.
	testRPCServer.Server = httptest.NewServer(initTestControlRPCEndPoint(objLayer))

	return testRPCServer
}

// Configure the server for the test run.
func newTestConfig(bucketLocation string) (rootPath string, err error) {
	// Get test root.
	rootPath, err = getTestRoot()
	if err != nil {
		return "", err
	}

	// Do this only once here.
	setGlobalConfigPath(rootPath)

	// Initialize server config.
	if err = initConfig(); err != nil {
		return "", err
	}

	// Set a default region.
	serverConfig.SetRegion(bucketLocation)

	// Save config.
	if err = serverConfig.Save(); err != nil {
		return "", err
	}

	// Return root path.
	return rootPath, nil
}

// Deleting the temporary backend and stopping the server.
func (testServer TestServer) Stop() {
	removeAll(testServer.Root)
	for _, disk := range testServer.Disks {
		removeAll(disk)
	}
	testServer.Server.Close()
}

// Sign given request using Signature V4.
func signStreamingRequest(req *http.Request, accessKey, secretKey string) (string, error) {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return "", fmt.Errorf("Invalid hashed payload.")
	}

	currTime := time.Now().UTC()
	// Set x-amz-date.
	req.Header.Set("x-amz-date", currTime.Format(iso8601Format))

	// Get header map.
	headerMap := make(map[string][]string)
	for k, vv := range req.Header {
		// If request header key is not in ignored headers, then add it.
		if _, ok := ignoredStreamingHeaders[http.CanonicalHeaderKey(k)]; !ok {
			headerMap[strings.ToLower(k)] = vv
		}
	}

	// Get header keys.
	headers := []string{"host"}
	for k := range headerMap {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	// Get canonical headers.
	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range headerMap[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	canonicalHeaders := buf.String()

	// Get signed headers.
	signedHeaders := strings.Join(headers, ";")

	// Get canonical query string.
	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)

	// Get canonical URI.
	canonicalURI := getURLEncodedName(req.URL.Path)

	// Get canonical request.
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		hashedPayload,
	}, "\n")

	// Get scope.
	scope := strings.Join([]string{
		currTime.Format(yyyymmdd),
		"us-east-1",
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("us-east-1"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return signature, nil
}

// Returns new HTTP request object.
func newTestStreamingRequest(method, urlStr string, dataLength, chunkSize int64, body io.ReadSeeker) (*http.Request, error) {
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		return nil, err
	}

	if body == nil {
		// this is added to avoid panic during ioutil.ReadAll(req.Body).
		// th stack trace can be found here  https://github.com/minio/minio/pull/2074 .
		// This is very similar to https://github.com/golang/go/issues/7527.
		req.Body = ioutil.NopCloser(bytes.NewReader([]byte("")))
	}

	contentLength := calculateStreamContentLength(dataLength, chunkSize)

	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("content-encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", strconv.FormatInt(dataLength, 10))
	req.Header.Set("content-length", strconv.FormatInt(contentLength, 10))

	// Seek back to beginning.
	body.Seek(0, 0)

	// Add body
	req.Body = ioutil.NopCloser(body)
	req.ContentLength = contentLength

	return req, nil
}

// Returns new HTTP request object signed with streaming signature v4.
func newTestStreamingSignedRequest(method, urlStr string, contentLength, chunkSize int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestStreamingRequest(method, urlStr, contentLength, chunkSize, body)
	if err != nil {
		return nil, err
	}

	signature, err := signStreamingRequest(req, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	regionStr := serverConfig.GetRegion()

	var stream []byte
	var buffer []byte
	body.Seek(0, 0)
	for {
		buffer = make([]byte, chunkSize)
		n, err := body.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, err
		}

		currTime := time.Now().UTC()
		// Get scope.
		scope := strings.Join([]string{
			currTime.Format(yyyymmdd),
			regionStr,
			"s3",
			"aws4_request",
		}, "/")

		stringToSign := "AWS4-HMAC-SHA256-PAYLOAD" + "\n"
		stringToSign = stringToSign + currTime.Format(iso8601Format) + "\n"
		stringToSign = stringToSign + scope + "\n"
		stringToSign = stringToSign + signature + "\n"
		stringToSign = stringToSign + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" + "\n" // hex(sum256(""))
		stringToSign = stringToSign + hex.EncodeToString(sum256(buffer[:n]))

		date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
		region := sumHMAC(date, []byte(regionStr))
		service := sumHMAC(region, []byte("s3"))
		signingKey := sumHMAC(service, []byte("aws4_request"))

		signature = hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

		stream = append(stream, []byte(fmt.Sprintf("%x", n)+";chunk-signature="+signature+"\r\n")...)
		stream = append(stream, buffer[:n]...)
		stream = append(stream, []byte("\r\n")...)

		if n <= 0 {
			break
		}

	}

	req.Body = ioutil.NopCloser(bytes.NewReader(stream))
	return req, nil
}

// Sign given request using Signature V4.
func signRequest(req *http.Request, accessKey, secretKey string) error {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return fmt.Errorf("Invalid hashed payload.")
	}

	currTime := time.Now().UTC()

	// Set x-amz-date.
	req.Header.Set("x-amz-date", currTime.Format(iso8601Format))

	// Get header map.
	headerMap := make(map[string][]string)
	for k, vv := range req.Header {
		// If request header key is not in ignored headers, then add it.
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; !ok {
			headerMap[strings.ToLower(k)] = vv
		}
	}

	// Get header keys.
	headers := []string{"host"}
	for k := range headerMap {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	region := serverConfig.GetRegion()

	// Get canonical headers.
	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range headerMap[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	canonicalHeaders := buf.String()

	// Get signed headers.
	signedHeaders := strings.Join(headers, ";")

	// Get canonical query string.
	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)

	// Get canonical URI.
	canonicalURI := getURLEncodedName(req.URL.Path)

	// Get canonical request.
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		hashedPayload,
	}, "\n")

	// Get scope.
	scope := strings.Join([]string{
		currTime.Format(yyyymmdd),
		region,
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	regionHMAC := sumHMAC(date, []byte(region))
	service := sumHMAC(regionHMAC, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return nil
}

// getCredential generate a credential string.
func getCredential(accessKeyID, location string, t time.Time) string {
	return accessKeyID + "/" + getScope(t, location)
}

// Returns new HTTP request object.
func newTestRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		return nil, err
	}

	// Add Content-Length
	req.ContentLength = contentLength

	// Save for subsequent use
	var hashedPayload string
	switch {
	case body == nil:
		hashedPayload = hex.EncodeToString(sum256([]byte{}))
	default:
		payloadBytes, e := ioutil.ReadAll(body)
		if e != nil {
			return nil, e
		}
		hashedPayload = hex.EncodeToString(sum256(payloadBytes))
		md5Base64 := base64.StdEncoding.EncodeToString(sumMD5(payloadBytes))
		req.Header.Set("Content-Md5", md5Base64)
	}
	req.Header.Set("x-amz-content-sha256", hashedPayload)
	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
		// Add body
		req.Body = ioutil.NopCloser(body)
	} else {
		// this is added to avoid panic during ioutil.ReadAll(req.Body).
		// th stack trace can be found here  https://github.com/minio/minio/pull/2074 .
		// This is very similar to https://github.com/golang/go/issues/7527.
		req.Body = ioutil.NopCloser(bytes.NewReader([]byte("")))
	}

	return req, nil
}

// Returns new HTTP request object signed with signature v4.
func newTestSignedRequest(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}

	// Anonymous request return quickly.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	err = signRequest(req, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// Return new WebRPC request object.
func newWebRPCRequest(methodRPC, authorization string, body io.ReadSeeker) (*http.Request, error) {
	req, err := http.NewRequest("POST", "/minio/webrpc", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		req.Header.Set("Authorization", "Bearer "+authorization)
	}
	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
		// Add body
		req.Body = ioutil.NopCloser(body)
	} else {
		// this is added to avoid panic during ioutil.ReadAll(req.Body).
		// th stack trace can be found here  https://github.com/minio/minio/pull/2074 .
		// This is very similar to https://github.com/golang/go/issues/7527.
		req.Body = ioutil.NopCloser(bytes.NewReader([]byte("")))
	}
	return req, nil
}

// Marshal request and return a new HTTP request object to call the webrpc
func newTestWebRPCRequest(rpcMethod string, authorization string, data interface{}) (*http.Request, error) {
	type genericJSON struct {
		JSONRPC string      `json:"jsonrpc"`
		ID      string      `json:"id"`
		Method  string      `json:"method"`
		Params  interface{} `json:"params"`
	}
	encapsulatedData := genericJSON{JSONRPC: "2.0", ID: "1", Method: rpcMethod, Params: data}
	jsonData, err := json.Marshal(encapsulatedData)
	if err != nil {
		return nil, err
	}
	req, err := newWebRPCRequest(rpcMethod, authorization, bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}
	return req, nil
}

type ErrWebRPC struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// Unmarshal response and return the webrpc response
func getTestWebRPCResponse(resp *httptest.ResponseRecorder, data interface{}) error {
	type rpcReply struct {
		ID      string      `json:"id"`
		JSONRPC string      `json:"jsonrpc"`
		Result  interface{} `json:"result"`
		Error   *ErrWebRPC  `json:"error"`
	}
	reply := &rpcReply{Result: &data}
	err := json.NewDecoder(resp.Body).Decode(reply)
	if err != nil {
		return err
	}
	// For the moment, web handlers errors code are not meaningful
	// Return only the error message
	if reply.Error != nil {
		return errors.New(reply.Error.Message)
	}
	return nil
}

// creates the temp backend setup.
// if the option is
// FS: Returns a temp single disk setup initializes FS Backend.
// XL: Returns a 16 temp single disk setup and initializse XL Backend.
func makeTestBackend(disks []string, instanceType string) (ObjectLayer, error) {
	switch instanceType {
	case "FS":
		objLayer, err := getSingleNodeObjectLayer(disks[0])
		if err != nil {
			return nil, err
		}
		return objLayer, err

	case "XL":
		objectLayer, err := getXLObjectLayer(disks, nil)
		if err != nil {
			return nil, err
		}
		return objectLayer, err
	default:
		errMsg := "Invalid instance type, Only FS and XL are valid options"
		return nil, fmt.Errorf("Failed obtaining Temp XL layer: <ERROR> %s", errMsg)
	}
}

var src = rand.NewSource(time.Now().UTC().UnixNano())

// Function to generate random string for bucket/object names.
func randString(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// generate random object name.
func getRandomObjectName() string {
	return randString(16)

}

// generate random bucket name.
func getRandomBucketName() string {
	return randString(60)

}

// TruncateWriter - Writes `n` bytes, then returns with number of bytes written.
// differs from iotest.TruncateWriter, the difference is commented in the Write method.
func TruncateWriter(w io.Writer, n int64) io.Writer {
	return &truncateWriter{w, n}
}

type truncateWriter struct {
	w io.Writer
	n int64
}

func (t *truncateWriter) Write(p []byte) (n int, err error) {
	if t.n <= 0 {
		return len(p), nil
	}
	// real write
	n = len(p)
	if int64(n) > t.n {
		n = int(t.n)
	}
	n, err = t.w.Write(p[0:n])
	t.n -= int64(n)
	// Removed from iotest.TruncateWriter.
	// Need the Write method to return truncated number of bytes written, not the size of the buffer requested to be written.
	// if err == nil {
	// 	n = len(p)
	// }
	return
}

// NewEOFWriter returns a Writer that writes to w,
// but returns EOF error after writing n bytes.
func NewEOFWriter(w io.Writer, n int64) io.Writer {
	return &EOFWriter{w, n}
}

type EOFWriter struct {
	w io.Writer
	n int64
}

// io.Writer implementation designed to error out with io.EOF after reading `n` bytes.
func (t *EOFWriter) Write(p []byte) (n int, err error) {
	if t.n <= 0 {
		return -1, io.EOF
	}
	// real write
	n = len(p)
	if int64(n) > t.n {
		n = int(t.n)
	}
	n, err = t.w.Write(p[0:n])
	t.n -= int64(n)
	if err == nil {
		n = len(p)
	}
	return
}

// queryEncode - encodes query values in their URL encoded form.
func queryEncode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := urlEncodePath(k) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(urlEncodePath(v))
		}
	}
	return buf.String()
}

// urlEncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncodePath(pathName string) string {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	if reservedNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}

// construct URL for http requests for bucket operations.
func makeTestTargetURL(endPoint, bucketName, objectName string, queryValues url.Values) string {
	urlStr := endPoint + "/"
	if bucketName != "" {
		urlStr = urlStr + bucketName + "/"
	}
	if objectName != "" {
		urlStr = urlStr + urlEncodePath(objectName)
	}
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + queryEncode(queryValues)
	}
	return urlStr
}

// return URL for uploading object into the bucket.
func getPutObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

// return URL for fetching object from the bucket.
func getGetObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

// return URL for deleting the object from the bucket.
func getDeleteObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

// return URL for deleting multiple objects from a bucket.
func getMultiDeleteObjectURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("delete", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)

}

// return URL for HEAD on the object.
func getHeadObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

// return url to be used while copying the object.
func getCopyObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

// return URL for inserting bucket notification.
func getPutNotificationURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("notification", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for fetching bucket notification.
func getGetNotificationURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("notification", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for inserting bucket policy.
func getPutPolicyURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("policy", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for fetching bucket policy.
func getGetPolicyURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("policy", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for deleting bucket policy.
func getDeletePolicyURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("policy", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for creating the bucket.
func getMakeBucketURL(endPoint, bucketName string) string {
	return makeTestTargetURL(endPoint, bucketName, "", url.Values{})
}

// return URL for listing buckets.
func getListBucketURL(endPoint string) string {
	return makeTestTargetURL(endPoint, "", "", url.Values{})
}

// return URL for HEAD on the bucket.
func getHEADBucketURL(endPoint, bucketName string) string {
	return makeTestTargetURL(endPoint, bucketName, "", url.Values{})
}

// return URL for deleting the bucket.
func getDeleteBucketURL(endPoint, bucketName string) string {
	return makeTestTargetURL(endPoint, bucketName, "", url.Values{})
}

// return URL For fetching location of the bucket.
func getBucketLocationURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("location", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for listing objects in the bucket with V1 legacy API.
func getListObjectsV1URL(endPoint, bucketName string, maxKeys string) string {
	queryValue := url.Values{}
	if maxKeys != "" {
		queryValue.Set("max-keys", maxKeys)
	}
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for listing objects in the bucket with V2 API.
func getListObjectsV2URL(endPoint, bucketName string, maxKeys string, fetchOwner string) string {
	queryValue := url.Values{}
	queryValue.Set("list-type", "2") // Enables list objects V2 URL.
	if maxKeys != "" {
		queryValue.Set("max-keys", maxKeys)
	}
	if fetchOwner != "" {
		queryValue.Set("fetch-owner", fetchOwner)
	}
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for a new multipart upload.
func getNewMultipartURL(endPoint, bucketName, objectName string) string {
	queryValue := url.Values{}
	queryValue.Set("uploads", "")
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValue)
}

// return URL for a new multipart upload.
func getPartUploadURL(endPoint, bucketName, objectName, uploadID, partNumber string) string {
	queryValues := url.Values{}
	queryValues.Set("uploadId", uploadID)
	queryValues.Set("partNumber", partNumber)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValues)
}

// return URL for aborting multipart upload.
func getAbortMultipartUploadURL(endPoint, bucketName, objectName, uploadID string) string {
	queryValue := url.Values{}
	queryValue.Set("uploadId", uploadID)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValue)
}

// return URL for a listing pending multipart uploads.
func getListMultipartURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("uploads", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for listing pending multipart uploads with parameters.
func getListMultipartUploadsURLWithParams(endPoint, bucketName, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads string) string {
	queryValue := url.Values{}
	queryValue.Set("uploads", "")
	queryValue.Set("prefix", prefix)
	queryValue.Set("delimiter", delimiter)
	queryValue.Set("key-marker", keyMarker)
	queryValue.Set("upload-id-marker", uploadIDMarker)
	queryValue.Set("max-uploads", maxUploads)
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for a listing parts on a given upload id.
func getListMultipartURLWithParams(endPoint, bucketName, objectName, uploadID, maxParts string) string {
	queryValues := url.Values{}
	queryValues.Set("uploadId", uploadID)
	queryValues.Set("max-parts", maxParts)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValues)
}

// return URL for completing multipart upload.
// complete multipart upload request is sent after all parts are uploaded.
func getCompleteMultipartUploadURL(endPoint, bucketName, objectName, uploadID string) string {
	queryValue := url.Values{}
	queryValue.Set("uploadId", uploadID)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValue)
}

// return URL for put bucket notification.
func getPutBucketNotificationURL(endPoint, bucketName string) string {
	return getGetBucketNotificationURL(endPoint, bucketName)
}

// return URL for get bucket notification.
func getGetBucketNotificationURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("notification", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// returns temp root directory. `
func getTestRoot() (string, error) {
	return ioutil.TempDir(os.TempDir(), "api-")
}

// getRandomDisks - Creates a slice of N random disks, each of the form - minio-XXX
func getRandomDisks(N int) ([]string, error) {
	var erasureDisks []string
	for i := 0; i < N; i++ {
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		if err != nil {
			// Remove directories created so far.
			removeRoots(erasureDisks)
			return nil, err
		}
		erasureDisks = append(erasureDisks, path)
	}
	return erasureDisks, nil
}

// getXLObjectLayer - Instantiates XL object layer and returns it.
func getXLObjectLayer(erasureDisks []string, ignoredDisks []string) (ObjectLayer, error) {
	err := formatDisks(erasureDisks, ignoredDisks)
	if err != nil {
		return nil, err
	}
	objLayer, err := newXLObjects(erasureDisks, ignoredDisks)
	if err != nil {
		return nil, err
	}
	// Disabling the cache for integration tests.
	// Should use the object layer tests for validating cache.
	if xl, ok := objLayer.(xlObjects); ok {
		xl.objCacheEnabled = false
	}

	return objLayer, nil
}

// getSingleNodeObjectLayer - Instantiates single node object layer and returns it.
func getSingleNodeObjectLayer(disk string) (ObjectLayer, error) {
	// Create the object layer.
	objLayer, err := newFSObjects(disk)
	if err != nil {
		return nil, err
	}
	return objLayer, nil
}

// removeRoots - Cleans up initialized directories during tests.
func removeRoots(roots []string) {
	for _, root := range roots {
		removeAll(root)
	}
}

//removeDiskN - removes N disks from supplied disk slice.
func removeDiskN(disks []string, n int) {
	if n > len(disks) {
		n = len(disks)
	}
	for _, disk := range disks[:n] {
		removeAll(disk)
	}
}

// creates a bucket for the tests and returns the bucket name.
// initializes the specified API endpoints for the tests.
// initialies the root and returns its path.
// return credentials.
func initAPIHandlerTest(obj ObjectLayer, endPoints []string) (bucketName, rootPath string, apiRouter http.Handler, err error) {
	// get random bucket name.
	bucketName = getRandomBucketName()

	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, return err.
		return "", "", nil, err
	}
	// Register the API end points with XL/FS object layer.
	// Registering only the GetObject handler.
	apiRouter = initTestAPIEndPoints(obj, endPoints)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err = newTestConfig("us-east-1")
	if err != nil {
		return "", "", nil, err
	}

	return bucketName, rootPath, apiRouter, nil
}

// ExecObjectLayerAPITest - executes object layer API tests.
// Creates single node and XL ObjectLayer instance, registers the specified API end points and runs test for both the layers.
func ExecObjectLayerAPITest(t TestErrHandler, objAPITest objAPITestType, endPoints []string) {
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}
	bucketFS, fsRoot, fsAPIRouter, err := initAPIHandlerTest(objLayer, endPoints)
	if err != nil {
		t.Fatalf("Initialzation of API handler tests failed: <ERROR> %s", err)
	}
	credentials := serverConfig.GetCredential()
	// Executing the object layer tests for single node setup.
	objAPITest(objLayer, singleNodeTestStr, bucketFS, fsAPIRouter, credentials, t)

	objLayer, xlDisks, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	bucketXL, xlRoot, xlAPIRouter, err := initAPIHandlerTest(objLayer, endPoints)
	if err != nil {
		t.Fatalf("Initialzation of API handler tests failed: <ERROR> %s", err)
	}
	credentials = serverConfig.GetCredential()
	// Executing the object layer tests for XL.
	objAPITest(objLayer, xLTestStr, bucketXL, xlAPIRouter, credentials, t)
	defer removeRoots(append(xlDisks, fsDir, fsRoot, xlRoot))
}

// function to be passed to ExecObjectLayerAPITest, for executing object layr API handler tests.
type objAPITestType func(obj ObjectLayer, instanceType string, bucketName string,
	apiRouter http.Handler, credentials credential, t TestErrHandler)

// Regular object test type.
type objTestType func(obj ObjectLayer, instanceType string, t TestErrHandler)

// Special object test type for disk not found situations.
type objTestDiskNotFoundType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerTest - executes object layer tests.
// Creates single node and XL ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTest(t TestErrHandler, objTest objTestType) {
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}
	// Executing the object layer tests for single node setup.
	objTest(objLayer, singleNodeTestStr, t)

	objLayer, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, t)
	defer removeRoots(append(fsDirs, fsDir))
}

// ExecObjectLayerDiskNotFoundTest - executes object layer tests while deleting
// disks in between tests. Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerDiskNotFoundTest(t *testing.T, objTest objTestDiskNotFoundType) {
	objLayer, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}

// Special object test type for stale files situations.
type objTestStaleFilesType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerStaleFilesTest - executes object layer tests those leaves stale
// files/directories under .minio/tmp.  Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerStaleFilesTest(t *testing.T, objTest objTestStaleFilesType) {
	nDisks := 16
	erasureDisks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatalf("Initialization of disks for XL setup: %s", err)
	}
	objLayer, err := getXLObjectLayer(erasureDisks, nil)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, erasureDisks, t)
	defer removeRoots(erasureDisks)
}

// Takes in XL/FS object layer, and the list of API end points to be tested/required, registers the API end points and returns the HTTP handler.
// Need isolated registration of API end points while writing unit tests for end points.
// All the API end points are registered only for the default case.
func initTestAPIEndPoints(objLayer ObjectLayer, apiFunctions []string) http.Handler {
	// initialize a new mux router.
	// goriilla/mux is the library used to register all the routes and handle them.
	muxRouter := router.NewRouter()

	// All object storage operations are registered as HTTP handlers on `objectAPIHandlers`.
	// When the handlers get a HTTP request they use the underlyting ObjectLayer to perform operations.
	objLayerMutex.Lock()
	globalObjectAPI = objLayer
	objLayerMutex.Unlock()

	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// API Router.
	apiRouter := muxRouter.NewRoute().PathPrefix("/").Subrouter()
	// Bucket router.
	bucket := apiRouter.PathPrefix("/{bucket}").Subrouter()
	// Iterate the list of API functions requested for and register them in mux HTTP handler.
	for _, apiFunction := range apiFunctions {
		switch apiFunction {
		// Register ListBuckets	handler.
		case "ListBuckets":
			apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
		// Register GetObject handler.
		case "GetObject":
			bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.GetObjectHandler)
			// Register Delete Object handler.
		case "DeleteObject":
			bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.DeleteObjectHandler)
		// Register Copy Object  handler.
		case "CopyObject":
			bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectHandler)
		// Register PutBucket Policy handler.
		case "PutBucketPolicy":
			bucket.Methods("PUT").HandlerFunc(api.PutBucketPolicyHandler).Queries("policy", "")
		// Register Delete bucket HTTP policy handler.
		case "DeleteBucketPolicy":
			bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketPolicyHandler).Queries("policy", "")
		// Register Get Bucket policy HTTP Handler.
		case "GetBucketPolicy":
			bucket.Methods("GET").HandlerFunc(api.GetBucketPolicyHandler).Queries("policy", "")
		// Register GetBucketLocation handler.
		case "GetBucketLocation":
			bucket.Methods("GET").HandlerFunc(api.GetBucketLocationHandler).Queries("location", "")
		// Register HeadBucket handler.
		case "HeadBucket":
			bucket.Methods("HEAD").HandlerFunc(api.HeadBucketHandler)
			// Register New Multipart upload handler.
		case "NewMultipart":
			bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.NewMultipartUploadHandler).Queries("uploads", "")

		// Register ListMultipartUploads handler.
		case "ListMultipartUploads":
			bucket.Methods("GET").HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
			// Register Complete Multipart Upload handler.
		case "CompleteMultipart":
			bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
			// Register GetBucketNotification Handler.
		case "GetBucketNotification":
			bucket.Methods("GET").HandlerFunc(api.GetBucketNotificationHandler).Queries("notification", "")
			// Register PutBucketNotification Handler.
		case "PutBucketNotification":
			bucket.Methods("PUT").HandlerFunc(api.PutBucketNotificationHandler).Queries("notification", "")
		// Register all api endpoints by default.
		default:
			registerAPIRouter(muxRouter, api)
			// No need to register any more end points, all the end points are registered.
			break
		}
	}
	return muxRouter
}

// Initialize Web RPC Handlers for testing
func initTestWebRPCEndPoint(objLayer ObjectLayer) http.Handler {
	// Initialize Web.
	webHandlers := &webAPIHandlers{
		ObjectAPI: func() ObjectLayer { return objLayer },
	}

	// Initialize router.
	muxRouter := router.NewRouter()
	registerWebRouter(muxRouter, webHandlers)
	return muxRouter
}
