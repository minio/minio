/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fatih/color"
	router "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/s3signer"
	"github.com/minio/minio/pkg/hash"
)

// Tests should initNSLock only once.
func init() {
	// Set as non-distributed.
	globalIsDistXL = false

	// Initialize name space lock.
	initNSLock(globalIsDistXL)

	// Disable printing console messages during tests.
	color.Output = ioutil.Discard

	// Set system resources to maximum.
	setMaxResources()

	// Quiet logging.
	log.logger.Hooks = nil
}

// concurreny level for certain parallel tests.
const testConcurrencyLevel = 10

///
/// Excerpts from @lsegal - https://github.com/aws/aws-sdk-js/issues/659#issuecomment-120477258
///
///  User-Agent:
///
///      This is ignored from signing because signing this causes problems with generating pre-signed URLs
///      (that are executed by other agents) or when customers pass requests through proxies, which may
///      modify the user-agent.
///
///  Content-Length:
///
///      This is ignored from signing because generating a pre-signed URL should not provide a content-length
///      constraint, specifically when vending a S3 pre-signed PUT URL. The corollary to this is that when
///      sending regular requests (non-pre-signed), the signature contains a checksum of the body, which
///      implicitly validates the payload length (since changing the number of bytes would change the checksum)
///      and therefore this header is not valuable in the signature.
///
///  Content-Type:
///
///      Signing this header causes quite a number of problems in browser environments, where browsers
///      like to modify and normalize the content-type header in different ways. There is more information
///      on this in https://github.com/aws/aws-sdk-js/issues/244. Avoiding this field simplifies logic
///      and reduces the possibility of future bugs
///
///  Authorization:
///
///      Is skipped for obvious reasons
///
var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// Headers to ignore in streaming v4
var ignoredStreamingHeaders = map[string]bool{
	"Authorization": true,
	"Content-Type":  true,
	"Content-Md5":   true,
	"User-Agent":    true,
}

// calculateSignedChunkLength - calculates the length of chunk metadata
func calculateSignedChunkLength(chunkDataSize int64) int64 {
	return int64(len(fmt.Sprintf("%x", chunkDataSize))) +
		17 + // ";chunk-signature="
		64 + // e.g. "f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2"
		2 + // CRLF
		chunkDataSize +
		2 // CRLF
}

func mustGetHashReader(t TestErrHandler, data io.Reader, size int64, md5hex, sha256hex string) *hash.Reader {
	hr, err := hash.NewReader(data, size, md5hex, sha256hex)
	if err != nil {
		t.Fatal(err)
	}
	return hr
}

// calculateSignedChunkLength - calculates the length of the overall stream (data + metadata)
func calculateStreamContentLength(dataLen, chunkSize int64) int64 {
	if dataLen <= 0 {
		return 0
	}
	chunksCount := int64(dataLen / chunkSize)
	remainingBytes := int64(dataLen % chunkSize)
	var streamLen int64
	streamLen += chunksCount * calculateSignedChunkLength(chunkSize)
	if remainingBytes > 0 {
		streamLen += calculateSignedChunkLength(remainingBytes)
	}
	streamLen += calculateSignedChunkLength(0)
	return streamLen
}

func prepareFS() (ObjectLayer, string, error) {
	nDisks := 1
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		return nil, "", err
	}
	obj, err := newFSObjectLayer(fsDirs[0])
	if err != nil {
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
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		removeRoots(fsDirs)
		return nil, nil, err
	}
	return obj, fsDirs, nil
}

// Initialize FS objects.
func initFSObjects(disk string, t *testing.T) (obj ObjectLayer) {
	newTestConfig(globalMinioDefaultRegion)
	var err error
	obj, err = newFSObjectLayer(disk)
	if err != nil {
		t.Fatal(err)
	}
	return obj
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
	// FSTestStr is the string which is used as notation for Single node ObjectLayer in the unit tests.
	FSTestStr string = "FS"

	// XLTestStr is the string which is used as notation for XL ObjectLayer in the unit tests.
	XLTestStr string = "XL"
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

// Temp files created in default Tmp dir
var globalTestTmpDir = os.TempDir()

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
	Disks     EndpointList
	AccessKey string
	SecretKey string
	Server    *httptest.Server
	Obj       ObjectLayer
	endpoints EndpointList
}

// UnstartedTestServer - Configures a temp FS/XL backend,
// initializes the endpoints and configures the test server.
// The server should be started using the Start() method.
func UnstartedTestServer(t TestErrHandler, instanceType string) TestServer {
	// create an instance of TestServer.
	testServer := TestServer{}
	// return FS/XL object layer and temp backend.
	objLayer, disks, err := prepareTestBackend(instanceType)
	if err != nil {
		t.Fatal(err)
	}
	// set the server configuration.
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Test Server needs to start before formatting of disks.
	// Get credential.
	credentials := serverConfig.GetCredential()

	testServer.Obj = objLayer
	testServer.Disks = mustGetNewEndpointList(disks...)
	testServer.Root = root
	testServer.AccessKey = credentials.AccessKey
	testServer.SecretKey = credentials.SecretKey

	httpHandler, err := configureServerHandler(testServer.Disks)
	if err != nil {
		t.Fatalf("Failed to configure one of the RPC services <ERROR> %s", err)
	}

	// Run TestServer.
	testServer.Server = httptest.NewUnstartedServer(httpHandler)

	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// initialize peer rpc
	host, port := mustSplitHostPort(testServer.Server.Listener.Addr().String())
	globalMinioHost = host
	globalMinioPort = port
	globalMinioAddr = getEndpointsLocalAddr(testServer.Disks)
	initGlobalS3Peers(testServer.Disks)

	return testServer
}

// testServerCertPEM and testServerKeyPEM are generated by
//  https://golang.org/src/crypto/tls/generate_cert.go
//    $ go run generate_cert.go -ca --host 127.0.0.1
// The generated certificate contains IP SAN, that way we don't need
// to enable InsecureSkipVerify in TLS config

// Starts the test server and returns the TestServer with TLS configured instance.
func StartTestTLSServer(t TestErrHandler, instanceType string, cert, key []byte) TestServer {
	// Fetch TLS key and pem files from test-data/ directory.
	//	dir, _ := os.Getwd()
	//	testDataDir := filepath.Join(filepath.Dir(dir), "test-data")
	//
	//	pemFile := filepath.Join(testDataDir, "server.pem")
	//	keyFile := filepath.Join(testDataDir, "server.key")
	cer, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatalf("Failed to load certificate: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cer}}

	testServer := UnstartedTestServer(t, instanceType)
	testServer.Server.TLS = config
	testServer.Server.StartTLS()
	return testServer
}

// Starts the test server and returns the TestServer instance.
func StartTestServer(t TestErrHandler, instanceType string) TestServer {
	// create an instance of TestServer.
	testServer := UnstartedTestServer(t, instanceType)
	testServer.Server.Start()
	return testServer
}

// Initializes storage RPC endpoints.
// The object Layer will be a temp back used for testing purpose.
func initTestStorageRPCEndPoint(endpoints EndpointList) http.Handler {
	// Initialize router.
	muxRouter := router.NewRouter().SkipClean(true)
	registerStorageRPCRouters(muxRouter, endpoints)
	return muxRouter
}

// StartTestStorageRPCServer - Creates a temp XL backend and initializes storage RPC end points,
// then starts a test server with those storage RPC end points registered.
func StartTestStorageRPCServer(t TestErrHandler, instanceType string, diskN int) TestServer {
	// create temporary backend for the test server.
	disks, err := getRandomDisks(diskN)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}

	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Create an instance of TestServer.
	testRPCServer := TestServer{}
	// Get credential.
	credentials := serverConfig.GetCredential()

	endpoints := mustGetNewEndpointList(disks...)
	testRPCServer.Root = root
	testRPCServer.Disks = endpoints
	testRPCServer.AccessKey = credentials.AccessKey
	testRPCServer.SecretKey = credentials.SecretKey

	// Run TestServer.
	testRPCServer.Server = httptest.NewServer(initTestStorageRPCEndPoint(endpoints))
	return testRPCServer
}

// Sets up a Peers RPC test server.
func StartTestPeersRPCServer(t TestErrHandler, instanceType string) TestServer {
	// create temporary backend for the test server.
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}

	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// create an instance of TestServer.
	testRPCServer := TestServer{}
	// Get credential.
	credentials := serverConfig.GetCredential()

	endpoints := mustGetNewEndpointList(disks...)
	testRPCServer.Root = root
	testRPCServer.Disks = endpoints
	testRPCServer.AccessKey = credentials.AccessKey
	testRPCServer.SecretKey = credentials.SecretKey

	// create temporary backend for the test server.
	objLayer, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}

	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	testRPCServer.Obj = objLayer
	globalObjLayerMutex.Unlock()

	mux := router.NewRouter().SkipClean(true)
	// need storage layer for bucket config storage.
	registerStorageRPCRouters(mux, endpoints)
	// need API layer to send requests, etc.
	registerAPIRouter(mux)
	// module being tested is Peer RPCs router.
	registerS3PeerRPCRouter(mux)

	// Run TestServer.
	testRPCServer.Server = httptest.NewServer(mux)

	// initialize remainder of serverCmdConfig
	testRPCServer.endpoints = endpoints

	return testRPCServer
}

// Sets the global config path to empty string.
func resetGlobalConfigPath() {
	setConfigDir("")
}

// sets globalObjectAPI to `nil`.
func resetGlobalObjectAPI() {
	globalObjLayerMutex.Lock()
	globalObjectAPI = nil
	globalObjLayerMutex.Unlock()
}

// reset the value of the Global server config.
// set it to `nil`.
func resetGlobalConfig() {
	// hold the mutex lock before a new config is assigned.
	serverConfigMu.Lock()
	// Save the loaded config globally.
	serverConfig = nil
	serverConfigMu.Unlock()
}

// reset global NSLock.
func resetGlobalNSLock() {
	if globalNSMutex != nil {
		globalNSMutex = nil
	}
}

// reset Global event notifier.
func resetGlobalEventnotify() {
	globalEventNotifier = nil
}

func resetGlobalEndpoints() {
	globalEndpoints = EndpointList{}
}

func resetGlobalIsXL() {
	globalIsXL = false
}

func resetGlobalIsEnvs() {
	globalIsEnvCreds = false
	globalIsEnvBrowser = false
	globalIsEnvRegion = false
}

// Resets all the globals used modified in tests.
// Resetting ensures that the changes made to globals by one test doesn't affect others.
func resetTestGlobals() {
	// set globalObjectAPI to `nil`.
	resetGlobalObjectAPI()
	// Reset config path set.
	resetGlobalConfigPath()
	// Reset Global server config.
	resetGlobalConfig()
	// Reset global NSLock.
	resetGlobalNSLock()
	// Reset global event notifier.
	resetGlobalEventnotify()
	// Reset global endpoints.
	resetGlobalEndpoints()
	// Reset global isXL flag.
	resetGlobalIsXL()
	// Reset global isEnvCreds flag.
	resetGlobalIsEnvs()
}

// Configure the server for the test run.
func newTestConfig(bucketLocation string) (rootPath string, err error) {
	// Get test root.
	rootPath, err = getTestRoot()
	if err != nil {
		return "", err
	}

	// Do this only once here.
	setConfigDir(rootPath)

	// Initialize server config.
	if err = newConfig(); err != nil {
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
	os.RemoveAll(testServer.Root)
	for _, disk := range testServer.Disks {
		os.RemoveAll(disk.Path)
	}
	testServer.Server.Close()
}

// Truncate request to simulate unexpected EOF for a request signed using streaming signature v4.
func truncateChunkByHalfSigv4(req *http.Request) (*http.Request, error) {
	bufReader := bufio.NewReader(req.Body)
	hexChunkSize, chunkSignature, err := readChunkLine(bufReader)
	if err != nil {
		return nil, err
	}

	newChunkHdr := []byte(fmt.Sprintf("%s"+s3ChunkSignatureStr+"%s\r\n",
		hexChunkSize, chunkSignature))
	newChunk, err := ioutil.ReadAll(bufReader)
	if err != nil {
		return nil, err
	}
	newReq := req
	newReq.Body = ioutil.NopCloser(
		bytes.NewReader(bytes.Join([][]byte{newChunkHdr, newChunk[:len(newChunk)/2]},
			[]byte(""))),
	)
	return newReq, nil
}

// Malform data given a request signed using streaming signature V4.
func malformDataSigV4(req *http.Request, newByte byte) (*http.Request, error) {
	bufReader := bufio.NewReader(req.Body)
	hexChunkSize, chunkSignature, err := readChunkLine(bufReader)
	if err != nil {
		return nil, err
	}

	newChunkHdr := []byte(fmt.Sprintf("%s"+s3ChunkSignatureStr+"%s\r\n",
		hexChunkSize, chunkSignature))
	newChunk, err := ioutil.ReadAll(bufReader)
	if err != nil {
		return nil, err
	}

	newChunk[0] = newByte
	newReq := req
	newReq.Body = ioutil.NopCloser(
		bytes.NewReader(bytes.Join([][]byte{newChunkHdr, newChunk},
			[]byte(""))),
	)

	return newReq, nil
}

// Malform chunk size given a request signed using streaming signatureV4.
func malformChunkSizeSigV4(req *http.Request, badSize int64) (*http.Request, error) {
	bufReader := bufio.NewReader(req.Body)
	_, chunkSignature, err := readChunkLine(bufReader)
	if err != nil {
		return nil, err
	}

	n := badSize
	newHexChunkSize := []byte(fmt.Sprintf("%x", n))
	newChunkHdr := []byte(fmt.Sprintf("%s"+s3ChunkSignatureStr+"%s\r\n",
		newHexChunkSize, chunkSignature))
	newChunk, err := ioutil.ReadAll(bufReader)
	if err != nil {
		return nil, err
	}

	newReq := req
	newReq.Body = ioutil.NopCloser(
		bytes.NewReader(bytes.Join([][]byte{newChunkHdr, newChunk},
			[]byte(""))),
	)

	return newReq, nil
}

// Sign given request using Signature V4.
func signStreamingRequest(req *http.Request, accessKey, secretKey string, currTime time.Time) (string, error) {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return "", fmt.Errorf("Invalid hashed payload")
	}

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
		globalMinioDefaultRegion,
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	region := sumHMAC(date, []byte(globalMinioDefaultRegion))
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

func assembleStreamingChunks(req *http.Request, body io.ReadSeeker, chunkSize int64,
	secretKey, signature string, currTime time.Time) (*http.Request, error) {

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
		stringToSign = stringToSign + getSHA256Hash(buffer[:n])

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

func newTestStreamingSignedBadChunkDateRequest(method, urlStr string, contentLength, chunkSize int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestStreamingRequest(method, urlStr, contentLength, chunkSize, body)
	if err != nil {
		return nil, err
	}

	currTime := UTCNow()
	signature, err := signStreamingRequest(req, accessKey, secretKey, currTime)
	if err != nil {
		return nil, err
	}

	// skew the time between the chunk signature calculation and seed signature.
	currTime = currTime.Add(1 * time.Second)
	req, err = assembleStreamingChunks(req, body, chunkSize, secretKey, signature, currTime)
	return req, err
}

func newTestStreamingSignedCustomEncodingRequest(method, urlStr string, contentLength, chunkSize int64, body io.ReadSeeker, accessKey, secretKey, contentEncoding string) (*http.Request, error) {
	req, err := newTestStreamingRequest(method, urlStr, contentLength, chunkSize, body)
	if err != nil {
		return nil, err
	}

	// Set custom encoding.
	req.Header.Set("content-encoding", contentEncoding)

	currTime := UTCNow()
	signature, err := signStreamingRequest(req, accessKey, secretKey, currTime)
	if err != nil {
		return nil, err
	}

	req, err = assembleStreamingChunks(req, body, chunkSize, secretKey, signature, currTime)
	return req, err
}

// Returns new HTTP request object signed with streaming signature v4.
func newTestStreamingSignedRequest(method, urlStr string, contentLength, chunkSize int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestStreamingRequest(method, urlStr, contentLength, chunkSize, body)
	if err != nil {
		return nil, err
	}

	currTime := UTCNow()
	signature, err := signStreamingRequest(req, accessKey, secretKey, currTime)
	if err != nil {
		return nil, err
	}

	req, err = assembleStreamingChunks(req, body, chunkSize, secretKey, signature, currTime)
	return req, err
}

// preSignV4 presign the request, in accordance with
// http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
func preSignV4(req *http.Request, accessKeyID, secretAccessKey string, expires int64) error {
	// Presign is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return errors.New("Presign cannot be generated without access and secret keys")
	}

	region := serverConfig.GetRegion()
	date := UTCNow()
	scope := getScope(date, region)
	credential := fmt.Sprintf("%s/%s", accessKeyID, scope)

	// Set URL query.
	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Date", date.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.FormatInt(expires, 10))
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Content-Sha256", unsignedPayload)

	// "host" is the only header required to be signed for Presigned URLs.
	extractedSignedHeaders := make(http.Header)
	extractedSignedHeaders.Set("host", req.Host)

	queryStr := strings.Replace(query.Encode(), "+", "%20", -1)
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, unsignedPayload, queryStr, req.URL.Path, req.Method)
	stringToSign := getStringToSign(canonicalRequest, date, scope)
	signingKey := getSigningKey(secretAccessKey, date, region)
	signature := getSignature(signingKey, stringToSign)

	req.URL.RawQuery = query.Encode()

	// Add signature header to RawQuery.
	req.URL.RawQuery += "&X-Amz-Signature=" + url.QueryEscape(signature)

	// Construct the final presigned URL.
	return nil
}

// preSignV2 - presign the request in following style.
// https://${S3_BUCKET}.s3.amazonaws.com/${S3_OBJECT}?AWSAccessKeyId=${S3_ACCESS_KEY}&Expires=${TIMESTAMP}&Signature=${SIGNATURE}.
func preSignV2(req *http.Request, accessKeyID, secretAccessKey string, expires int64) error {
	// Presign is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return errors.New("Presign cannot be generated without access and secret keys")
	}

	// FIXME: Remove following portion of code after fixing a bug in minio-go preSignV2.

	d := UTCNow()
	// Find epoch expires when the request will expire.
	epochExpires := d.Unix() + expires

	// Add expires header if not present.
	expiresStr := req.Header.Get("Expires")
	if expiresStr == "" {
		expiresStr = strconv.FormatInt(epochExpires, 10)
		req.Header.Set("Expires", expiresStr)
	}

	// url.RawPath will be valid if path has any encoded characters, if not it will
	// be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := req.URL.RawPath
	encodedQuery := req.URL.RawQuery
	if encodedResource == "" {
		splits := strings.SplitN(req.URL.Path, "?", 2)
		encodedResource = splits[0]
		if len(splits) == 2 {
			encodedQuery = splits[1]
		}
	}

	unescapedQueries, err := unescapeQueries(encodedQuery)
	if err != nil {
		return err
	}

	// Get presigned string to sign.
	stringToSign := getStringToSignV2(req.Method, encodedResource, strings.Join(unescapedQueries, "&"), req.Header, expiresStr)
	hm := hmac.New(sha1.New, []byte(secretAccessKey))
	hm.Write([]byte(stringToSign))

	// Calculate signature.
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))

	query := req.URL.Query()
	// Handle specially for Google Cloud Storage.
	query.Set("AWSAccessKeyId", accessKeyID)
	// Fill in Expires for presigned query.
	query.Set("Expires", strconv.FormatInt(epochExpires, 10))

	// Encode query and save.
	req.URL.RawQuery = query.Encode()

	// Save signature finally.
	req.URL.RawQuery += "&Signature=" + url.QueryEscape(signature)
	return nil
}

// Sign given request using Signature V2.
func signRequestV2(req *http.Request, accessKey, secretKey string) error {
	req = s3signer.SignV2(*req, accessKey, secretKey)
	return nil
}

// Sign given request using Signature V4.
func signRequestV4(req *http.Request, accessKey, secretKey string) error {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return fmt.Errorf("Invalid hashed payload")
	}

	currTime := UTCNow()

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
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

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

// getCredentialString generate a credential string.
func getCredentialString(accessKeyID, location string, t time.Time) string {
	return accessKeyID + "/" + getScope(t, location)
}

// Returns new HTTP request object.
func newTestRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	if method == "" {
		method = "POST"
	}

	// Save for subsequent use
	var hashedPayload string
	var md5Base64 string
	switch {
	case body == nil:
		hashedPayload = getSHA256Hash([]byte{})
	default:
		payloadBytes, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		hashedPayload = getSHA256Hash(payloadBytes)
		md5Base64 = getMD5HashBase64(payloadBytes)
	}
	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
	} else {
		body = bytes.NewReader([]byte(""))
	}
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	if md5Base64 != "" {
		req.Header.Set("Content-Md5", md5Base64)
	}
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	// Add Content-Length
	req.ContentLength = contentLength

	return req, nil
}

// Various signature types we are supporting, currently
// two main signature types.
type signerType int

const (
	signerV2 signerType = iota
	signerV4
)

func newTestSignedRequest(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string, signer signerType) (*http.Request, error) {
	if signer == signerV2 {
		return newTestSignedRequestV2(method, urlStr, contentLength, body, accessKey, secretKey)
	}
	return newTestSignedRequestV4(method, urlStr, contentLength, body, accessKey, secretKey)
}

// Returns request with correct signature but with incorrect SHA256.
func newTestSignedBadSHARequest(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string, signer signerType) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}

	// Anonymous request return early.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	if signer == signerV2 {
		err = signRequestV2(req, accessKey, secretKey)
		req.Header.Del("x-amz-content-sha256")
	} else {
		req.Header.Set("x-amz-content-sha256", "92b165232fbd011da355eca0b033db22b934ba9af0145a437a832d27310b89f9")
		err = signRequestV4(req, accessKey, secretKey)
	}

	return req, err
}

// Returns new HTTP request object signed with signature v2.
func newTestSignedRequestV2(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}
	req.Header.Del("x-amz-content-sha256")

	// Anonymous request return quickly.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	err = signRequestV2(req, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// Returns new HTTP request object signed with signature v4.
func newTestSignedRequestV4(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}

	// Anonymous request return quickly.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	err = signRequestV4(req, accessKey, secretKey)
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

var src = rand.NewSource(UTCNow().UnixNano())

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

// construct URL for http requests for bucket operations.
func makeTestTargetURL(endPoint, bucketName, objectName string, queryValues url.Values) string {
	urlStr := endPoint + "/"
	if bucketName != "" {
		urlStr = urlStr + bucketName + "/"
	}
	if objectName != "" {
		urlStr = urlStr + getURLEncodedName(objectName)
	}
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + queryValues.Encode()
	}
	return urlStr
}

// return URL for uploading object into the bucket.
func getPutObjectURL(endPoint, bucketName, objectName string) string {
	return makeTestTargetURL(endPoint, bucketName, objectName, url.Values{})
}

func getPutObjectPartURL(endPoint, bucketName, objectName, uploadID, partNumber string) string {
	queryValues := url.Values{}
	queryValues.Set("uploadId", uploadID)
	queryValues.Set("partNumber", partNumber)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValues)
}

func getCopyObjectPartURL(endPoint, bucketName, objectName, uploadID, partNumber string) string {
	queryValues := url.Values{}
	queryValues.Set("uploadId", uploadID)
	queryValues.Set("partNumber", partNumber)
	return makeTestTargetURL(endPoint, bucketName, objectName, queryValues)
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

// return URL for deleting the bucket.
func getDeleteMultipleObjectsURL(endPoint, bucketName string) string {
	queryValue := url.Values{}
	queryValue.Set("delete", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
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
func getListMultipartURLWithParams(endPoint, bucketName, objectName, uploadID, maxParts, partNumberMarker, encoding string) string {
	queryValues := url.Values{}
	queryValues.Set("uploadId", uploadID)
	queryValues.Set("max-parts", maxParts)
	if partNumberMarker != "" {
		queryValues.Set("part-number-marker", partNumberMarker)
	}
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

// return URL for listen bucket notification.
func getListenBucketNotificationURL(endPoint, bucketName string, prefixes, suffixes, events []string) string {
	queryValue := url.Values{}

	queryValue["prefix"] = prefixes
	queryValue["suffix"] = suffixes
	queryValue["events"] = events
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// returns temp root directory. `
func getTestRoot() (string, error) {
	return ioutil.TempDir(globalTestTmpDir, "api-")
}

// getRandomDisks - Creates a slice of N random disks, each of the form - minio-XXX
func getRandomDisks(N int) ([]string, error) {
	var erasureDisks []string
	for i := 0; i < N; i++ {
		path, err := ioutil.TempDir(globalTestTmpDir, "minio-")
		if err != nil {
			// Remove directories created so far.
			removeRoots(erasureDisks)
			return nil, err
		}
		erasureDisks = append(erasureDisks, path)
	}
	return erasureDisks, nil
}

// initObjectLayer - Instantiates object layer and returns it.
func initObjectLayer(endpoints EndpointList) (ObjectLayer, []StorageAPI, error) {
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return nil, nil, err
	}

	formattedDisks, err := waitForFormatXLDisks(true, endpoints, storageDisks)
	if err != nil {
		return nil, nil, err
	}

	objLayer, err := newXLObjectLayer(formattedDisks)
	if err != nil {
		return nil, nil, err
	}

	// Disabling the cache for integration tests.
	// Should use the object layer tests for validating cache.
	if xl, ok := objLayer.(*xlObjects); ok {
		xl.objCacheEnabled = false
	}

	// Success.
	return objLayer, formattedDisks, nil
}

// removeRoots - Cleans up initialized directories during tests.
func removeRoots(roots []string) {
	for _, root := range roots {
		os.RemoveAll(root)
	}
}

//removeDiskN - removes N disks from supplied disk slice.
func removeDiskN(disks []string, n int) {
	if n > len(disks) {
		n = len(disks)
	}
	for _, disk := range disks[:n] {
		os.RemoveAll(disk)
	}
}

// Makes a entire new copy of a StorageAPI slice.
func deepCopyStorageDisks(storageDisks []StorageAPI) []StorageAPI {
	newStorageDisks := make([]StorageAPI, len(storageDisks))
	copy(newStorageDisks, storageDisks)
	return newStorageDisks
}

// Initializes storage disks with 'N' errored disks, N disks return 'err' for each disk access.
func prepareNErroredDisks(storageDisks []StorageAPI, offline int, err error, t *testing.T) []StorageAPI {
	if offline > len(storageDisks) {
		t.Fatal("Requested more offline disks than supplied storageDisks slice", offline, len(storageDisks))
	}

	for i := 0; i < offline; i++ {
		storageDisks[i] = &naughtyDisk{disk: &retryStorage{
			remoteStorage:    storageDisks[i],
			maxRetryAttempts: 1,
			retryUnit:        time.Millisecond,
			retryCap:         time.Millisecond * 10,
		}, defaultErr: err}
	}
	return storageDisks
}

// Initializes storage disks with 'N' offline disks, N disks returns 'errDiskNotFound' for each disk access.
func prepareNOfflineDisks(storageDisks []StorageAPI, offline int, t *testing.T) []StorageAPI {
	return prepareNErroredDisks(storageDisks, offline, errDiskNotFound, t)
}

// Initializes backend storage disks.
func prepareXLStorageDisks(t *testing.T) ([]StorageAPI, []string) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	_, storageDisks, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		removeRoots(fsDirs)
		t.Fatal("Unable to initialize storage disks", err)
	}
	return storageDisks, fsDirs
}

// creates a bucket for the tests and returns the bucket name.
// initializes the specified API endpoints for the tests.
// initialies the root and returns its path.
// return credentials.
func initAPIHandlerTest(obj ObjectLayer, endpoints []string) (string, http.Handler, error) {
	// get random bucket name.
	bucketName := getRandomBucketName()

	// Create bucket.
	err := obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, return err.
		return "", nil, err
	}
	// Register the API end points with XL object layer.
	// Registering only the GetObject handler.
	apiRouter := initTestAPIEndPoints(obj, endpoints)
	var f http.HandlerFunc
	f = func(w http.ResponseWriter, r *http.Request) {
		r.RequestURI = r.URL.RequestURI()
		apiRouter.ServeHTTP(w, r)
	}
	return bucketName, f, nil
}

// prepare test backend.
// create FS/XL bankend.
// return object layer, backend disks.
func prepareTestBackend(instanceType string) (ObjectLayer, []string, error) {
	switch instanceType {
	// Total number of disks for XL backend is set to 16.
	case XLTestStr:
		return prepareXL()
	default:
		// return FS backend by default.
		obj, disk, err := prepareFS()
		if err != nil {
			return nil, nil, err
		}
		return obj, []string{disk}, nil
	}
}

// ExecObjectLayerAPIAnonTest - Helper function to validate object Layer API handler
// response for anonymous/unsigned and unknown signature type HTTP request.

// Here is the brief description of some of the arguments to the function below.
//   apiRouter - http.Handler with the relevant API endPoint (API endPoint under test) registered.
//   anonReq   - unsigned *http.Request to invoke the handler's response for anonymous requests.
//   policyFunc    - function to return bucketPolicy statement which would permit the anonymous request to be served.
// The test works in 2 steps, here is the description of the steps.
//   STEP 1: Call the handler with the unsigned HTTP request (anonReq), assert for the `ErrAccessDenied` error response.
//   STEP 2: Set the policy to allow the unsigned request, use the policyFunc to obtain the relevant statement and call
//           the handler again to verify its success.
func ExecObjectLayerAPIAnonTest(t *testing.T, testName, bucketName, objectName, instanceType string, apiRouter http.Handler,
	anonReq *http.Request, policyFunc func(string, string) policyStatement) {

	anonTestStr := "Anonymous HTTP request test"
	unknownSignTestStr := "Unknown HTTP signature test"

	// simple function which returns a message which gives the context of the test
	// and then followed by the the actual error message.
	failTestStr := func(testType, failMsg string) string {
		return fmt.Sprintf("Minio %s: %s fail for \"%s\": \n<Error> %s", instanceType, testType, testName, failMsg)
	}

	// httptest Recorder to capture all the response by the http handler.
	rec := httptest.NewRecorder()
	// reading the body to preserve it so that it can be used again for second attempt of sending unsigned HTTP request.
	// If the body is read in the handler the same request cannot be made use of.
	buf, err := ioutil.ReadAll(anonReq.Body)
	if err != nil {
		t.Fatal(failTestStr(anonTestStr, err.Error()))
	}

	// creating 2 read closer (to set as request body) from the body content.
	readerOne := ioutil.NopCloser(bytes.NewBuffer(buf))
	readerTwo := ioutil.NopCloser(bytes.NewBuffer(buf))
	readerThree := ioutil.NopCloser(bytes.NewBuffer(buf))

	anonReq.Body = readerOne

	// call the HTTP handler.
	apiRouter.ServeHTTP(rec, anonReq)

	// expected error response when the unsigned HTTP request is not permitted.
	accesDeniedHTTPStatus := getAPIError(ErrAccessDenied).HTTPStatusCode
	if rec.Code != accesDeniedHTTPStatus {
		t.Fatal(failTestStr(anonTestStr, fmt.Sprintf("Object API Nil Test expected to fail with %d, but failed with %d", accesDeniedHTTPStatus, rec.Code)))
	}

	// expected error response in bytes when objectLayer is not initialized, or set to `nil`.
	expectedErrResponse := encodeResponse(getAPIErrorResponse(getAPIError(ErrAccessDenied), getGetObjectURL("", bucketName, objectName)))

	// HEAD HTTTP request doesn't contain response body.
	if anonReq.Method != "HEAD" {
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatal(failTestStr(anonTestStr, fmt.Sprintf("Failed parsing response body: <ERROR> %v", err)))
		}
		// verify whether actual error response (from the response body), matches the expected error response.
		if !bytes.Equal(expectedErrResponse, actualContent) {
			t.Fatal(failTestStr(anonTestStr, "error response content differs from expected value"))
		}
	}
	// Set write only policy on bucket to allow anonymous HTTP request for the operation under test.
	// request to go through.
	policy := bucketPolicy{
		Version:    "1.0",
		Statements: []policyStatement{policyFunc(bucketName, "")},
	}

	globalBucketPolicies.SetBucketPolicy(bucketName, policyChange{false, &policy})
	// now call the handler again with the unsigned/anonymous request, it should be accepted.
	rec = httptest.NewRecorder()

	anonReq.Body = readerTwo

	apiRouter.ServeHTTP(rec, anonReq)

	var expectedHTTPStatus int
	// expectedHTTPStatus returns 204 (http.StatusNoContent) on success.
	if testName == "TestAPIDeleteObjectHandler" || testName == "TestAPIAbortMultipartHandler" {
		expectedHTTPStatus = http.StatusNoContent
	} else if strings.Contains(testName, "BucketPolicyHandler") || testName == "ListBucketsHandler" {
		// BucketPolicyHandlers and `ListBucketsHandler` doesn't support anonymous request, policy changes should allow unsigned requests.
		expectedHTTPStatus = http.StatusForbidden
	} else {
		// other API handlers return 200OK on success.
		expectedHTTPStatus = http.StatusOK
	}

	// compare the HTTP response status code with the expected one.
	if rec.Code != expectedHTTPStatus {
		t.Fatal(failTestStr(anonTestStr, fmt.Sprintf("Expected the anonymous HTTP request to be served after the policy changes\n,Expected response HTTP status code to be %d, got %d",
			expectedHTTPStatus, rec.Code)))
	}

	// test for unknown auth case.
	anonReq.Body = readerThree
	// Setting the `Authorization` header to a random value so that the signature falls into unknown auth case.
	anonReq.Header.Set("Authorization", "nothingElse")
	// initialize new response recorder.
	rec = httptest.NewRecorder()
	// call the handler using the HTTP Request.
	apiRouter.ServeHTTP(rec, anonReq)
	// verify the response body for `ErrAccessDenied` message =.
	if anonReq.Method != "HEAD" {
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatal(failTestStr(unknownSignTestStr, fmt.Sprintf("Failed parsing response body: <ERROR> %v", err)))
		}
		// verify whether actual error response (from the response body), matches the expected error response.
		if !bytes.Equal(expectedErrResponse, actualContent) {
			fmt.Println(string(expectedErrResponse))
			fmt.Println(string(actualContent))
			t.Fatal(failTestStr(unknownSignTestStr, "error response content differs from expected value"))
		}
	}

	if rec.Code != accesDeniedHTTPStatus {
		t.Fatal(failTestStr(unknownSignTestStr, fmt.Sprintf("Object API Unknow auth test for \"%s\", expected to fail with %d, but failed with %d", testName, accesDeniedHTTPStatus, rec.Code)))
	}
}

// ExecObjectLayerAPINilTest - Sets the object layer to `nil`, and calls rhe registered object layer API endpoint,
// and assert the error response. The purpose is to validate the API handlers response when the object layer is uninitialized.
// Usage hint: Should be used at the end of the API end points tests (ex: check the last few lines of `testAPIListObjectPartsHandler`),
// need a sample HTTP request to be sent as argument so that the relevant handler is called, the handler registration is expected
// to be done since its called from within the API handler tests, the reference to the registered HTTP handler has to be sent
// as an argument.
func ExecObjectLayerAPINilTest(t TestErrHandler, bucketName, objectName, instanceType string, apiRouter http.Handler, req *http.Request) {
	// httptest Recorder to capture all the response by the http handler.
	rec := httptest.NewRecorder()

	// The  API handler gets the referece to the object layer via the global object Layer,
	// setting it to `nil` in order test for handlers response for uninitialized object layer.
	globalObjLayerMutex.Lock()
	globalObjectAPI = nil
	globalObjLayerMutex.Unlock()

	// call the HTTP handler.
	apiRouter.ServeHTTP(rec, req)

	// expected error response when the API handler is called before the object layer is initialized,
	// or when objectLayer is `nil`.
	serverNotInitializedErr := getAPIError(ErrServerNotInitialized).HTTPStatusCode
	if rec.Code != serverNotInitializedErr {
		t.Errorf("Object API Nil Test expected to fail with %d, but failed with %d", serverNotInitializedErr, rec.Code)
	}
	// expected error response in bytes when objectLayer is not initialized, or set to `nil`.
	expectedErrResponse := encodeResponse(getAPIErrorResponse(getAPIError(ErrServerNotInitialized),
		getGetObjectURL("", bucketName, objectName)))

	// HEAD HTTP Request doesn't contain body in its response,
	// for other type of HTTP requests compare the response body content with the expected one.
	if req.Method != "HEAD" {
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Minio %s: Failed parsing response body: <ERROR> %v", instanceType, err)
		}
		// verify whether actual error response (from the response body), matches the expected error response.
		if !bytes.Equal(expectedErrResponse, actualContent) {
			t.Errorf("Minio %s: Object content differs from expected value", instanceType)
		}
	}
}

// ExecObjectLayerAPITest - executes object layer API tests.
// Creates single node and XL ObjectLayer instance, registers the specified API end points and runs test for both the layers.
func ExecObjectLayerAPITest(t *testing.T, objAPITest objAPITestType, endpoints []string) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified value.
	resetTestGlobals()

	// initialize NSLock.
	initNSLock(false)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}
	bucketFS, fsAPIRouter, err := initAPIHandlerTest(objLayer, endpoints)
	if err != nil {
		t.Fatalf("Initialzation of API handler tests failed: <ERROR> %s", err)
	}
	credentials := serverConfig.GetCredential()
	// Executing the object layer tests for single node setup.
	objAPITest(objLayer, FSTestStr, bucketFS, fsAPIRouter, credentials, t)

	objLayer, xlDisks, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	bucketXL, xlAPIRouter, err := initAPIHandlerTest(objLayer, endpoints)
	if err != nil {
		t.Fatalf("Initialzation of API handler tests failed: <ERROR> %s", err)
	}
	// Executing the object layer tests for XL.
	objAPITest(objLayer, XLTestStr, bucketXL, xlAPIRouter, credentials, t)
	// clean up the temporary test backend.
	removeRoots(append(xlDisks, fsDir, rootPath))
}

// function to be passed to ExecObjectLayerAPITest, for executing object layr API handler tests.
type objAPITestType func(obj ObjectLayer, instanceType string, bucketName string,
	apiRouter http.Handler, credentials credential, t *testing.T)

// Regular object test type.
type objTestType func(obj ObjectLayer, instanceType string, t TestErrHandler)

// Special object test type for disk not found situations.
type objTestDiskNotFoundType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerTest - executes object layer tests.
// Creates single node and XL ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTest(t TestErrHandler, objTest objTestType) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	defer os.RemoveAll(rootPath)

	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}

	// Executing the object layer tests for single node setup.
	objTest(objLayer, FSTestStr, t)

	objLayer, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, XLTestStr, t)
	defer removeRoots(append(fsDirs, fsDir))
}

// ExecObjectLayerDiskAlteredTest - executes object layer tests while altering
// disks in between tests. Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerDiskAlteredTest(t *testing.T, objTest objTestDiskNotFoundType) {
	configPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Failed to create config directory", err)
	}
	defer os.RemoveAll(configPath)

	objLayer, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}

	// Executing the object layer tests for XL.
	objTest(objLayer, XLTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}

// Special object test type for stale files situations.
type objTestStaleFilesType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerStaleFilesTest - executes object layer tests those leaves stale
// files/directories under .minio/tmp.  Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerStaleFilesTest(t *testing.T, objTest objTestStaleFilesType) {
	configPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Failed to create config directory", err)
	}
	defer os.RemoveAll(configPath)

	nDisks := 16
	erasureDisks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatalf("Initialization of disks for XL setup: %s", err)
	}
	objLayer, _, err := initObjectLayer(mustGetNewEndpointList(erasureDisks...))
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, XLTestStr, erasureDisks, t)
	defer removeRoots(erasureDisks)
}

func registerBucketLevelFunc(bucket *router.Router, api objectAPIHandlers, apiFunctions ...string) {
	for _, apiFunction := range apiFunctions {
		switch apiFunction {
		case "PostPolicy":
			// Register PostPolicy handler.
			bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(api.PostPolicyBucketHandler)
		case "HeadObject":
			// Register HeadObject handler.
			bucket.Methods("Head").Path("/{object:.+}").HandlerFunc(api.HeadObjectHandler)
		case "GetObject":
			// Register GetObject handler.
			bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.GetObjectHandler)
		case "PutObject":
			// Register PutObject handler.
			bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectHandler)
		case "DeleteObject":
			// Register Delete Object handler.
			bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.DeleteObjectHandler)
		case "CopyObject":
			// Register Copy Object  handler.
			bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectHandler)
		case "PutBucketPolicy":
			// Register PutBucket Policy handler.
			bucket.Methods("PUT").HandlerFunc(api.PutBucketPolicyHandler).Queries("policy", "")
		case "DeleteBucketPolicy":
			// Register Delete bucket HTTP policy handler.
			bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketPolicyHandler).Queries("policy", "")
		case "GetBucketPolicy":
			// Register Get Bucket policy HTTP Handler.
			bucket.Methods("GET").HandlerFunc(api.GetBucketPolicyHandler).Queries("policy", "")
		case "GetBucketLocation":
			// Register GetBucketLocation handler.
			bucket.Methods("GET").HandlerFunc(api.GetBucketLocationHandler).Queries("location", "")
		case "HeadBucket":
			// Register HeadBucket handler.
			bucket.Methods("HEAD").HandlerFunc(api.HeadBucketHandler)
		case "DeleteMultipleObjects":
			// Register DeleteMultipleObjects handler.
			bucket.Methods("POST").HandlerFunc(api.DeleteMultipleObjectsHandler).Queries("delete", "")
		case "NewMultipart":
			// Register New Multipart upload handler.
			bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.NewMultipartUploadHandler).Queries("uploads", "")
		case "CopyObjectPart":
			// Register CopyObjectPart handler.
			bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		case "PutObjectPart":
			// Register PutObjectPart handler.
			bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		case "ListObjectParts":
			// Register ListObjectParts handler.
			bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}")
		case "ListMultipartUploads":
			// Register ListMultipartUploads handler.
			bucket.Methods("GET").HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
		case "CompleteMultipart":
			// Register Complete Multipart Upload handler.
			bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
		case "AbortMultipart":
			// Register AbortMultipart Handler.
			bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
		case "GetBucketNotification":
			// Register GetBucketNotification Handler.
			bucket.Methods("GET").HandlerFunc(api.GetBucketNotificationHandler).Queries("notification", "")
		case "PutBucketNotification":
			// Register PutBucketNotification Handler.
			bucket.Methods("PUT").HandlerFunc(api.PutBucketNotificationHandler).Queries("notification", "")
		case "ListenBucketNotification":
			// Register ListenBucketNotification Handler.
			bucket.Methods("GET").HandlerFunc(api.ListenBucketNotificationHandler).Queries("events", "{events:.*}")
		}
	}
}

// registerAPIFunctions helper function to add API functions identified by name to the routers.
func registerAPIFunctions(muxRouter *router.Router, objLayer ObjectLayer, apiFunctions ...string) {
	if len(apiFunctions) == 0 {
		// Register all api endpoints by default.
		registerAPIRouter(muxRouter)
		return
	}
	// API Router.
	apiRouter := muxRouter.NewRoute().PathPrefix("/").Subrouter()
	// Bucket router.
	bucketRouter := apiRouter.PathPrefix("/{bucket}").Subrouter()

	// All object storage operations are registered as HTTP handlers on `objectAPIHandlers`.
	// When the handlers get a HTTP request they use the underlyting ObjectLayer to perform operations.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// Register ListBuckets	handler.
	apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
	// Register all bucket level handlers.
	registerBucketLevelFunc(bucketRouter, api, apiFunctions...)
}

// Takes in XL object layer, and the list of API end points to be tested/required, registers the API end points and returns the HTTP handler.
// Need isolated registration of API end points while writing unit tests for end points.
// All the API end points are registered only for the default case.
func initTestAPIEndPoints(objLayer ObjectLayer, apiFunctions []string) http.Handler {
	// initialize a new mux router.
	// goriilla/mux is the library used to register all the routes and handle them.
	muxRouter := router.NewRouter().SkipClean(true)
	if len(apiFunctions) > 0 {
		// Iterate the list of API functions requested for and register them in mux HTTP handler.
		registerAPIFunctions(muxRouter, objLayer, apiFunctions...)
		return muxRouter
	}
	registerAPIRouter(muxRouter)
	return muxRouter
}

// Initialize Web RPC Handlers for testing
func initTestWebRPCEndPoint(objLayer ObjectLayer) http.Handler {
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Initialize router.
	muxRouter := router.NewRouter().SkipClean(true)
	registerWebRouter(muxRouter)
	return muxRouter
}

// Initialize browser RPC endpoint.
func initTestBrowserPeerRPCEndPoint() http.Handler {
	// Initialize router.
	muxRouter := router.NewRouter().SkipClean(true)
	registerBrowserPeerRPCRouter(muxRouter)
	return muxRouter
}

func StartTestBrowserPeerRPCServer(t TestErrHandler, instanceType string) TestServer {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Create an instance of TestServer.
	testRPCServer := TestServer{}

	// Fetch credentials for the test server.
	credentials := serverConfig.GetCredential()

	testRPCServer.Root = root
	testRPCServer.AccessKey = credentials.AccessKey
	testRPCServer.SecretKey = credentials.SecretKey

	// Initialize and run the TestServer.
	testRPCServer.Server = httptest.NewServer(initTestBrowserPeerRPCEndPoint())
	return testRPCServer
}

func StartTestS3PeerRPCServer(t TestErrHandler) (TestServer, []string) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Create an instance of TestServer.
	testRPCServer := TestServer{}

	// Fetch credentials for the test server.
	credentials := serverConfig.GetCredential()

	testRPCServer.Root = root
	testRPCServer.AccessKey = credentials.AccessKey
	testRPCServer.SecretKey = credentials.SecretKey

	// init disks
	objLayer, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("%s", err)
	}
	// set object layer
	testRPCServer.Obj = objLayer
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Register router on a new mux
	muxRouter := router.NewRouter().SkipClean(true)
	err = registerS3PeerRPCRouter(muxRouter)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Initialize and run the TestServer.
	testRPCServer.Server = httptest.NewServer(muxRouter)
	return testRPCServer, fsDirs
}

// generateTLSCertKey creates valid key/cert with registered DNS or IP address
// depending on the passed parameter. That way, we can use tls config without
// passing InsecureSkipVerify flag.  This code is a simplified version of
// https://golang.org/src/crypto/tls/generate_cert.go
func generateTLSCertKey(host string) ([]byte, []byte, error) {
	validFor := 365 * 24 * time.Hour
	rsaBits := 2048

	if len(host) == 0 {
		return nil, nil, fmt.Errorf("Missing host parameter")
	}

	publicKey := func(priv interface{}) interface{} {
		switch k := priv.(type) {
		case *rsa.PrivateKey:
			return &k.PublicKey
		case *ecdsa.PrivateKey:
			return &k.PublicKey
		default:
			return nil
		}
	}

	pemBlockForKey := func(priv interface{}) *pem.Block {
		switch k := priv.(type) {
		case *rsa.PrivateKey:
			return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
		case *ecdsa.PrivateKey:
			b, err := x509.MarshalECPrivateKey(k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to marshal ECDSA private key: %v", err)
				os.Exit(2)
			}
			return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
		default:
			return nil
		}
	}

	var priv interface{}
	var err error
	priv, err = rsa.GenerateKey(crand.Reader, rsaBits)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %s", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validFor)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := crand.Int(crand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate serial number: %s", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	hosts := strings.Split(host, ",")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign

	derBytes, err := x509.CreateCertificate(crand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create certificate: %s", err)
	}

	certOut := bytes.NewBuffer([]byte{})
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyOut := bytes.NewBuffer([]byte{})
	pem.Encode(keyOut, pemBlockForKey(priv))

	return certOut.Bytes(), keyOut.Bytes(), nil
}

func mustGetNewEndpointList(args ...string) (endpoints EndpointList) {
	if len(args) == 1 {
		endpoint, err := NewEndpoint(args[0])
		fatalIf(err, "unable to create new endpoint")
		endpoints = append(endpoints, endpoint)
	} else {
		var err error
		endpoints, err = NewEndpointList(args...)
		fatalIf(err, "unable to create new endpoint list")
	}

	return endpoints
}

func getEndpointsLocalAddr(endpoints EndpointList) string {
	for _, endpoint := range endpoints {
		if endpoint.IsLocal && endpoint.Type() == URLEndpointType {
			return endpoint.Host
		}
	}

	return globalMinioHost + ":" + globalMinioPort
}

// fetches a random number between range min-max.
func getRandomRange(min, max int, seed int64) int {
	// special value -1 means no explicit seeding.
	if seed != -1 {
		rand.Seed(seed)
	}
	return rand.Intn(max-min) + min
}

// Randomizes the order of bytes in the byte array
// using Knuth Fisher-Yates shuffle algorithm.
func randomizeBytes(s []byte, seed int64) []byte {
	// special value -1 means no explicit seeding.
	if seed != -1 {
		rand.Seed(seed)
	}
	n := len(s)
	var j int
	for i := 0; i < n-1; i++ {
		j = i + rand.Intn(n-i)
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func TestToErrIsNil(t *testing.T) {
	if toObjectErr(nil) != nil {
		t.Errorf("Test expected to return nil, failed instead got a non-nil value %s", toObjectErr(nil))
	}
	if toStorageErr(nil) != nil {
		t.Errorf("Test expected to return nil, failed instead got a non-nil value %s", toStorageErr(nil))
	}
	if toAPIErrorCode(nil) != ErrNone {
		t.Errorf("Test expected error code to be ErrNone, failed instead provided %d", toAPIErrorCode(nil))
	}
	if s3ToObjectError(nil) != nil {
		t.Errorf("Test expected to return nil, failed instead got a non-nil value %s", s3ToObjectError(nil))
	}
	if azureToObjectError(nil) != nil {
		t.Errorf("Test expected to return nil, failed instead got a non-nil value %s", azureToObjectError(nil))
	}
	if gcsToObjectError(nil) != nil {
		t.Errorf("Test expected to return nil, failed instead got a non-nil value %s", gcsToObjectError(nil))
	}
}
