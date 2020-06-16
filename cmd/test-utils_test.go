/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"context"
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
	"encoding/xml"
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
	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	"github.com/minio/minio-go/v6/pkg/signer"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/hash"
)

// Tests should initNSLock only once.
func init() {
	// disable ENVs which interfere with tests.
	for _, env := range []string{
		crypto.EnvAutoEncryptionLegacy,
		crypto.EnvKMSAutoEncryption,
		config.EnvAccessKey,
		config.EnvAccessKeyOld,
		config.EnvSecretKey,
		config.EnvSecretKeyOld,
	} {
		os.Unsetenv(env)
	}

	// Set as non-distributed.
	globalIsDistErasure = false

	// Disable printing console messages during tests.
	color.Output = ioutil.Discard

	// Set system resources to maximum.
	setMaxResources()

	// Initialize globalConsoleSys system
	globalConsoleSys = NewConsoleLogger(context.Background())

	logger.Disable = true

	initHelp()

	resetTestGlobals()
	// Uncomment the following line to see trace logs during unit tests.
	// logger.AddTarget(console.New())
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
///  Authorization:
///
///      Is skipped for obvious reasons
///
var ignoredHeaders = map[string]bool{
	"Authorization": true,
	"User-Agent":    true,
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

func mustGetPutObjReader(t TestErrHandler, data io.Reader, size int64, md5hex, sha256hex string) *PutObjReader {
	hr, err := hash.NewReader(data, size, md5hex, sha256hex, size, globalCLIContext.StrictS3Compat)
	if err != nil {
		t.Fatal(err)
	}
	return NewPutObjReader(hr, nil, nil)
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
	obj, err := NewFSObjectLayer(fsDirs[0])
	if err != nil {
		return nil, "", err
	}
	return obj, fsDirs[0], nil
}

func prepareErasureSets32(ctx context.Context) (ObjectLayer, []string, error) {
	return prepareErasure(ctx, 32)
}

func prepareErasure(ctx context.Context, nDisks int) (ObjectLayer, []string, error) {
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		return nil, nil, err
	}
	obj, _, err := initObjectLayer(ctx, mustGetZoneEndpoints(fsDirs...))
	if err != nil {
		removeRoots(fsDirs)
		return nil, nil, err
	}
	return obj, fsDirs, nil
}

func prepareErasure16(ctx context.Context) (ObjectLayer, []string, error) {
	return prepareErasure(ctx, 16)
}

// Initialize FS objects.
func initFSObjects(disk string, t *testing.T) (obj ObjectLayer) {
	var err error
	obj, err = NewFSObjectLayer(disk)
	if err != nil {
		t.Fatal(err)
	}
	newTestConfig(globalMinioDefaultRegion, obj)
	return obj
}

// TestErrHandler - Go testing.T satisfy this interface.
// This makes it easy to run the TestServer from any of the tests.
// Using this interface, functionalities to be used in tests can be
// made generalized, and can be integrated in benchmarks/unit tests/go check suite tests.
type TestErrHandler interface {
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

const (
	// FSTestStr is the string which is used as notation for Single node ObjectLayer in the unit tests.
	FSTestStr string = "FS"

	// ErasureTestStr is the string which is used as notation for Erasure ObjectLayer in the unit tests.
	ErasureTestStr string = "Erasure"

	// ErasureSetsTestStr is the string which is used as notation for Erasure sets object layer in the unit tests.
	ErasureSetsTestStr string = "ErasureSet"
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

// TestServer encapsulates an instantiation of a MinIO instance with a temporary backend.
// Example usage:
//   s := StartTestServer(t,"Erasure")
//   defer s.Stop()
type TestServer struct {
	Root      string
	Disks     EndpointZones
	AccessKey string
	SecretKey string
	Server    *httptest.Server
	Obj       ObjectLayer
	cancel    context.CancelFunc
}

// UnstartedTestServer - Configures a temp FS/Erasure backend,
// initializes the endpoints and configures the test server.
// The server should be started using the Start() method.
func UnstartedTestServer(t TestErrHandler, instanceType string) TestServer {
	ctx, cancel := context.WithCancel(context.Background())
	// create an instance of TestServer.
	testServer := TestServer{cancel: cancel}
	// return FS/Erasure object layer and temp backend.
	objLayer, disks, err := prepareTestBackend(ctx, instanceType)
	if err != nil {
		t.Fatal(err)
	}

	// set the server configuration.
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("%s", err)
	}

	// Test Server needs to start before formatting of disks.
	// Get credential.
	credentials := globalActiveCred

	testServer.Obj = objLayer
	testServer.Disks = mustGetZoneEndpoints(disks...)
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

	newAllSubsystems()

	initAllSubsystems(ctx, objLayer)

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

// Sets the global config path to empty string.
func resetGlobalConfigPath() {
	globalConfigDir = &ConfigDir{path: ""}
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
	globalServerConfigMu.Lock()
	// Save the loaded config globally.
	globalServerConfig = nil
	globalServerConfigMu.Unlock()
}

func resetGlobalEndpoints() {
	globalEndpoints = EndpointZones{}
}

func resetGlobalIsErasure() {
	globalIsErasure = false
}

// reset global heal state
func resetGlobalHealState() {
	// Init global heal state
	if globalAllHealState == nil {
		globalAllHealState = initHealState()
	} else {
		globalAllHealState.Lock()
		for _, v := range globalAllHealState.healSeqMap {
			if !v.hasEnded() {
				v.stop()
			}
		}
		globalAllHealState.Unlock()
	}

	// Init background heal state
	if globalBackgroundHealState == nil {
		globalBackgroundHealState = initHealState()
	} else {
		globalBackgroundHealState.Lock()
		for _, v := range globalBackgroundHealState.healSeqMap {
			if !v.hasEnded() {
				v.stop()
			}
		}
		globalBackgroundHealState.Unlock()
	}
}

// sets globalIAMSys to `nil`.
func resetGlobalIAMSys() {
	globalIAMSys = nil
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
	// Reset global endpoints.
	resetGlobalEndpoints()
	// Reset global isErasure flag.
	resetGlobalIsErasure()
	// Reset global heal state
	resetGlobalHealState()
	// Reset globalIAMSys to `nil`
	resetGlobalIAMSys()
}

// Configure the server for the test run.
func newTestConfig(bucketLocation string, obj ObjectLayer) (err error) {
	// Initialize server config.
	if err = newSrvConfig(obj); err != nil {
		return err
	}

	globalActiveCred = auth.Credentials{
		AccessKey: auth.DefaultAccessKey,
		SecretKey: auth.DefaultSecretKey,
	}

	globalConfigEncrypted = true

	// Set a default region.
	config.SetRegion(globalServerConfig, bucketLocation)

	// Save config.
	return saveServerConfig(context.Background(), obj, globalServerConfig)
}

// Deleting the temporary backend and stopping the server.
func (testServer TestServer) Stop() {
	testServer.cancel()
	testServer.Server.Close()
	os.RemoveAll(testServer.Root)
	for _, ep := range testServer.Disks {
		for _, disk := range ep.Endpoints {
			os.RemoveAll(disk.Path)
		}
	}
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
	canonicalURI := s3utils.EncodePath(req.URL.Path)

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
		string(serviceS3),
		"aws4_request",
	}, SlashSeparator)

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	region := sumHMAC(date, []byte(globalMinioDefaultRegion))
	service := sumHMAC(region, []byte(string(serviceS3)))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + SlashSeparator + scope,
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

	regionStr := globalServerRegion
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
			string(serviceS3),
			"aws4_request",
		}, SlashSeparator)

		stringToSign := "AWS4-HMAC-SHA256-PAYLOAD" + "\n"
		stringToSign = stringToSign + currTime.Format(iso8601Format) + "\n"
		stringToSign = stringToSign + scope + "\n"
		stringToSign = stringToSign + signature + "\n"
		stringToSign = stringToSign + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" + "\n" // hex(sum256(""))
		stringToSign = stringToSign + getSHA256Hash(buffer[:n])

		date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
		region := sumHMAC(date, []byte(regionStr))
		service := sumHMAC(region, []byte(serviceS3))
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

	region := globalServerRegion
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
	signingKey := getSigningKey(secretAccessKey, date, region, serviceS3)
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
	signer.SignV2(*req, accessKey, secretKey, false)
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

	region := globalServerRegion

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
	canonicalURI := s3utils.EncodePath(req.URL.Path)

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
		string(serviceS3),
		"aws4_request",
	}, SlashSeparator)

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	regionHMAC := sumHMAC(date, []byte(region))
	service := sumHMAC(regionHMAC, []byte(serviceS3))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + SlashSeparator + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return nil
}

// getCredentialString generate a credential string.
func getCredentialString(accessKeyID, location string, t time.Time) string {
	return accessKeyID + SlashSeparator + getScope(t, location)
}

// getMD5HashBase64 returns MD5 hash in base64 encoding of given data.
func getMD5HashBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(getMD5Sum(data))
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
		return newTestSignedRequestV2(method, urlStr, contentLength, body, accessKey, secretKey, nil)
	}
	return newTestSignedRequestV4(method, urlStr, contentLength, body, accessKey, secretKey, nil)
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
func newTestSignedRequestV2(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string, headers map[string]string) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}
	req.Header.Del("x-amz-content-sha256")

	// Anonymous request return quickly.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	err = signRequestV2(req, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// Returns new HTTP request object signed with signature v4.
func newTestSignedRequestV4(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string, headers map[string]string) (*http.Request, error) {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		return nil, err
	}

	// Anonymous request return quickly.
	if accessKey == "" || secretKey == "" {
		return req, nil
	}

	for k, v := range headers {
		req.Header.Add(k, v)
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
	req.Header.Set("User-Agent", "Mozilla")
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
	urlStr := endPoint + SlashSeparator
	if bucketName != "" {
		urlStr = urlStr + bucketName + SlashSeparator
	}
	if objectName != "" {
		urlStr = urlStr + s3utils.EncodePath(objectName)
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

// return URL For set/get lifecycle of the bucket.
func getBucketLifecycleURL(endPoint, bucketName string) (ret string) {
	queryValue := url.Values{}
	queryValue.Set("lifecycle", "")
	return makeTestTargetURL(endPoint, bucketName, "", queryValue)
}

// return URL for listing objects in the bucket with V1 legacy API.
func getListObjectsV1URL(endPoint, bucketName, prefix, maxKeys, encodingType string) string {
	queryValue := url.Values{}
	if maxKeys != "" {
		queryValue.Set("max-keys", maxKeys)
	}
	if encodingType != "" {
		queryValue.Set("encoding-type", encodingType)
	}
	return makeTestTargetURL(endPoint, bucketName, prefix, queryValue)
}

// return URL for listing objects in the bucket with V2 API.
func getListObjectsV2URL(endPoint, bucketName, prefix, maxKeys, fetchOwner, encodingType string) string {
	queryValue := url.Values{}
	queryValue.Set("list-type", "2") // Enables list objects V2 URL.
	if maxKeys != "" {
		queryValue.Set("max-keys", maxKeys)
	}
	if fetchOwner != "" {
		queryValue.Set("fetch-owner", fetchOwner)
	}
	if encodingType != "" {
		queryValue.Set("encoding-type", encodingType)
	}
	return makeTestTargetURL(endPoint, bucketName, prefix, queryValue)
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

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newTestObjectLayer(ctx context.Context, endpointZones EndpointZones) (newObject ObjectLayer, err error) {
	// For FS only, directly use the disk.
	if endpointZones.NEndpoints() == 1 {
		// Initialize new FS object layer.
		return NewFSObjectLayer(endpointZones[0].Endpoints[0].Path)
	}

	z, err := newErasureZones(ctx, endpointZones)
	if err != nil {
		return nil, err
	}

	newAllSubsystems()

	initAllSubsystems(ctx, z)

	return z, nil
}

// initObjectLayer - Instantiates object layer and returns it.
func initObjectLayer(ctx context.Context, endpointZones EndpointZones) (ObjectLayer, []StorageAPI, error) {
	objLayer, err := newTestObjectLayer(ctx, endpointZones)
	if err != nil {
		return nil, nil, err
	}

	var formattedDisks []StorageAPI
	// Should use the object layer tests for validating cache.
	if z, ok := objLayer.(*erasureZones); ok {
		formattedDisks = z.zones[0].GetDisks(0)()
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

// creates a bucket for the tests and returns the bucket name.
// initializes the specified API endpoints for the tests.
// initialies the root and returns its path.
// return credentials.
func initAPIHandlerTest(obj ObjectLayer, endpoints []string) (string, http.Handler, error) {
	newAllSubsystems()

	initAllSubsystems(context.Background(), obj)

	// get random bucket name.
	bucketName := getRandomBucketName()

	// Create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		// failed to create newbucket, return err.
		return "", nil, err
	}
	// Register the API end points with Erasure object layer.
	// Registering only the GetObject handler.
	apiRouter := initTestAPIEndPoints(obj, endpoints)
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RequestURI = r.URL.RequestURI()
		apiRouter.ServeHTTP(w, r)
	})
	return bucketName, f, nil
}

// prepare test backend.
// create FS/Erasure/ErasureSet backend.
// return object layer, backend disks.
func prepareTestBackend(ctx context.Context, instanceType string) (ObjectLayer, []string, error) {
	switch instanceType {
	// Total number of disks for Erasure sets backend is set to 32.
	case ErasureSetsTestStr:
		return prepareErasureSets32(ctx)
	// Total number of disks for Erasure backend is set to 16.
	case ErasureTestStr:
		return prepareErasure16(ctx)
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
func ExecObjectLayerAPIAnonTest(t *testing.T, obj ObjectLayer, testName, bucketName, objectName, instanceType string, apiRouter http.Handler,
	anonReq *http.Request, bucketPolicy *policy.Policy) {

	anonTestStr := "Anonymous HTTP request test"
	unknownSignTestStr := "Unknown HTTP signature test"

	// simple function which returns a message which gives the context of the test
	// and then followed by the the actual error message.
	failTestStr := func(testType, failMsg string) string {
		return fmt.Sprintf("MinIO %s: %s fail for \"%s\": \n<Error> %s", instanceType, testType, testName, failMsg)
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

	anonReq.Body = readerOne

	// call the HTTP handler.
	apiRouter.ServeHTTP(rec, anonReq)

	// expected error response when the unsigned HTTP request is not permitted.
	accessDenied := getAPIError(ErrAccessDenied).HTTPStatusCode
	if rec.Code != accessDenied {
		t.Fatal(failTestStr(anonTestStr, fmt.Sprintf("Object API Nil Test expected to fail with %d, but failed with %d", accessDenied, rec.Code)))
	}

	// HEAD HTTTP request doesn't contain response body.
	if anonReq.Method != "HEAD" {
		// read the response body.
		var actualContent []byte
		actualContent, err = ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatal(failTestStr(anonTestStr, fmt.Sprintf("Failed parsing response body: <ERROR> %v", err)))
		}

		actualError := &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Fatal(failTestStr(anonTestStr, "error response failed to parse error XML"))
		}

		if actualError.BucketName != bucketName {
			t.Fatal(failTestStr(anonTestStr, "error response bucket name differs from expected value"))
		}

		if actualError.Key != objectName {
			t.Fatal(failTestStr(anonTestStr, "error response object name differs from expected value"))
		}
	}

	// test for unknown auth case.
	anonReq.Body = readerTwo
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

		actualError := &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Fatal(failTestStr(unknownSignTestStr, "error response failed to parse error XML"))
		}

		if actualError.BucketName != bucketName {
			t.Fatal(failTestStr(unknownSignTestStr, "error response bucket name differs from expected value"))
		}

		if actualError.Key != objectName {
			t.Fatal(failTestStr(unknownSignTestStr, "error response object name differs from expected value"))
		}
	}

	// expected error response when the unsigned HTTP request is not permitted.
	unsupportedSignature := getAPIError(ErrSignatureVersionNotSupported).HTTPStatusCode
	if rec.Code != unsupportedSignature {
		t.Fatal(failTestStr(unknownSignTestStr, fmt.Sprintf("Object API Unknow auth test for \"%s\", expected to fail with %d, but failed with %d", testName, unsupportedSignature, rec.Code)))
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

	// HEAD HTTP Request doesn't contain body in its response,
	// for other type of HTTP requests compare the response body content with the expected one.
	if req.Method != "HEAD" {
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("MinIO %s: Failed parsing response body: <ERROR> %v", instanceType, err)
		}

		actualError := &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Errorf("MinIO %s: error response failed to parse error XML", instanceType)
		}

		if actualError.BucketName != bucketName {
			t.Errorf("MinIO %s: error response bucket name differs from expected value", instanceType)
		}

		if actualError.Key != objectName {
			t.Errorf("MinIO %s: error response object name differs from expected value", instanceType)
		}
	}
}

// ExecObjectLayerAPITest - executes object layer API tests.
// Creates single node and Erasure ObjectLayer instance, registers the specified API end points and runs test for both the layers.
func ExecObjectLayerAPITest(t *testing.T, objAPITest objAPITestType, endpoints []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// reset globals.
	// this is to make sure that the tests are not affected by modified value.
	resetTestGlobals()

	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}

	bucketFS, fsAPIRouter, err := initAPIHandlerTest(objLayer, endpoints)
	if err != nil {
		t.Fatalf("Initialization of API handler tests failed: <ERROR> %s", err)
	}

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}

	credentials := globalActiveCred

	// Executing the object layer tests for single node setup.
	objAPITest(objLayer, FSTestStr, bucketFS, fsAPIRouter, credentials, t)

	objLayer, erasureDisks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}

	bucketErasure, erAPIRouter, err := initAPIHandlerTest(objLayer, endpoints)
	if err != nil {
		t.Fatalf("Initialzation of API handler tests failed: <ERROR> %s", err)
	}
	// Executing the object layer tests for Erasure.
	objAPITest(objLayer, ErasureTestStr, bucketErasure, erAPIRouter, credentials, t)

	// clean up the temporary test backend.
	removeRoots(append(erasureDisks, fsDir))
}

// function to be passed to ExecObjectLayerAPITest, for executing object layr API handler tests.
type objAPITestType func(obj ObjectLayer, instanceType string, bucketName string,
	apiRouter http.Handler, credentials auth.Credentials, t *testing.T)

// Regular object test type.
type objTestType func(obj ObjectLayer, instanceType string, t TestErrHandler)

// Special test type for test with directories
type objTestTypeWithDirs func(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler)

// Special object test type for disk not found situations.
type objTestDiskNotFoundType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerTest - executes object layer tests.
// Creates single node and Erasure ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTest(t TestErrHandler, objTest objTestType) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err)
	}

	newAllSubsystems()

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatal("Unexpected error", err)
	}

	initAllSubsystems(ctx, objLayer)

	// Executing the object layer tests for single node setup.
	objTest(objLayer, FSTestStr, t)

	newAllSubsystems()

	objLayer, fsDirs, err := prepareErasureSets32(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}

	initAllSubsystems(ctx, objLayer)

	defer removeRoots(append(fsDirs, fsDir))
	// Executing the object layer tests for Erasure.
	objTest(objLayer, ErasureTestStr, t)
}

// ExecObjectLayerTestWithDirs - executes object layer tests.
// Creates single node and Erasure ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTestWithDirs(t TestErrHandler, objTest objTestTypeWithDirs) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objLayer, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatal("Unexpected error", err)
	}

	// Executing the object layer tests for Erasure.
	objTest(objLayer, ErasureTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}

// ExecObjectLayerDiskAlteredTest - executes object layer tests while altering
// disks in between tests. Creates Erasure ObjectLayer instance and runs test for Erasure layer.
func ExecObjectLayerDiskAlteredTest(t *testing.T, objTest objTestDiskNotFoundType) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objLayer, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatal("Failed to create config directory", err)
	}

	// Executing the object layer tests for Erasure.
	objTest(objLayer, ErasureTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}

// Special object test type for stale files situations.
type objTestStaleFilesType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerStaleFilesTest - executes object layer tests those leaves stale
// files/directories under .minio/tmp.  Creates Erasure ObjectLayer instance and runs test for Erasure layer.
func ExecObjectLayerStaleFilesTest(t *testing.T, objTest objTestStaleFilesType) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDisks := 16
	erasureDisks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatalf("Initialization of disks for Erasure setup: %s", err)
	}
	objLayer, _, err := initObjectLayer(ctx, mustGetZoneEndpoints(erasureDisks...))
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatal("Failed to create config directory", err)
	}

	// Executing the object layer tests for Erasure.
	objTest(objLayer, ErasureTestStr, erasureDisks, t)
	defer removeRoots(erasureDisks)
}

func registerBucketLevelFunc(bucket *mux.Router, api objectAPIHandlers, apiFunctions ...string) {
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
		case "GetBucketLifecycle":
			bucket.Methods("GET").HandlerFunc(api.GetBucketLifecycleHandler).Queries("lifecycle", "")
		case "PutBucketLifecycle":
			bucket.Methods("PUT").HandlerFunc(api.PutBucketLifecycleHandler).Queries("lifecycle", "")
		case "DeleteBucketLifecycle":
			bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketLifecycleHandler).Queries("lifecycle", "")
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
func registerAPIFunctions(muxRouter *mux.Router, objLayer ObjectLayer, apiFunctions ...string) {
	if len(apiFunctions) == 0 {
		// Register all api endpoints by default.
		registerAPIRouter(muxRouter, true, false)
		return
	}
	// API Router.
	apiRouter := muxRouter.PathPrefix(SlashSeparator).Subrouter()
	// Bucket router.
	bucketRouter := apiRouter.PathPrefix("/{bucket}").Subrouter()

	// All object storage operations are registered as HTTP handlers on `objectAPIHandlers`.
	// When the handlers get a HTTP request they use the underlying ObjectLayer to perform operations.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// When cache is enabled, Put and Get operations are passed
	// to underlying cache layer to manage object layer operation and disk caching
	// operation
	api := objectAPIHandlers{
		ObjectAPI: func() ObjectLayer {
			if !globalSafeMode {
				return globalObjectAPI
			}
			return nil
		},
		CacheAPI: func() CacheObjectLayer {
			if !globalSafeMode {
				return globalCacheObjectAPI
			}
			return nil
		},
		EncryptionEnabled: func() bool { return true },
	}

	// Register ListBuckets	handler.
	apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
	// Register all bucket level handlers.
	registerBucketLevelFunc(bucketRouter, api, apiFunctions...)
}

// Takes in Erasure object layer, and the list of API end points to be tested/required, registers the API end points and returns the HTTP handler.
// Need isolated registration of API end points while writing unit tests for end points.
// All the API end points are registered only for the default case.
func initTestAPIEndPoints(objLayer ObjectLayer, apiFunctions []string) http.Handler {
	// initialize a new mux router.
	// goriilla/mux is the library used to register all the routes and handle them.
	muxRouter := mux.NewRouter().SkipClean(true)
	if len(apiFunctions) > 0 {
		// Iterate the list of API functions requested for and register them in mux HTTP handler.
		registerAPIFunctions(muxRouter, objLayer, apiFunctions...)
		return muxRouter
	}
	registerAPIRouter(muxRouter, true, false)
	return muxRouter
}

// Initialize Web RPC Handlers for testing
func initTestWebRPCEndPoint(objLayer ObjectLayer) http.Handler {
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Initialize router.
	muxRouter := mux.NewRouter().SkipClean(true)
	registerWebRouter(muxRouter)
	return muxRouter
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
		return nil, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validFor)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := crand.Int(crand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate serial number: %w", err)
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
		return nil, nil, fmt.Errorf("Failed to create certificate: %w", err)
	}

	certOut := bytes.NewBuffer([]byte{})
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyOut := bytes.NewBuffer([]byte{})
	pem.Encode(keyOut, pemBlockForKey(priv))

	return certOut.Bytes(), keyOut.Bytes(), nil
}

func mustGetZoneEndpoints(args ...string) EndpointZones {
	endpoints := mustGetNewEndpoints(args...)
	return []ZoneEndpoints{{
		SetCount:     1,
		DrivesPerSet: len(args),
		Endpoints:    endpoints,
	}}
}

func mustGetNewEndpoints(args ...string) (endpoints Endpoints) {
	endpoints, err := NewEndpoints(args...)
	logger.FatalIf(err, "unable to create new endpoint list")
	return endpoints
}

func getEndpointsLocalAddr(endpointZones EndpointZones) string {
	for _, endpoints := range endpointZones {
		for _, endpoint := range endpoints.Endpoints {
			if endpoint.IsLocal && endpoint.Type() == URLEndpointType {
				return endpoint.Host
			}
		}
	}

	return net.JoinHostPort(globalMinioHost, globalMinioPort)
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
	ctx := context.Background()
	if toAPIError(ctx, nil) != noError {
		t.Errorf("Test expected error code to be ErrNone, failed instead provided %s", toAPIError(ctx, nil).Code)
	}
}

// Uploads an object using DummyDataGen directly via the http
// handler. Each part in a multipart object is a new DummyDataGen
// instance (so the part sizes are needed to reconstruct the whole
// object). When `len(partSizes) == 1`, asMultipart is used to upload
// the object as multipart with 1 part or as a regular single object.
//
// All upload failures are considered test errors - this function is
// intended as a helper for other tests.
func uploadTestObject(t *testing.T, apiRouter http.Handler, creds auth.Credentials, bucketName, objectName string,
	partSizes []int64, metadata map[string]string, asMultipart bool) {

	if len(partSizes) == 0 {
		t.Fatalf("Cannot upload an object without part sizes")
	}
	if len(partSizes) > 1 {
		asMultipart = true
	}

	checkRespErr := func(rec *httptest.ResponseRecorder, exp int) {
		if rec.Code != exp {
			b, err := ioutil.ReadAll(rec.Body)
			t.Fatalf("Expected: %v, Got: %v, Body: %s, err: %v", exp, rec.Code, string(b), err)
		}
	}

	if !asMultipart {
		srcData := NewDummyDataGen(partSizes[0], 0)
		req, err := newTestSignedRequestV4("PUT", getPutObjectURL("", bucketName, objectName),
			partSizes[0], srcData, creds.AccessKey, creds.SecretKey, metadata)
		if err != nil {
			t.Fatalf("Unexpected err: %#v", err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)
		checkRespErr(rec, http.StatusOK)
	} else {
		// Multipart upload - each part is a new DummyDataGen
		// (so the part lengths are required to verify the
		// object when reading).

		// Initiate mp upload
		reqI, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName),
			0, nil, creds.AccessKey, creds.SecretKey, metadata)
		if err != nil {
			t.Fatalf("Unexpected err: %#v", err)
		}
		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, reqI)
		checkRespErr(rec, http.StatusOK)
		decoder := xml.NewDecoder(rec.Body)
		multipartResponse := &InitiateMultipartUploadResponse{}
		err = decoder.Decode(multipartResponse)
		if err != nil {
			t.Fatalf("Error decoding the recorded response Body")
		}
		upID := multipartResponse.UploadID

		// Upload each part
		var cp []CompletePart
		cumulativeSum := int64(0)
		for i, partLen := range partSizes {
			partID := i + 1
			partSrc := NewDummyDataGen(partLen, cumulativeSum)
			cumulativeSum += partLen
			req, errP := newTestSignedRequestV4("PUT",
				getPutObjectPartURL("", bucketName, objectName, upID, fmt.Sprintf("%d", partID)),
				partLen, partSrc, creds.AccessKey, creds.SecretKey, metadata)
			if errP != nil {
				t.Fatalf("Unexpected err: %#v", errP)
			}
			rec = httptest.NewRecorder()
			apiRouter.ServeHTTP(rec, req)
			checkRespErr(rec, http.StatusOK)
			header := rec.Header()
			if v, ok := header["ETag"]; ok {
				etag := v[0]
				if etag == "" {
					t.Fatalf("Unexpected empty etag")
				}
				cp = append(cp, CompletePart{partID, etag[1 : len(etag)-1]})
			} else {
				t.Fatalf("Missing etag header")
			}
		}

		// Call CompleteMultipart API
		compMpBody, err := xml.Marshal(CompleteMultipartUpload{Parts: cp})
		if err != nil {
			t.Fatalf("Unexpected err: %#v", err)
		}
		reqC, errP := newTestSignedRequestV4("POST",
			getCompleteMultipartUploadURL("", bucketName, objectName, upID),
			int64(len(compMpBody)), bytes.NewReader(compMpBody),
			creds.AccessKey, creds.SecretKey, metadata)
		if errP != nil {
			t.Fatalf("Unexpected err: %#v", errP)
		}
		rec = httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, reqC)
		checkRespErr(rec, http.StatusOK)
	}
}
