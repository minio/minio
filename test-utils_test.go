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

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// The Argument to TestServer should satidy the interface.
// Golang Testing.T and Testing.B, and gocheck.C satisfy the interface.
// This makes it easy to run the TestServer from any of the tests.
type TestErrHandler interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

const (
	// singleNodeTestStr is the string which is used as notation for Single node ObjectLayer in the unit tests.
	singleNodeTestStr string = "SingleNode"
	// xLTestStr is the string which is used as notation for XL ObjectLayer in the unit tests.
	xLTestStr string = "XL"
)

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
}

// Starts the test server and returns the TestServer instance.
func StartTestServer(t TestErrHandler, instanceType string) TestServer {
	// create an instance of TestServer.
	testServer := TestServer{}
	// create temporary backend for the test server.
	erasureDisks, err := makeTestBackend(instanceType)
	if err != nil {
		t.Fatalf("Failed obtaining Temp XL layer: <ERROR> %s", err)
	}
	testServer.Disks = erasureDisks
	// Obtain temp root.
	root, err := getTestRoot()
	if err != nil {
		t.Fatalf("Failed obtaining Temp XL layer: <ERROR> %s", err)
	}
	testServer.Root = root
	testServer.Disks = erasureDisks
	// Initialize server config.
	initConfig()
	// Get credential.
	credentials := serverConfig.GetCredential()
	testServer.AccessKey = credentials.AccessKeyID
	testServer.SecretKey = credentials.SecretAccessKey
	// Set a default region.
	serverConfig.SetRegion("us-east-1")

	// Do this only once here.
	setGlobalConfigPath(root)

	err = serverConfig.Save()
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Run TestServer.
	testServer.Server = httptest.NewServer(configureServerHandler(serverCmdConfig{exportPaths: erasureDisks}))

	return testServer
}

// Deleting the temporary backend and stopping the server.
func (testServer TestServer) Stop() {
	removeAll(testServer.Root)
	for _, disk := range testServer.Disks {
		removeAll(disk)
	}
	testServer.Server.Close()
}

// used to formulate HTTP v4 signed HTTP request.
func newTestRequest(method, urlStr string, contentLength int64, body io.ReadSeeker, accessKey, secretKey string) (*http.Request, error) {
	if method == "" {
		method = "POST"
	}
	t := time.Now().UTC()

	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-amz-date", t.Format(iso8601Format))

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
		md5base64 := base64.StdEncoding.EncodeToString(sumMD5(payloadBytes))
		req.Header.Set("Content-Md5", md5base64)
	}
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
		// Add body
		req.Body = ioutil.NopCloser(body)
	}

	var headers []string
	vals := make(map[string][]string)
	for k, vv := range req.Header {
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // ignored header
		}
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)

	var canonicalHeaders bytes.Buffer
	for _, k := range headers {
		canonicalHeaders.WriteString(k)
		canonicalHeaders.WriteByte(':')
		switch {
		case k == "host":
			canonicalHeaders.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					canonicalHeaders.WriteByte(',')
				}
				canonicalHeaders.WriteString(v)
			}
			canonicalHeaders.WriteByte('\n')
		}
	}

	signedHeaders := strings.Join(headers, ";")

	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)
	encodedPath := getURLEncodedName(req.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)

	//
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
		encodedPath,
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		hashedPayload,
	}, "\n")

	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"us-east-1",
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
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

	return req, nil
}

// creates the temp backend setup.
// if the option is
// FS: Returns a temp single disk setup initializes FS Backend.
// XL: Returns a 16 temp single disk setup and initializse XL Backend.
func makeTestBackend(instanceType string) ([]string, error) {
	switch instanceType {
	case "FS":
		_, fsroot, err := getSingleNodeObjectLayer()
		if err != nil {
			return []string{}, err
		}
		return []string{fsroot}, err

	case "XL":
		_, erasureDisks, err := getXLObjectLayer()
		if err != nil {
			return []string{}, err
		}
		return erasureDisks, err
	default:
		errMsg := "Invalid instance type, Only FS and XL are valid options"
		return []string{}, fmt.Errorf("Failed obtaining Temp XL layer: <ERROR> %s", errMsg)
	}
}

// returns temp root directory. `
func getTestRoot() (string, error) {
	return ioutil.TempDir(os.TempDir(), "api-")
}

// getXLObjectLayer - Instantiates XL object layer and returns it.
func getXLObjectLayer() (ObjectLayer, []string, error) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		if err != nil {
			return nil, nil, err
		}
		erasureDisks = append(erasureDisks, path)
	}

	// Initialize name space lock.
	initNSLock()

	objLayer, err := newXLObjects(erasureDisks)
	if err != nil {
		return nil, nil, err
	}
	return objLayer, erasureDisks, nil
}

// getSingleNodeObjectLayer - Instantiates single node object layer and returns it.
func getSingleNodeObjectLayer() (ObjectLayer, string, error) {
	// Make a temporary directory to use as the obj.
	fsDir, err := ioutil.TempDir("", "minio-")
	if err != nil {
		return nil, "", err
	}

	// Initialize name space lock.
	initNSLock()

	// Create the obj.
	objLayer, err := newFSObjects(fsDir)
	if err != nil {
		return nil, "", err
	}
	return objLayer, fsDir, nil
}

// removeRoots - Cleans up initialized directories during tests.
func removeRoots(roots []string) {
	for _, root := range roots {
		removeAll(root)
	}
}

// removeRandomDisk - removes a count of random disks from a disk slice.
func removeRandomDisk(disks []string, removeCount int) {
	ints := randInts(len(disks))
	for _, i := range ints[:removeCount] {
		removeAll(disks[i-1])
	}
}

// Regular object test type.
type objTestType func(obj ObjectLayer, instanceType string, t *testing.T)

// Special object test type for disk not found situations.
type objTestDiskNotFoundType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerTest - executes object layer tests.
// Creates single node and XL ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTest(t *testing.T, objTest objTestType) {
	objLayer, fsDir, err := getSingleNodeObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err.Error())
	}
	// Executing the object layer tests for single node setup.
	objTest(objLayer, singleNodeTestStr, t)

	objLayer, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err.Error())
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, t)
	defer removeRoots(append(fsDirs, fsDir))
}

// ExecObjectLayerDiskNotFoundTest - executes object layer tests while deleting
// disks in between tests. Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerDiskNotFoundTest(t *testing.T, objTest objTestDiskNotFoundType) {
	objLayer, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err.Error())
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}
