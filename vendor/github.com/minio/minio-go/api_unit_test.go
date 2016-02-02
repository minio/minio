/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

package minio

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

type customReader struct{}

func (c *customReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (c *customReader) Size() (n int64) {
	return 10
}

// Tests getReaderSize() for various Reader types.
func TestGetReaderSize(t *testing.T) {
	var reader io.Reader
	size, err := getReaderSize(reader)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != -1 {
		t.Fatal("Reader shouldn't have any length.")
	}

	bytesReader := bytes.NewReader([]byte("Hello World"))
	size, err = getReaderSize(bytesReader)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != int64(len("Hello World")) {
		t.Fatalf("Reader length doesn't match got: %v, want: %v", size, len("Hello World"))
	}

	size, err = getReaderSize(new(customReader))
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != int64(10) {
		t.Fatalf("Reader length doesn't match got: %v, want: %v", size, 10)
	}

	stringsReader := strings.NewReader("Hello World")
	size, err = getReaderSize(stringsReader)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != int64(len("Hello World")) {
		t.Fatalf("Reader length doesn't match got: %v, want: %v", size, len("Hello World"))
	}

	// Create request channel.
	reqCh := make(chan readRequest)
	// Create response channel.
	resCh := make(chan readResponse)
	// Create done channel.
	doneCh := make(chan struct{})
	// objectInfo.
	objectInfo := ObjectInfo{Size: 10}
	objectReader := newObject(reqCh, resCh, doneCh, objectInfo)
	defer objectReader.Close()

	size, err = getReaderSize(objectReader)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != int64(10) {
		t.Fatalf("Reader length doesn't match got: %v, want: %v", size, 10)
	}

	fileReader, err := ioutil.TempFile(os.TempDir(), "prefix")
	if err != nil {
		t.Fatal("Error:", err)
	}
	defer fileReader.Close()
	defer os.RemoveAll(fileReader.Name())

	size, err = getReaderSize(fileReader)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size == -1 {
		t.Fatal("Reader length for file cannot be -1.")
	}

	// Verify for standard input, output and error file descriptors.
	size, err = getReaderSize(os.Stdin)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != -1 {
		t.Fatal("Stdin should have length of -1.")
	}
	size, err = getReaderSize(os.Stdout)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != -1 {
		t.Fatal("Stdout should have length of -1.")
	}
	size, err = getReaderSize(os.Stderr)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if size != -1 {
		t.Fatal("Stderr should have length of -1.")
	}
	file, err := os.Open(os.TempDir())
	if err != nil {
		t.Fatal("Error:", err)
	}
	defer file.Close()
	_, err = getReaderSize(file)
	if err == nil {
		t.Fatal("Input file as directory should throw an error.")
	}
}

// Tests valid hosts for location.
func TestValidBucketLocation(t *testing.T) {
	s3Hosts := []struct {
		bucketLocation string
		endpoint       string
	}{
		{"us-east-1", "s3.amazonaws.com"},
		{"unknown", "s3.amazonaws.com"},
		{"ap-southeast-1", "s3-ap-southeast-1.amazonaws.com"},
	}
	for _, s3Host := range s3Hosts {
		endpoint := getS3Endpoint(s3Host.bucketLocation)
		if endpoint != s3Host.endpoint {
			t.Fatal("Error: invalid bucket location", endpoint)
		}
	}
}

// Tests valid bucket names.
func TestBucketNames(t *testing.T) {
	buckets := []struct {
		name  string
		valid error
	}{
		{".mybucket", ErrInvalidBucketName("Bucket name cannot start or end with a '.' dot.")},
		{"mybucket.", ErrInvalidBucketName("Bucket name cannot start or end with a '.' dot.")},
		{"mybucket-", ErrInvalidBucketName("Bucket name contains invalid characters.")},
		{"my", ErrInvalidBucketName("Bucket name cannot be smaller than 3 characters.")},
		{"", ErrInvalidBucketName("Bucket name cannot be empty.")},
		{"my..bucket", ErrInvalidBucketName("Bucket name cannot have successive periods.")},
		{"my.bucket.com", nil},
		{"my-bucket", nil},
		{"123my-bucket", nil},
	}

	for _, b := range buckets {
		err := isValidBucketName(b.name)
		if err != b.valid {
			t.Fatal("Error:", err)
		}
	}
}

// Tests temp file.
func TestTempFile(t *testing.T) {
	tmpFile, err := newTempFile("testing")
	if err != nil {
		t.Fatal("Error:", err)
	}
	fileName := tmpFile.Name()
	// Closing temporary file purges the file.
	err = tmpFile.Close()
	if err != nil {
		t.Fatal("Error:", err)
	}
	st, err := os.Stat(fileName)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("Error:", err)
	}
	if err == nil && st != nil {
		t.Fatal("Error: file should be deleted and should not exist.")
	}
}

// Tests url encoding.
func TestEncodeURL2Path(t *testing.T) {
	type urlStrings struct {
		objName        string
		encodedObjName string
	}

	bucketName := "bucketName"
	want := []urlStrings{
		{
			objName:        "本語",
			encodedObjName: "%E6%9C%AC%E8%AA%9E",
		},
		{
			objName:        "本語.1",
			encodedObjName: "%E6%9C%AC%E8%AA%9E.1",
		},
		{
			objName:        ">123>3123123",
			encodedObjName: "%3E123%3E3123123",
		},
		{
			objName:        "test 1 2.txt",
			encodedObjName: "test%201%202.txt",
		},
		{
			objName:        "test++ 1.txt",
			encodedObjName: "test%2B%2B%201.txt",
		},
	}

	for _, o := range want {
		u, err := url.Parse(fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucketName, o.objName))
		if err != nil {
			t.Fatal("Error:", err)
		}
		urlPath := "/" + bucketName + "/" + o.encodedObjName
		if urlPath != encodeURL2Path(u) {
			t.Fatal("Error")
		}
	}
}

// Tests error response structure.
func TestErrorResponse(t *testing.T) {
	var err error
	err = ErrorResponse{
		Code: "Testing",
	}
	errResp := ToErrorResponse(err)
	if errResp.Code != "Testing" {
		t.Fatal("Type conversion failed, we have an empty struct.")
	}

	// Test http response decoding.
	var httpResponse *http.Response
	// Set empty variables
	httpResponse = nil
	var bucketName, objectName string

	// Should fail with invalid argument.
	err = httpRespToErrorResponse(httpResponse, bucketName, objectName)
	errResp = ToErrorResponse(err)
	if errResp.Code != "InvalidArgument" {
		t.Fatal("Empty response input should return invalid argument.")
	}
}

// Tests signature calculation.
func TestSignatureCalculation(t *testing.T) {
	req, err := http.NewRequest("GET", "https://s3.amazonaws.com", nil)
	if err != nil {
		t.Fatal("Error:", err)
	}
	req = signV4(*req, "", "", "us-east-1")
	if req.Header.Get("Authorization") != "" {
		t.Fatal("Error: anonymous credentials should not have Authorization header.")
	}

	req = preSignV4(*req, "", "", "us-east-1", 0)
	if strings.Contains(req.URL.RawQuery, "X-Amz-Signature") {
		t.Fatal("Error: anonymous credentials should not have Signature query resource.")
	}

	req = signV2(*req, "", "")
	if req.Header.Get("Authorization") != "" {
		t.Fatal("Error: anonymous credentials should not have Authorization header.")
	}

	req = preSignV2(*req, "", "", 0)
	if strings.Contains(req.URL.RawQuery, "Signature") {
		t.Fatal("Error: anonymous credentials should not have Signature query resource.")
	}

	req = signV4(*req, "ACCESS-KEY", "SECRET-KEY", "us-east-1")
	if req.Header.Get("Authorization") == "" {
		t.Fatal("Error: normal credentials should have Authorization header.")
	}

	req = preSignV4(*req, "ACCESS-KEY", "SECRET-KEY", "us-east-1", 0)
	if !strings.Contains(req.URL.RawQuery, "X-Amz-Signature") {
		t.Fatal("Error: normal credentials should have Signature query resource.")
	}

	req = signV2(*req, "ACCESS-KEY", "SECRET-KEY")
	if req.Header.Get("Authorization") == "" {
		t.Fatal("Error: normal credentials should have Authorization header.")
	}

	req = preSignV2(*req, "ACCESS-KEY", "SECRET-KEY", 0)
	if !strings.Contains(req.URL.RawQuery, "Signature") {
		t.Fatal("Error: normal credentials should not have Signature query resource.")
	}
}

// Tests signature type.
func TestSignatureType(t *testing.T) {
	clnt := Client{}
	if !clnt.signature.isV4() {
		t.Fatal("Error")
	}
	clnt.signature = SignatureV2
	if !clnt.signature.isV2() {
		t.Fatal("Error")
	}
	if clnt.signature.isV4() {
		t.Fatal("Error")
	}
	clnt.signature = SignatureV4
	if !clnt.signature.isV4() {
		t.Fatal("Error")
	}
}

// Tests bucket acl types.
func TestBucketACLTypes(t *testing.T) {
	want := map[string]bool{
		"private":            true,
		"public-read":        true,
		"public-read-write":  true,
		"authenticated-read": true,
		"invalid":            false,
	}
	for acl, ok := range want {
		if BucketACL(acl).isValidBucketACL() != ok {
			t.Fatal("Error")
		}
	}
}

// Tests optimal part size.
func TestPartSize(t *testing.T) {
	totalPartsCount, partSize, lastPartSize, err := optimalPartInfo(5000000000000000000)
	if err == nil {
		t.Fatal("Error: should fail")
	}
	totalPartsCount, partSize, lastPartSize, err = optimalPartInfo(5497558138880)
	if err != nil {
		t.Fatal("Error: ", err)
	}
	if totalPartsCount != 9987 {
		t.Fatalf("Error: expecting total parts count of 9987: got %v instead", totalPartsCount)
	}
	if partSize != 550502400 {
		t.Fatalf("Error: expecting part size of 550502400: got %v instead", partSize)
	}
	if lastPartSize != 241172480 {
		t.Fatalf("Error: expecting last part size of 241172480: got %v instead", lastPartSize)
	}
	totalPartsCount, partSize, lastPartSize, err = optimalPartInfo(5000000000)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if partSize != minPartSize {
		t.Fatalf("Error: expecting part size of %v: got %v instead", minPartSize, partSize)
	}
	totalPartsCount, partSize, lastPartSize, err = optimalPartInfo(-1)
	if err != nil {
		t.Fatal("Error:", err)
	}
	if totalPartsCount != 9987 {
		t.Fatalf("Error: expecting total parts count of 9987: got %v instead", totalPartsCount)
	}
	if partSize != 550502400 {
		t.Fatalf("Error: expecting part size of 550502400: got %v instead", partSize)
	}
	if lastPartSize != 241172480 {
		t.Fatalf("Error: expecting last part size of 241172480: got %v instead", lastPartSize)
	}
}

// Tests query values to URL encoding.
func TestQueryURLEncoding(t *testing.T) {
	urlValues := make(url.Values)
	urlValues.Set("prefix", "test@1123")
	urlValues.Set("delimiter", "/")
	urlValues.Set("marker", "%%%@$$$")

	queryStr := queryEncode(urlValues)
	if !strings.Contains(queryStr, "test%401123") {
		t.Fatalf("Error: @ should be encoded as %s, invalid query string %s", "test%401123", queryStr)
	}
	if !strings.Contains(queryStr, "%25%25%25%40%24%24%24") {
		t.Fatalf("Error: %s should be encoded as %s, invalid query string %s", "%%%@$$$", "%25%25%25%40%24%24%24", queryStr)
	}
}

// Tests url encoding.
func TestURLEncoding(t *testing.T) {
	type urlStrings struct {
		name        string
		encodedName string
	}

	want := []urlStrings{
		{
			name:        "bigfile-1._%",
			encodedName: "bigfile-1._%25",
		},
		{
			name:        "本語",
			encodedName: "%E6%9C%AC%E8%AA%9E",
		},
		{
			name:        "本語.1",
			encodedName: "%E6%9C%AC%E8%AA%9E.1",
		},
		{
			name:        ">123>3123123",
			encodedName: "%3E123%3E3123123",
		},
		{
			name:        "test 1 2.txt",
			encodedName: "test%201%202.txt",
		},
		{
			name:        "test++ 1.txt",
			encodedName: "test%2B%2B%201.txt",
		},
	}

	for _, u := range want {
		if u.encodedName != urlEncodePath(u.name) {
			t.Fatal("Error")
		}
	}
}

// Tests constructing valid endpoint url.
func TestGetEndpointURL(t *testing.T) {
	if _, err := getEndpointURL("s3.amazonaws.com", false); err != nil {
		t.Fatal("Error:", err)
	}
	if _, err := getEndpointURL("192.168.1.1", false); err != nil {
		t.Fatal("Error:", err)
	}
	if _, err := getEndpointURL("13333.123123.-", false); err == nil {
		t.Fatal("Error")
	}
	if _, err := getEndpointURL("s3.aamzza.-", false); err == nil {
		t.Fatal("Error")
	}
	if _, err := getEndpointURL("s3.amazonaws.com:443", false); err == nil {
		t.Fatal("Error")
	}
}

// Tests valid ip address.
func TestValidIPAddr(t *testing.T) {
	type validIP struct {
		ip    string
		valid bool
	}

	want := []validIP{
		{
			ip:    "192.168.1.1",
			valid: true,
		},
		{
			ip:    "192.1.8",
			valid: false,
		},
		{
			ip:    "..192.",
			valid: false,
		},
		{
			ip:    "192.168.1.1.1",
			valid: false,
		},
	}
	for _, w := range want {
		valid := isValidIP(w.ip)
		if valid != w.valid {
			t.Fatal("Error")
		}
	}
}

// Tests valid endpoint domain.
func TestValidEndpointDomain(t *testing.T) {
	type validEndpoint struct {
		endpointDomain string
		valid          bool
	}

	want := []validEndpoint{
		{
			endpointDomain: "s3.amazonaws.com",
			valid:          true,
		},
		{
			endpointDomain: "s3.amazonaws.com_",
			valid:          false,
		},
		{
			endpointDomain: "%$$$",
			valid:          false,
		},
		{
			endpointDomain: "s3.amz.test.com",
			valid:          true,
		},
		{
			endpointDomain: "s3.%%",
			valid:          false,
		},
		{
			endpointDomain: "localhost",
			valid:          true,
		},
		{
			endpointDomain: "-localhost",
			valid:          false,
		},
		{
			endpointDomain: "",
			valid:          false,
		},
		{
			endpointDomain: "\n \t",
			valid:          false,
		},
		{
			endpointDomain: "    ",
			valid:          false,
		},
	}
	for _, w := range want {
		valid := isValidDomain(w.endpointDomain)
		if valid != w.valid {
			t.Fatal("Error:", w.endpointDomain)
		}
	}
}

// Tests valid endpoint url.
func TestValidEndpointURL(t *testing.T) {
	type validURL struct {
		url   string
		valid bool
	}
	want := []validURL{
		{
			url:   "https://s3.amazonaws.com",
			valid: true,
		},
		{
			url:   "https://s3.amazonaws.com/bucket/object",
			valid: false,
		},
		{
			url:   "192.168.1.1",
			valid: false,
		},
	}
	for _, w := range want {
		u, err := url.Parse(w.url)
		if err != nil {
			t.Fatal("Error:", err)
		}
		valid := false
		if err := isValidEndpointURL(u); err == nil {
			valid = true
		}
		if valid != w.valid {
			t.Fatal("Error")
		}
	}
}
