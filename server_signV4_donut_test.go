/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"

	"github.com/minio/minio/pkg/donut"
	. "gopkg.in/check.v1"
)

type MyAPISignatureV4Suite struct {
	root            string
	req             *http.Request
	body            io.ReadSeeker
	accessKeyID     string
	secretAccessKey string
}

var _ = Suite(&MyAPISignatureV4Suite{})

var testSignatureV4Server *httptest.Server

// create a dummy TestNodeDiskMap
func createTestNodeDiskMap(p string) map[string][]string {
	nodes := make(map[string][]string)
	nodes["localhost"] = make([]string, 16)
	for i := 0; i < len(nodes["localhost"]); i++ {
		diskPath := filepath.Join(p, strconv.Itoa(i))
		if _, err := os.Stat(diskPath); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(diskPath, 0700)
			}
		}
		nodes["localhost"][i] = diskPath
	}
	return nodes
}

func (s *MyAPISignatureV4Suite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(err, IsNil)
	s.root = root

	conf := &donut.Config{}
	conf.Version = "0.0.1"
	conf.DonutName = "test"
	conf.NodeDiskMap = createTestNodeDiskMap(root)
	conf.MaxSize = 100000
	donut.SetDonutConfigPath(filepath.Join(root, "donut.json"))
	perr := donut.SaveConfig(conf)
	c.Assert(perr, IsNil)

	accessKeyID, perr := generateAccessKeyID()
	c.Assert(perr, IsNil)
	secretAccessKey, perr := generateSecretAccessKey()
	c.Assert(perr, IsNil)

	authConf := &AuthConfig{}
	authConf.Users = make(map[string]*AuthUser)
	authConf.Users[string(accessKeyID)] = &AuthUser{
		Name:            "testuser",
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	s.accessKeyID = string(accessKeyID)
	s.secretAccessKey = string(secretAccessKey)

	SetAuthConfigPath(root)
	perr = SaveConfig(authConf)
	c.Assert(perr, IsNil)

	minioAPI := getNewAPI(false)
	httpHandler := getAPIHandler(false, minioAPI)
	go startTM(minioAPI)
	testSignatureV4Server = httptest.NewServer(httpHandler)
}

func (s *MyAPISignatureV4Suite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	testSignatureV4Server.Close()
}

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

// urlEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncodeName(name string) (string, error) {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	if reservedNames.MatchString(name) {
		return name, nil
	}
	var encodedName string
	for _, s := range name {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				return "", errors.New("invalid utf-8")
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedName = encodedName + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedName, nil
}

// sum256Reader calculate sha256 sum for an input read seeker
func sum256Reader(reader io.ReadSeeker) ([]byte, error) {
	h := sha256.New()
	var err error

	start, _ := reader.Seek(0, 1)
	defer reader.Seek(start, 0)

	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		h.Write(byteBuffer)
	}

	if err != io.EOF {
		return nil, err
	}

	return h.Sum(nil), nil
}

// sum256 calculate sha256 sum for an input byte array
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumHMAC calculate hmac between two input byte array
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func (s *MyAPISignatureV4Suite) newRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	t := time.Now().UTC()
	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-amz-date", t.Format(iso8601Format))
	if method == "" {
		method = "POST"
	}

	// add Content-Length
	req.ContentLength = contentLength

	// add body
	switch {
	case body == nil:
		req.Body = nil
	default:
		req.Body = ioutil.NopCloser(body)
	}

	// save for subsequent use
	hash := func() string {
		switch {
		case body == nil:
			return hex.EncodeToString(sum256([]byte{}))
		default:
			sum256Bytes, _ := sum256Reader(body)
			return hex.EncodeToString(sum256Bytes)
		}
	}
	hashedPayload := hash()
	req.Header.Set("x-amz-content-sha256", hashedPayload)

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
	encodedPath, _ := urlEncodeName(req.URL.Path)
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
		"milkyway",
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := authHeaderPrefix + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	date := sumHMAC([]byte("AWS4"+s.secretAccessKey), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		authHeaderPrefix + " Credential=" + s.accessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return req, nil
}

func (s *MyAPISignatureV4Suite) TestDeleteBucket(c *C) {
	request, err := s.newRequest("DELETE", testSignatureV4Server.URL+"/mybucket", 0, nil)
	c.Assert(err, IsNil)

	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPISignatureV4Suite) TestDeleteObject(c *C) {
	request, err := s.newRequest("DELETE", testSignatureV4Server.URL+"/mybucket/myobject", 0, nil)
	c.Assert(err, IsNil)
	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPISignatureV4Suite) TestNonExistantBucket(c *C) {
	request, err := s.newRequest("HEAD", testSignatureV4Server.URL+"/nonexistantbucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestEmptyObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/emptyobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/emptyobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/emptyobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))
}

func (s *MyAPISignatureV4Suite) TestBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestObject(c *C) {
	buffer := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/testobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/testobject/object", int64(buffer.Len()), buffer)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/testobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPISignatureV4Suite) TestMultipleObjects(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewReader([]byte("hello one"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object1", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	buffer2 := bytes.NewReader([]byte("hello two"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object2", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	buffer3 := bytes.NewReader([]byte("hello three"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object3", int64(buffer3.Len()), buffer3)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object3", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify object
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
}

func (s *MyAPISignatureV4Suite) TestNotImplemented(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/bucket/object?policy", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MyAPISignatureV4Suite) TestHeader(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/bucket/object", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestPutBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/put-bucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestPutObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/put-object", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/put-object/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestListBuckets(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var results ListBucketsResponse
	decoder := xml.NewDecoder(response.Body)
	err = decoder.Decode(&results)
	c.Assert(err, IsNil)
}

func (s *MyAPISignatureV4Suite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/innonexistantbucket/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestHeadOnObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/headonobject", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/headonobject/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/headonobject/object1", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestHeadOnBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

func (s *MyAPISignatureV4Suite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/xmlnamenotinobjectlistjson", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/xmlnamenotinobjectlistjson", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

func (s *MyAPISignatureV4Suite) TestContentTypePersists(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists/one", int64(buffer1.Len()), buffer1)
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists/two", int64(buffer2.Len()), buffer2)
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MyAPISignatureV4Suite) TestPartialContent(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/partial-content", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/partial-content/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// prepare request
	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/partial-content/bar", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Range", "bytes=6-7")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "Wo")
}

func (s *MyAPISignatureV4Suite) TestListObjectsHandlerErrors(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors-.", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors?max-keys=-2", 0, nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647.", http.StatusBadRequest)
}

func (s *MyAPISignatureV4Suite) TestPutBucketErrors(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket-.", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket?acl", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPISignatureV4Suite) TestGetObjectErrors(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors/bar", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors-./bar", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPISignatureV4Suite) TestGetObjectRangeErrors(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/getobjectrangeerrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/getobjectrangeerrors/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjectrangeerrors/bar", 0, nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPISignatureV4Suite) TestObjectMultipartAbort(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultipartabort/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISignatureV4Suite) TestBucketMultipartList(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/bucketmultipartlist?uploads", 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &ListMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPISignatureV4Suite) TestObjectMultipartList(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultipartlist/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000.", http.StatusBadRequest)
}

func (s *MyAPISignatureV4Suite) TestObjectMultipart(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultiparts/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// complete multipart upload
	completeUploads := &donut.CompleteMultipartUpload{
		Part: []donut.CompletePart{
			{
				PartNumber: 1,
				ETag:       response1.Header.Get("ETag"),
			},
			{
				PartNumber: 2,
				ETag:       response2.Header.Get("ETag"),
			},
		},
	}

	completeBytes, err := xml.Marshal(completeUploads)
	c.Assert(err, IsNil)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID, int64(len(completeBytes)), bytes.NewReader(completeBytes))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}
