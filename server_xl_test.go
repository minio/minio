/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"

	. "gopkg.in/check.v1"
)

// API suite container.
type MyAPIXLSuite struct {
	root         string
	erasureDisks []string
	req          *http.Request
	body         io.ReadSeeker
	credential   credential
}

var _ = Suite(&MyAPIXLSuite{})

var testAPIXLServer *httptest.Server

func (s *MyAPIXLSuite) SetUpSuite(c *C) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		c.Check(err, IsNil)
		erasureDisks = append(erasureDisks, path)
	}

	root, e := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(e, IsNil)
	s.root = root
	s.erasureDisks = erasureDisks

	// Initialize server config.
	initConfig()

	// Initialize name space lock.
	initNSLock()

	// Set port.
	addr := ":" + strconv.Itoa(getFreePort())

	// Get credential.
	s.credential = serverConfig.GetCredential()

	// Set a default region.
	serverConfig.SetRegion("us-east-1")

	// Do this only once here
	setGlobalConfigPath(root)

	// Save config.
	c.Assert(serverConfig.Save(), IsNil)

	apiServer := configureServer(serverCmdConfig{
		serverAddr:  addr,
		exportPaths: erasureDisks,
	})
	testAPIXLServer = httptest.NewServer(apiServer.Handler)
}

func (s *MyAPIXLSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	for _, disk := range s.erasureDisks {
		os.RemoveAll(disk)
	}
	testAPIXLServer.Close()
}

func (s *MyAPIXLSuite) newRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
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

	date := sumHMAC([]byte("AWS4"+s.credential.SecretAccessKey), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("us-east-1"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + s.credential.AccessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return req, nil
}

func (s *MyAPIXLSuite) TestAuth(c *C) {
	secretID, err := genSecretAccessKey()
	c.Assert(err, IsNil)

	accessID, err := genAccessKeyID()
	c.Assert(err, IsNil)

	c.Assert(len(secretID), Equals, minioSecretID)
	c.Assert(len(accessID), Equals, minioAccessID)
}

func (s *MyAPIXLSuite) TestBucketPolicy(c *C) {
	// Sample bucket policy.
	bucketPolicyBuf := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::policybucket"
            ]
        },
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::policybucket/this*"
            ]
        }
    ]
}`

	// Put a new bucket policy.
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/policybucket?policy", int64(len(bucketPolicyBuf)), bytes.NewReader([]byte(bucketPolicyBuf)))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Fetch the uploaded policy.
	request, err = s.newRequest("GET", testAPIXLServer.URL+"/policybucket?policy", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	bucketPolicyReadBuf, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// Verify if downloaded policy matches with previousy uploaded.
	c.Assert(bytes.Equal([]byte(bucketPolicyBuf), bucketPolicyReadBuf), Equals, true)

	// Delete policy.
	request, err = s.newRequest("DELETE", testAPIXLServer.URL+"/policybucket?policy", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Verify if the policy was indeed deleted.
	request, err = s.newRequest("GET", testAPIXLServer.URL+"/policybucket?policy", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPIXLSuite) TestDeleteBucket(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/deletebucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testAPIXLServer.URL+"/deletebucket", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIXLSuite) TestDeleteBucketNotEmpty(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/deletebucket-notempty", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/deletebucket-notempty/myobject", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testAPIXLServer.URL+"/deletebucket-notempty", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusConflict)
}

func (s *MyAPIXLSuite) TestDeleteObject(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/deletebucketobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/deletebucketobject/myobject", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/deletebucketobject/myobject", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testAPIXLServer.URL+"/deletebucketobject/myobject", 0, nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIXLSuite) TestNonExistentBucket(c *C) {
	request, err := s.newRequest("HEAD", testAPIXLServer.URL+"/nonexistentbucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPIXLSuite) TestEmptyObject(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/emptyobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/emptyobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/emptyobject/object", 0, nil)
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

func (s *MyAPIXLSuite) TestBucket(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIXLSuite) TestObject(c *C) {
	buffer := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/testobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/testobject/object", int64(buffer.Len()), buffer)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/testobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPIXLSuite) TestMultipleObjects(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/multipleobjects", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/multipleobjects/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewReader([]byte("hello one"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/multipleobjects/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/multipleobjects/object1", 0, nil)
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
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/multipleobjects/object2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/multipleobjects/object2", 0, nil)
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
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/multipleobjects/object3", int64(buffer3.Len()), buffer3)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/multipleobjects/object3", 0, nil)
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

func (s *MyAPIXLSuite) TestNotImplemented(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/bucket/object?policy", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)
}

func (s *MyAPIXLSuite) TestHeader(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/bucket/object", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPIXLSuite) TestPutBucket(c *C) {
	// Block 1: Testing for racey access
	// The assertion is removed from this block since the purpose of this block is to find races
	// The purpose this block is not to check for correctness of functionality
	// Run the test with -race flag to utilize this
	var wg sync.WaitGroup
	for i := 0; i < ConcurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, err := s.newRequest("PUT", testAPIXLServer.URL+"/put-bucket", 0, nil)
			c.Assert(err, IsNil)

			client := http.Client{}
			response, err := client.Do(request)
			defer response.Body.Close()
		}()
	}
	wg.Wait()

	//Block 2: testing for correctness of the functionality
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/put-bucket-slash/", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	response.Body.Close()

}

func (s *MyAPIXLSuite) TestCopyObject(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/put-object-copy", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/put-object-copy/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/put-object-copy/object1", 0, nil)
	request.Header.Set("X-Amz-Copy-Source", "/put-object-copy/object")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/put-object-copy/object1", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	object, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(object), Equals, "hello world")
}

func (s *MyAPIXLSuite) TestPutObject(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/put-object", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/put-object/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIXLSuite) TestPutObjectLongName(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/put-object-long-name", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer := bytes.NewReader([]byte("hello world"))
	longObjName := fmt.Sprintf("%0256d", 1)
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/put-object-long-name/"+longObjName, int64(buffer.Len()), buffer)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPIXLSuite) TestListBuckets(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/", 0, nil)
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

func (s *MyAPIXLSuite) TestNotBeAbleToCreateObjectInNonexistentBucket(c *C) {
	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/innonexistentbucket/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPIXLSuite) TestHeadOnObject(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/headonobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/headonobject/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/headonobject/object1", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	lastModified := response.Header.Get("Last-Modified")
	t, err := time.Parse(http.TimeFormat, lastModified)
	c.Assert(err, IsNil)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/headonobject/object1", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Set("If-Modified-Since", t.Add(1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotModified)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/headonobject/object1", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Set("If-Unmodified-Since", t.Add(-1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPreconditionFailed)
}

func (s *MyAPIXLSuite) TestHeadOnBucket(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIXLSuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/", 0, nil)
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

func (s *MyAPIXLSuite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/xmlnamenotinobjectlistjson", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/xmlnamenotinobjectlistjson", 0, nil)
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

// Tests if content type persists.
func (s *MyAPIXLSuite) TestContentTypePersists(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/contenttype-persists", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/contenttype-persists/one", int64(buffer1.Len()), buffer1)
	request.Header.Set("Content-Type", "application/zip")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/contenttype-persists/two", int64(buffer2.Len()), buffer2)
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testAPIXLServer.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")
}

func (s *MyAPIXLSuite) TestPartialContent(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/partial-content", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/partial-content/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Prepare request
	request, err = s.newRequest("GET", testAPIXLServer.URL+"/partial-content/bar", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Range", "bytes=6-7")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "Wo")
}

func (s *MyAPIXLSuite) TestListObjectsHandlerErrors(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/objecthandlererrors-.", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/objecthandlererrors?max-keys=-2", 0, nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647.", http.StatusBadRequest)
}

func (s *MyAPIXLSuite) TestPutBucketErrors(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/putbucket-.", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/putbucket?acl", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPIXLSuite) TestGetObjectErrors(c *C) {
	request, err := s.newRequest("GET", testAPIXLServer.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/getobjecterrors/bar", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/getobjecterrors-./bar", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPIXLSuite) TestGetObjectRangeErrors(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/getobjectrangeerrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/getobjectrangeerrors/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/getobjectrangeerrors/bar", 0, nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPIXLSuite) TestObjectMultipartAbort(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartabort", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/objectmultipartabort/object?uploads", 0, nil)
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
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testAPIXLServer.URL+"/objectmultipartabort/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIXLSuite) TestBucketMultipartList(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/bucketmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/bucketmultipartlist/object?uploads", 0, nil)
	c.Assert(err, IsNil)

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
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/bucketmultipartlist?uploads", 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	// The reason to duplicate this structure here is to verify if the
	// unmarshalling works from a client perspective, specifically
	// while unmarshalling time.Time type for 'Initiated' field.
	// time.Time does not honor xml marshaler, it means that we need
	// to encode/format it before giving it to xml marshalling.

	// This below check adds client side verification to see if its
	// truly parseable.

	// listMultipartUploadsResponse - format for list multipart uploads response.
	type listMultipartUploadsResponse struct {
		XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

		Bucket             string
		KeyMarker          string
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string
		NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		EncodingType       string
		MaxUploads         int
		IsTruncated        bool
		// All the in progress multipart uploads.
		Uploads []struct {
			Key          string
			UploadID     string `xml:"UploadId"`
			Initiator    Initiator
			Owner        Owner
			StorageClass string
			Initiated    time.Time // Keep this native to be able to parse properly.
		}
		Prefix         string
		Delimiter      string
		CommonPrefixes []CommonPrefix
	}

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &listMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPIXLSuite) TestValidateObjectMultipartUploadID(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartlist-uploadid", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/objectmultipartlist-uploadid/directory1/directory2/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
}

func (s *MyAPIXLSuite) TestObjectMultipartList(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/objectmultipartlist/object?uploads", 0, nil)
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
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/objectmultipartlist/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testAPIXLServer.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000.", http.StatusBadRequest)
}

func (s *MyAPIXLSuite) TestObjectMultipart(c *C) {
	request, err := s.newRequest("PUT", testAPIXLServer.URL+"/objectmultiparts", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/objectmultiparts/object?uploads", 0, nil)
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

	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*1024*1024/16)

	hasher := md5.New()
	hasher.Write(data)
	md5Sum := hasher.Sum(nil)

	buffer1 := bytes.NewReader(data)
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// Create a byte array of 1 byte.
	data = []byte("0")

	hasher = md5.New()
	hasher.Write(data)
	md5Sum = hasher.Sum(nil)

	buffer2 := bytes.NewReader(data)
	request, err = s.newRequest("PUT", testAPIXLServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// Complete multipart upload
	completeUploads := &completeMultipartUpload{
		Parts: []completePart{
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

	request, err = s.newRequest("POST", testAPIXLServer.URL+"/objectmultiparts/object?uploadId="+uploadID, int64(len(completeBytes)), bytes.NewReader(completeBytes))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}
