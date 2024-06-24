// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio/internal/auth"
)

type bucketCorsTestCase struct {
	name       string
	method     string
	bucketName string
	accessKey  string
	secretKey  string
	body       string

	// Wanted response
	wantStatus   int
	wantBodyResp []byte
	wantErrResp  *APIErrorResponse
}

var testCORSEndpoints = []string{"GetBucketCors", "PutBucketCors", "DeleteBucketCors"}

// TestBucketCorsWrongCreds tests the authentication layer is correctly applied
func TestBucketCorsWrongCreds(t *testing.T) {
	args := ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketCorsWrongCredentials, endpoints: testCORSEndpoints}
	ExecObjectLayerAPITest(args)
}

// TestBucketCorsNotExist tests when a bucket does not exist
func TestBucketCorsNotExist(t *testing.T) {
	args := ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketCorsBucketNotExist, endpoints: testCORSEndpoints}
	ExecObjectLayerAPITest(args)
}

// TestBucketCorsPutValidation tests the validation of the PUT request
func TestBucketCorsPutValidation(t *testing.T) {
	args := ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketCorsPutValidation, endpoints: testCORSEndpoints}
	ExecObjectLayerAPITest(args)
}

// TestBucketCorsCRUD tests we can create, read and delete CORS configuration
func TestBucketCorsCRUD(t *testing.T) {
	args := ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketCorsPutDelete, endpoints: testCORSEndpoints}
	ExecObjectLayerAPITest(args)
}

func testBucketCorsWrongCredentials(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
	resource := SlashSeparator + bucketName + SlashSeparator
	fakeAccessKey := "notarealaccesskey"
	fakeSecretKey := "notarealsecretkey"
	testCases := []bucketCorsTestCase{
		{
			name:       "get empty credentials",
			method:     http.MethodGet,
			bucketName: bucketName,
			accessKey:  "",
			secretKey:  "",
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
		},
		{
			name:       "get wrong credentials",
			method:     http.MethodGet,
			bucketName: bucketName,
			accessKey:  fakeAccessKey,
			secretKey:  fakeSecretKey,
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
		},
		{
			name:       "put empty credentials",
			method:     http.MethodPut,
			bucketName: bucketName,
			accessKey:  "",
			secretKey:  "",
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
		},
		{
			name:       "put wrong credentials",
			method:     http.MethodPut,
			bucketName: bucketName,
			accessKey:  fakeAccessKey,
			secretKey:  fakeSecretKey,
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
		},
		{
			name:       "delete empty credentials",
			method:     http.MethodDelete,
			bucketName: bucketName,
			accessKey:  "",
			secretKey:  "",
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
		},
		{
			name:       "delete wrong credentials",
			method:     http.MethodDelete,
			bucketName: bucketName,
			accessKey:  fakeAccessKey,
			secretKey:  fakeSecretKey,
			wantStatus: http.StatusForbidden,
			wantErrResp: &APIErrorResponse{
				Resource: resource,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
		},
	}

	testBucketCors(obj, instanceType, bucketName, apiRouter, t, testCases)
}

func testBucketCorsBucketNotExist(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
	nonExistentBucket := "doesnotexist"
	resource := SlashSeparator + nonExistentBucket + SlashSeparator

	testCases := []bucketCorsTestCase{
		{
			name:   "get non existent bucket",
			method: http.MethodGet,
		},
		{
			name:   "put non existent bucket",
			method: http.MethodPut,
		},
		{
			name:   "delete non existent bucket",
			method: http.MethodDelete,
		},
	}

	// Common attributes for these test cases
	for i := range testCases {
		testCases[i].bucketName = nonExistentBucket
		testCases[i].accessKey = credentials.AccessKey
		testCases[i].secretKey = credentials.SecretKey
		testCases[i].wantStatus = http.StatusNotFound
		testCases[i].wantErrResp = &APIErrorResponse{
			Resource: resource,
			Code:     "NoSuchBucket",
			Message:  "The specified bucket does not exist",
		}
	}

	testBucketCors(obj, instanceType, bucketName, apiRouter, t, testCases)
}

func testBucketCorsPutValidation(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
	resource := SlashSeparator + bucketName + SlashSeparator

	testCases := []bucketCorsTestCase{
		{
			name:       "too many rules max 100",
			body:       getTooManyRulesXML(),
			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "The number of CORS rules should not exceed allowed limit of 100 rules.",
			},
		},
		{
			name: "broken xml data",
			body: `<CORSRule>
				<AllowedOrigin>http://*.example23.*</AllowedOrigin>
				<AllowedOrigin>http://*.example.*</AllowedOrigin>
				<AllowedMethod>PUT</AllowedMethod>
				<AllowedMethod>POST</Allowed`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "MalformedXML",
				Message: "The XML you provided was not well-formed or did not validate against our published schema",
			},
		},
		{
			name: "invalid AllowedHeader multiple wildcard",
			body: `<CORSRule>
					<AllowedOrigin>http://www.example.com</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedHeader>HELLO*WORLD*</AllowedHeader>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "AllowedHeader HELLO*WORLD* can not have more than one wildcard.",
			},
		},
		{
			name: "invalid AllowedMethod not in hardcoded list",
			body: `<CORSRule>
					<AllowedOrigin>http://www.example.com</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedMethod>PATCH</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "Found unsupported HTTP method in CORS config. Unsupported method is PATCH",
			},
		},
		{
			name: "invalid AllowedOrigin multiple wildcards",
			body: `<CORSRule>
					<AllowedOrigin>http://*.example23.*</AllowedOrigin>
					<AllowedOrigin>http://*.example.*</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "AllowedOrigin http://*.example23.* can not have more than one wildcard.",
			},
		},
		{
			name: "multiple invalid rules picks first one",
			body: `<CORSRule>
					<AllowedOrigin>http://*.example.*</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>
				<CORSRule>
					<AllowedOrigin>http://*.example323.*</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "AllowedOrigin http://*.example.* can not have more than one wildcard.",
			},
		},
		{
			name: "no allowed methods in rule",
			body: `<CORSRule>
					<AllowedOrigin>http://www.example.com</AllowedOrigin>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "MalformedXML",
				Message: "The XML you provided was not well-formed or did not validate against our published schema",
			},
		},
		{
			name: "no allowed origins in rule",
			body: `<CORSRule>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "MalformedXML",
				Message: "The XML you provided was not well-formed or did not validate against our published schema",
			},
		},
		{
			name:       "no rules found",
			body:       ``,
			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "MalformedXML",
				Message: "The XML you provided was not well-formed or did not validate against our published schema",
			},
		},
		{
			name: "invalid empty method",
			body: `<CORSRule>
					<AllowedOrigin>http://example.com</AllowedOrigin>
					<AllowedMethod></AllowedMethod>
				</CORSRule>`,

			wantStatus: http.StatusBadRequest,
			wantErrResp: &APIErrorResponse{
				Code:    "InvalidRequest",
				Message: "Found unsupported HTTP method in CORS config. Unsupported method is ",
			},
		},
		{
			name: "valid empty origin as AWS allows it",
			body: `<CORSRule>
					<AllowedOrigin></AllowedOrigin>
					<AllowedMethod>GET</AllowedMethod>
				</CORSRule>`,

			wantStatus:   http.StatusOK,
			wantBodyResp: []byte(``),
		},
		{
			name: "valid rules",
			body: `<CORSRule>
					<AllowedOrigin>http://www.example1.com</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedMethod>POST</AllowedMethod>
					<AllowedMethod>DELETE</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>
				<CORSRule>
					<AllowedOrigin>http://www.example2.com</AllowedOrigin>
					<AllowedMethod>PUT</AllowedMethod>
					<AllowedMethod>POST</AllowedMethod>
					<AllowedMethod>DELETE</AllowedMethod>
					<AllowedHeader>*</AllowedHeader>
				</CORSRule>
				<CORSRule>
					<AllowedOrigin>*</AllowedOrigin>
					<AllowedMethod>GET</AllowedMethod>
				</CORSRule>`,

			wantStatus:   http.StatusOK,
			wantBodyResp: []byte(``),
		},
	}

	// Set the common attributes for these test cases to save repetition
	for i := range testCases {
		testCases[i].method = http.MethodPut
		testCases[i].bucketName = bucketName
		testCases[i].accessKey = credentials.AccessKey
		testCases[i].secretKey = credentials.SecretKey
		if testCases[i].wantErrResp != nil {
			testCases[i].wantErrResp.Resource = resource
		}

	}

	testBucketCors(obj, instanceType, bucketName, apiRouter, t, testCases)
}

func testBucketCorsPutDelete(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
	validRuleBytes := []byte(`<CORSRule><AllowedHeader>*</AllowedHeader><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>http://www.example1.com</AllowedOrigin></CORSRule>`)

	testCases := []bucketCorsTestCase{
		{
			name:         "put a valid cors config",
			method:       http.MethodPut,
			body:         string(validRuleBytes),
			wantStatus:   http.StatusOK,
			wantBodyResp: []byte(``),
		},
		{
			name:       "get back that config",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantBodyResp: []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">%s</CORSConfiguration>`, validRuleBytes)),
		},
		{
			name:         "delete the cors config",
			method:       http.MethodDelete,
			wantStatus:   http.StatusNoContent,
			wantBodyResp: []byte(``),
		},
		{
			name:       "get back no config",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
			wantErrResp: &APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "NoSuchCORSConfiguration",
				Message:  "The CORS configuration does not exist",
			},
		},
	}

	// Set the common attributes for these test cases to save repetition
	for i := range testCases {
		testCases[i].bucketName = bucketName
		testCases[i].accessKey = credentials.AccessKey
		testCases[i].secretKey = credentials.SecretKey
	}

	testBucketCors(obj, instanceType, bucketName, apiRouter, t, testCases)
}

// testBucketCors does the actual running of the steps of the test case
func testBucketCors(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, t *testing.T, testCases []bucketCorsTestCase) {
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// HTTP request body
			if test.body != "" {
				test.body = `<?xml version="1.0" encoding="UTF-8"?>
					<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` + test.body + `</CORSConfiguration>`
			}

			// HTTP request and recorder
			rec := httptest.NewRecorder()
			req, err := newTestSignedRequestV4(test.method,
				getBucketCorsURL("", test.bucketName),
				int64(len(test.body)),
				strings.NewReader(test.body),
				test.accessKey,
				test.secretKey,
				nil,
			)
			if err != nil {
				t.Fatalf("Instance: %s, error creating request: %s", instanceType, err)
			}

			// Execute the handler
			apiRouter.ServeHTTP(rec, req)
			if rec.Code != test.wantStatus {
				t.Errorf("Instance: %s, want status code: %d, got: %d", instanceType, test.wantStatus, rec.Code)
			}

			// Check non-error body response against wanted
			if test.wantBodyResp != nil {
				if !bytes.Equal(test.wantBodyResp, rec.Body.Bytes()) {
					t.Errorf("Instance: %s, want response: %s, got: %s", instanceType, string(test.wantBodyResp), rec.Body.String())
				}
			}

			// Check error response against wanted
			if test.wantErrResp != nil {
				errResp := APIErrorResponse{}
				err = xml.Unmarshal(rec.Body.Bytes(), &errResp)
				if err != nil {
					t.Fatalf("Instance: %s, error unmarshalling error response: %s", instanceType, err)
				}
				if errResp.Resource != test.wantErrResp.Resource {
					t.Errorf("Instance: %s, want APIErrorResponse.Resource: %s, got: %s", instanceType, test.wantErrResp.Resource, errResp.Resource)
				}
				if errResp.Message != test.wantErrResp.Message {
					t.Errorf("Instance: %s, want APIErrorResponse.Message: %s, got: %s", instanceType, test.wantErrResp.Message, errResp.Message)
				}
				if errResp.Code != test.wantErrResp.Code {
					t.Errorf("Instance: %s, want APIErrorResponse.Code: %s, got: %s", instanceType, test.wantErrResp.Code, errResp.Code)
				}
			}
		})
	}
}

func getTooManyRulesXML() string {
	// Generate a XML with 101 rules which will be invalid
	var rules strings.Builder
	for i := 0; i < 101; i++ {
		rules.WriteString("<CORSRule>")
		rules.WriteString("<AllowedOrigin>*</AllowedOrigin>")
		rules.WriteString("<AllowedMethod>GET</AllowedMethod>")
		rules.WriteString("</CORSRule>")
	}
	return rules.String()
}
