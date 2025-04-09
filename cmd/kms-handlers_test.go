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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/pkg/v3/policy"
)

const (
	// KMS API paths
	// For example: /minio/kms/v1/key/list?pattern=*
	kmsURL           = kmsPathPrefix + kmsAPIVersionPrefix
	kmsStatusPath    = kmsURL + "/status"
	kmsMetricsPath   = kmsURL + "/metrics"
	kmsAPIsPath      = kmsURL + "/apis"
	kmsVersionPath   = kmsURL + "/version"
	kmsKeyCreatePath = kmsURL + "/key/create"
	kmsKeyListPath   = kmsURL + "/key/list"
	kmsKeyStatusPath = kmsURL + "/key/status"

	// Admin API paths
	// For example: /minio/admin/v3/kms/status
	adminURL              = adminPathPrefix + adminAPIVersionPrefix
	kmsAdminStatusPath    = adminURL + "/kms/status"
	kmsAdminKeyStatusPath = adminURL + "/kms/key/status"
	kmsAdminKeyCreate     = adminURL + "/kms/key/create"
)

const (
	userAccessKey = "miniofakeuseraccesskey"
	userSecretKey = "miniofakeusersecret"
)

type kmsTestCase struct {
	name   string
	method string
	path   string
	query  map[string]string

	// User credentials and policy for request
	policy string
	asRoot bool

	// Wanted in response.
	wantStatusCode int
	wantKeyNames   []string
	wantResp       []string
}

func TestKMSHandlersCreateKey(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, true)
	defer tearDown()

	tests := []kmsTestCase{
		// Create key test
		{
			name:   "create key as user with no policy want forbidden",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "new-test-key"},
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "create key as user with no resources specified want success",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "new-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:CreateKey"] }`,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "create key as user set policy to allow want success",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "second-new-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:CreateKey"],
				"Resource": ["arn:minio:kms:::second-new-test-*"] }`,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "create key as user set policy to non matching resource want forbidden",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "third-new-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:CreateKey"],
				"Resource": ["arn:minio:kms:::non-matching-key-name"] }`,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
	}
	for testNum, test := range tests {
		t.Run(fmt.Sprintf("%d %s", testNum+1, test.name), func(t *testing.T) {
			execKMSTest(t, test, adminTestBed)
		})
	}
}

func TestKMSHandlersKeyStatus(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, true)
	defer tearDown()

	tests := []kmsTestCase{
		{
			name:   "create a first key root user",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "key status as root want success",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"abc-test-key"},
		},
		{
			name:   "key status as user no policy want forbidden",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "key status as user legacy no resources specified want success",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:KeyStatus"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"abc-test-key"},
		},
		{
			name:   "key status as user set policy to allow only one key",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:KeyStatus"],
					"Resource": ["arn:minio:kms:::abc-test-*"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"abc-test-key"},
		},
		{
			name:   "key status as user set policy to allow non-matching key",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:KeyStatus"],
					"Resource": ["arn:minio:kms:::xyz-test-key"] }`,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
	}
	for testNum, test := range tests {
		t.Run(fmt.Sprintf("%d %s", testNum+1, test.name), func(t *testing.T) {
			execKMSTest(t, test, adminTestBed)
		})
	}
}

func TestKMSHandlersAPIs(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, true)
	defer tearDown()

	tests := []kmsTestCase{
		// Version test
		{
			name:   "version as root want success",
			method: http.MethodGet,
			path:   kmsVersionPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"version"},
		},
		{
			name:   "version as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsVersionPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "version as user with policy ignores resource want success",
			method: http.MethodGet,
			path:   kmsVersionPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:Version"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"version"},
		},

		// APIs test
		{
			name:   "apis as root want success",
			method: http.MethodGet,
			path:   kmsAPIsPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"stub/path"},
		},
		{
			name:   "apis as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsAPIsPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "apis as user with policy ignores resource want success",
			method: http.MethodGet,
			path:   kmsAPIsPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:API"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"stub/path"},
		},

		// Metrics test
		{
			name:   "metrics as root want success",
			method: http.MethodGet,
			path:   kmsMetricsPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"kms"},
		},
		{
			name:   "metrics as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsMetricsPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "metrics as user with policy ignores resource want success",
			method: http.MethodGet,
			path:   kmsMetricsPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:Metrics"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"kms"},
		},

		// Status tests
		{
			name:   "status as root want success",
			method: http.MethodGet,
			path:   kmsStatusPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"MinIO builtin"},
		},
		{
			name:   "status as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsStatusPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "status as user with policy ignores resource want success",
			method: http.MethodGet,
			path:   kmsStatusPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:Status"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"]}`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"MinIO builtin"},
		},
	}
	for testNum, test := range tests {
		t.Run(fmt.Sprintf("%d %s", testNum+1, test.name), func(t *testing.T) {
			execKMSTest(t, test, adminTestBed)
		})
	}
}

func TestKMSHandlersListKeys(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, true)
	defer tearDown()

	tests := []kmsTestCase{
		{
			name:   "create a first key root user",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "create a second key root user",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "xyz-test-key"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
		},

		// List keys tests
		{
			name:   "list keys as root want all to be returned",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{"default-test-key", "abc-test-key", "xyz-test-key"},
		},
		{
			name:   "list keys as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "list keys as user with no resources specified want success",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["kms:ListKeys"]
			}`,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{"default-test-key", "abc-test-key", "xyz-test-key"},
		},
		{
			name:   "list keys as user set policy resource to allow only one key",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:ListKeys"],
					"Resource": ["arn:minio:kms:::abc*"]}`,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{"abc-test-key"},
		},
		{
			name:   "list keys as user set policy to allow only one key, use pattern that includes correct key",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "abc*"},

			policy: `{"Effect": "Allow",
					"Action": ["kms:ListKeys"],
					"Resource": ["arn:minio:kms:::abc*"]}`,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{"abc-test-key"},
		},
		{
			name:   "list keys as user set policy to allow only one key, use pattern that excludes correct key",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "xyz*"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:ListKeys"],
					"Resource": ["arn:minio:kms:::abc*"]}`,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{},
		},
		{
			name:   "list keys as user set policy that has no matching key resources",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: false,

			policy: `{"Effect": "Allow",
					"Action": ["kms:ListKeys"],
					"Resource": ["arn:minio:kms:::nonematch*"]}`,

			wantStatusCode: http.StatusOK,
			wantKeyNames:   []string{},
		},
		{
			name:   "list keys as user set policy that allows listing but denies specific keys",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
			asRoot: false,

			// It looks like this should allow listing any key that isn't "default-test-key", however
			// the policy engine matches all Deny statements first, without regard to Resources (for KMS).
			// This is for backwards compatibility where historically KMS statements ignored Resources.
			policy: `{
						"Effect": "Allow",
						"Action": ["kms:ListKeys"]
					},{
						"Effect": "Deny",
						"Action": ["kms:ListKeys"],
						"Resource": ["arn:minio:kms:::default-test-key"]
					}`,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
	}

	for testNum, test := range tests {
		t.Run(fmt.Sprintf("%d %s", testNum+1, test.name), func(t *testing.T) {
			execKMSTest(t, test, adminTestBed)
		})
	}
}

func TestKMSHandlerAdminAPI(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, true)
	defer tearDown()

	tests := []kmsTestCase{
		// Create key tests
		{
			name:   "create a key root user",
			method: http.MethodPost,
			path:   kmsAdminKeyCreate,
			query:  map[string]string{"key-id": "abc-test-key"},
			asRoot: true,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "create key as user with no policy want forbidden",
			method: http.MethodPost,
			path:   kmsAdminKeyCreate,
			query:  map[string]string{"key-id": "new-test-key"},
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "create key as user with no resources specified want success",
			method: http.MethodPost,
			path:   kmsAdminKeyCreate,
			query:  map[string]string{"key-id": "new-test-key"},
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["admin:KMSCreateKey"] }`,

			wantStatusCode: http.StatusOK,
		},
		{
			name:   "create key as user set policy to non matching resource want success",
			method: http.MethodPost,
			path:   kmsAdminKeyCreate,
			query:  map[string]string{"key-id": "third-new-test-key"},
			asRoot: false,

			// Admin actions ignore Resources
			policy: `{"Effect": "Allow",
				"Action": ["admin:KMSCreateKey"],
				"Resource": ["arn:minio:kms:::this-is-disregarded"] }`,

			wantStatusCode: http.StatusOK,
		},

		// Status tests
		{
			name:   "status as root want success",
			method: http.MethodPost,
			path:   kmsAdminStatusPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"MinIO builtin"},
		},
		{
			name:   "status as user with no policy want forbidden",
			method: http.MethodPost,
			path:   kmsAdminStatusPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "status as user with policy ignores resource want success",
			method: http.MethodPost,
			path:   kmsAdminStatusPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["admin:KMSKeyStatus"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"MinIO builtin"},
		},

		// Key status tests
		{
			name:   "key status as root want success",
			method: http.MethodGet,
			path:   kmsAdminKeyStatusPath,
			asRoot: true,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"key-id"},
		},
		{
			name:   "key status as user with no policy want forbidden",
			method: http.MethodGet,
			path:   kmsAdminKeyStatusPath,
			asRoot: false,

			wantStatusCode: http.StatusForbidden,
			wantResp:       []string{"AccessDenied"},
		},
		{
			name:   "key status as user with policy ignores resource want success",
			method: http.MethodGet,
			path:   kmsAdminKeyStatusPath,
			asRoot: false,

			policy: `{"Effect": "Allow",
				"Action": ["admin:KMSKeyStatus"],
				"Resource": ["arn:minio:kms:::does-not-matter-it-is-ignored"] }`,

			wantStatusCode: http.StatusOK,
			wantResp:       []string{"key-id"},
		},
	}

	for testNum, test := range tests {
		t.Run(fmt.Sprintf("%d %s", testNum+1, test.name), func(t *testing.T) {
			execKMSTest(t, test, adminTestBed)
		})
	}
}

// execKMSTest runs a single test case for KMS handlers
func execKMSTest(t *testing.T, test kmsTestCase, adminTestBed *adminErasureTestBed) {
	var accessKey, secretKey string
	if test.asRoot {
		accessKey, secretKey = globalActiveCred.AccessKey, globalActiveCred.SecretKey
	} else {
		setupKMSUser(t, userAccessKey, userSecretKey, test.policy)
		accessKey = userAccessKey
		secretKey = userSecretKey
	}

	req := buildKMSRequest(t, test.method, test.path, accessKey, secretKey, test.query)
	rec := httptest.NewRecorder()
	adminTestBed.router.ServeHTTP(rec, req)

	t.Logf("HTTP req: %s, resp code: %d, resp body: %s", req.URL.String(), rec.Code, rec.Body.String())

	// Check status code
	if rec.Code != test.wantStatusCode {
		t.Errorf("want status code %d, got %d", test.wantStatusCode, rec.Code)
	}

	// Check returned key list is correct
	if test.wantKeyNames != nil {
		keys := []madmin.KMSKeyInfo{}
		err := json.Unmarshal(rec.Body.Bytes(), &keys)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != len(test.wantKeyNames) {
			t.Fatalf("want %d keys, got %d", len(test.wantKeyNames), len(keys))
		}

		for i, want := range keys {
			if want.CreatedBy != kms.StubCreatedBy {
				t.Fatalf("want key created by %s, got %s", kms.StubCreatedBy, want.CreatedBy)
			}
			if want.CreatedAt != kms.StubCreatedAt {
				t.Fatalf("want key created at %s, got %s", kms.StubCreatedAt, want.CreatedAt)
			}
			if test.wantKeyNames[i] != want.Name {
				t.Fatalf("want key name %s, got %s", test.wantKeyNames[i], want.Name)
			}
		}
	}

	// Check generic text in the response
	if test.wantResp != nil {
		for _, want := range test.wantResp {
			if !strings.Contains(rec.Body.String(), want) {
				t.Fatalf("want response to contain %s, got %s", want, rec.Body.String())
			}
		}
	}
}

// TestKMSHandlerNotConfiguredOrInvalidCreds tests KMS handlers for situations where KMS is not configured
// or invalid credentials are provided.
func TestKMSHandlerNotConfiguredOrInvalidCreds(t *testing.T) {
	adminTestBed, tearDown := setupKMSTest(t, false)
	defer tearDown()

	tests := []struct {
		name   string
		method string
		path   string
		query  map[string]string
	}{
		{
			name:   "GET status",
			method: http.MethodGet,
			path:   kmsStatusPath,
		},
		{
			name:   "GET metrics",
			method: http.MethodGet,
			path:   kmsMetricsPath,
		},
		{
			name:   "GET apis",
			method: http.MethodGet,
			path:   kmsAPIsPath,
		},
		{
			name:   "GET version",
			method: http.MethodGet,
			path:   kmsVersionPath,
		},
		{
			name:   "POST key create",
			method: http.MethodPost,
			path:   kmsKeyCreatePath,
			query:  map[string]string{"key-id": "master-key-id"},
		},
		{
			name:   "GET key list",
			method: http.MethodGet,
			path:   kmsKeyListPath,
			query:  map[string]string{"pattern": "*"},
		},
		{
			name:   "GET key status",
			method: http.MethodGet,
			path:   kmsKeyStatusPath,
			query:  map[string]string{"key-id": "master-key-id"},
		},
	}

	// Test when the GlobalKMS is not configured
	for _, test := range tests {
		t.Run(test.name+" not configured", func(t *testing.T) {
			req := buildKMSRequest(t, test.method, test.path, "", "", test.query)
			rec := httptest.NewRecorder()
			adminTestBed.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotImplemented {
				t.Errorf("want status code %d, got %d", http.StatusNotImplemented, rec.Code)
			}
		})
	}

	// Test when the GlobalKMS is configured but the credentials are invalid
	GlobalKMS = kms.NewStub("default-test-key")
	for _, test := range tests {
		t.Run(test.name+" invalid credentials", func(t *testing.T) {
			req := buildKMSRequest(t, test.method, test.path, userAccessKey, userSecretKey, test.query)
			rec := httptest.NewRecorder()
			adminTestBed.router.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Errorf("want status code %d, got %d", http.StatusForbidden, rec.Code)
			}
		})
	}
}

func setupKMSTest(t *testing.T, enableKMS bool) (*adminErasureTestBed, func()) {
	adminTestBed, err := prepareAdminErasureTestBed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	registerKMSRouter(adminTestBed.router)

	if enableKMS {
		GlobalKMS = kms.NewStub("default-test-key")
	}

	tearDown := func() {
		adminTestBed.TearDown()
		GlobalKMS = nil
	}
	return adminTestBed, tearDown
}

func buildKMSRequest(t *testing.T, method, path, accessKey, secretKey string, query map[string]string) *http.Request {
	if len(query) > 0 {
		queryVal := url.Values{}
		for k, v := range query {
			queryVal.Add(k, v)
		}
		path = path + "?" + queryVal.Encode()
	}

	if accessKey == "" && secretKey == "" {
		accessKey = globalActiveCred.AccessKey
		secretKey = globalActiveCred.SecretKey
	}

	req, err := newTestSignedRequestV4(method, path, 0, nil, accessKey, secretKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// setupKMSUser is a test helper that creates a new user with the provided access key and secret key
// and applies the given policy to the user.
func setupKMSUser(t *testing.T, accessKey, secretKey, p string) {
	ctx := t.Context()
	createUserParams := madmin.AddOrUpdateUserReq{
		SecretKey: secretKey,
		Status:    madmin.AccountEnabled,
	}
	_, err := globalIAMSys.CreateUser(ctx, accessKey, createUserParams)
	if err != nil {
		t.Fatal(err)
	}

	testKMSPolicyName := "testKMSPolicy"
	if p != "" {
		p = `{"Version":"2012-10-17","Statement":[` + p + `]}`
		policyData, err := policy.ParseConfig(strings.NewReader(p))
		if err != nil {
			t.Fatal(err)
		}
		_, err = globalIAMSys.SetPolicy(ctx, testKMSPolicyName, *policyData)
		if err != nil {
			t.Fatal(err)
		}
		_, err = globalIAMSys.PolicyDBSet(ctx, accessKey, testKMSPolicyName, regUser, false)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		err = globalIAMSys.DeletePolicy(ctx, testKMSPolicyName, false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = globalIAMSys.PolicyDBSet(ctx, accessKey, "", regUser, false)
		if err != nil {
			t.Fatal(err)
		}
	}
}
