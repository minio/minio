/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	xhttp "github.com/minio/minio/cmd/http"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio/cmd/crypto"
)

// Tests getRedirectLocation function for all its criteria.
func TestRedirectLocation(t *testing.T) {
	testCases := []struct {
		urlPath  string
		location string
	}{
		{
			// 1. When urlPath is '/minio'
			urlPath:  minioReservedBucketPath,
			location: minioReservedBucketPath + SlashSeparator,
		},
		{
			// 2. When urlPath is '/'
			urlPath:  SlashSeparator,
			location: minioReservedBucketPath + SlashSeparator,
		},
		{
			// 3. When urlPath is '/webrpc'
			urlPath:  "/webrpc",
			location: minioReservedBucketPath + "/webrpc",
		},
		{
			// 4. When urlPath is '/login'
			urlPath:  "/login",
			location: minioReservedBucketPath + "/login",
		},
		{
			// 5. When urlPath is '/favicon-16x16.png'
			urlPath:  "/favicon-16x16.png",
			location: minioReservedBucketPath + "/favicon-16x16.png",
		},
		{
			// 6. When urlPath is '/favicon-16x16.png'
			urlPath:  "/favicon-32x32.png",
			location: minioReservedBucketPath + "/favicon-32x32.png",
		},
		{
			// 7. When urlPath is '/favicon-96x96.png'
			urlPath:  "/favicon-96x96.png",
			location: minioReservedBucketPath + "/favicon-96x96.png",
		},
		{
			// 8. When urlPath is '/unknown'
			urlPath:  "/unknown",
			location: "",
		},
	}

	// Validate all conditions.
	for i, testCase := range testCases {
		loc := getRedirectLocation(testCase.urlPath)
		if testCase.location != loc {
			t.Errorf("Test %d: Unexpected location expected %s, got %s", i+1, testCase.location, loc)
		}
	}
}

// Tests request guess function for net/rpc requests.
func TestGuessIsRPC(t *testing.T) {
	if guessIsRPCReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}

	u, err := url.Parse("http://localhost:9000/minio/lock")
	if err != nil {
		t.Fatal(err)
	}

	r := &http.Request{
		Proto:  "HTTP/1.0",
		Method: http.MethodPost,
		URL:    u,
	}
	if !guessIsRPCReq(r) {
		t.Fatal("Test shouldn't fail for a possible net/rpc request.")
	}
	r = &http.Request{
		Proto:  "HTTP/1.1",
		Method: http.MethodGet,
	}
	if guessIsRPCReq(r) {
		t.Fatal("Test shouldn't report as net/rpc for a non net/rpc request.")
	}
}

// Tests browser request guess function.
func TestGuessIsBrowser(t *testing.T) {
	globalBrowserEnabled = true
	if guessIsBrowserReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}
	r := &http.Request{
		Header: http.Header{},
		URL:    &url.URL{},
	}
	r.Header.Set("User-Agent", "Mozilla")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request anonymous user")
	}
	r.Header.Set("Authorization", "Bearer token")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request JWT user")
	}
	r = &http.Request{
		Header: http.Header{},
		URL:    &url.URL{},
	}
	r.Header.Set("User-Agent", "mc")
	if guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't report as browser for a non browser request.")
	}
}

var isHTTPHeaderSizeTooLargeTests = []struct {
	header     http.Header
	shouldFail bool
}{
	{header: generateHeader(0, 0), shouldFail: false},
	{header: generateHeader(1024, 0), shouldFail: false},
	{header: generateHeader(2048, 0), shouldFail: false},
	{header: generateHeader(8*1024+1, 0), shouldFail: true},
	{header: generateHeader(0, 1024), shouldFail: false},
	{header: generateHeader(0, 2048), shouldFail: true},
	{header: generateHeader(0, 2048+1), shouldFail: true},
}

func generateHeader(size, usersize int) http.Header {
	header := http.Header{}
	for i := 0; i < size; i++ {
		header.Add(strconv.Itoa(i), "")
	}
	userlength := 0
	for i := 0; userlength < usersize; i++ {
		userlength += len(userMetadataKeyPrefixes[0] + strconv.Itoa(i))
		header.Add(userMetadataKeyPrefixes[0]+strconv.Itoa(i), "")
	}
	return header
}

func TestIsHTTPHeaderSizeTooLarge(t *testing.T) {
	for i, test := range isHTTPHeaderSizeTooLargeTests {
		if res := isHTTPHeaderSizeTooLarge(test.header); res != test.shouldFail {
			t.Errorf("Test %d: Expected %v got %v", i, res, test.shouldFail)
		}
	}
}

var containsReservedMetadataTests = []struct {
	header     http.Header
	shouldFail bool
}{
	{
		header: http.Header{"X-Minio-Key": []string{"value"}},
	},
	{
		header:     http.Header{crypto.SSEIV: []string{"iv"}},
		shouldFail: true,
	},
	{
		header:     http.Header{crypto.SSESealAlgorithm: []string{crypto.InsecureSealAlgorithm}},
		shouldFail: true,
	},
	{
		header:     http.Header{crypto.SSECSealedKey: []string{"mac"}},
		shouldFail: true,
	},
	{
		header:     http.Header{ReservedMetadataPrefix + "Key": []string{"value"}},
		shouldFail: true,
	},
}

func TestContainsReservedMetadata(t *testing.T) {
	for i, test := range containsReservedMetadataTests {
		if contains := containsReservedMetadata(test.header); contains && !test.shouldFail {
			t.Errorf("Test %d: contains reserved header but should not fail", i)
		} else if !contains && test.shouldFail {
			t.Errorf("Test %d: does not contain reserved header but failed", i)
		}
	}
}

var sseTLSHandlerTests = []struct {
	URL               *url.URL
	Header            http.Header
	IsTLS, ShouldFail bool
}{
	{URL: &url.URL{}, Header: http.Header{}, IsTLS: false, ShouldFail: false},                                        // 0
	{URL: &url.URL{}, Header: http.Header{crypto.SSECAlgorithm: []string{"AES256"}}, IsTLS: false, ShouldFail: true}, // 1
	{URL: &url.URL{}, Header: http.Header{crypto.SSECAlgorithm: []string{"AES256"}}, IsTLS: true, ShouldFail: false}, // 2
	{URL: &url.URL{}, Header: http.Header{crypto.SSECKey: []string{""}}, IsTLS: true, ShouldFail: false},             // 3
	{URL: &url.URL{}, Header: http.Header{crypto.SSECopyAlgorithm: []string{""}}, IsTLS: false, ShouldFail: true},    // 4
}

func TestSSETLSHandler(t *testing.T) {
	defer func(isSSL bool) { globalIsSSL = isSSL }(globalIsSSL) // reset globalIsSSL after test

	var okHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	for i, test := range sseTLSHandlerTests {
		globalIsSSL = test.IsTLS

		w := httptest.NewRecorder()
		r := new(http.Request)
		r.Header = test.Header
		r.URL = test.URL

		h := setSSETLSHandler(okHandler)
		h.ServeHTTP(w, r)

		switch {
		case test.ShouldFail && w.Code == http.StatusOK:
			t.Errorf("Test %d: should fail but status code is HTTP %d", i, w.Code)
		case !test.ShouldFail && w.Code != http.StatusOK:
			t.Errorf("Test %d: should not fail but status code is HTTP %d and not 200 OK", i, w.Code)
		}
	}
}

func Test_guessIsHealthCheckReq(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type testCase struct {
		name string
		args args
		want bool
	}
	tests := []testCase{
		{
			name: "Nil Request",
			args: args{
				req: nil,
			},
			want: false,
		}, {
			name: "Valid GET HealthCheck Request",
			args: args{
				req: &http.Request{
					Method: http.MethodGet,
					URL:    &url.URL{Path: healthCheckPathPrefix + healthCheckLivenessPath},
				},
			},
			want: true,
		}, {
			name: "Valid HEAD HealthCheck Request",
			args: args{
				req: &http.Request{
					Method: http.MethodHead,
					URL:    &url.URL{Path: healthCheckPathPrefix + healthCheckLivenessPath},
				},
			},
			want: true,
		}, {
			name: "Invalid HEAD Request",
			args: args{
				req: &http.Request{
					Method: http.MethodHead,
					URL:    &url.URL{Path: healthCheckPathPrefix},
				},
			},
			want: false,
		}, {
			name: "Invalid GET Request",
			args: args{
				req: &http.Request{
					Method: http.MethodGet,
					URL:    &url.URL{Path: healthCheckPathPrefix},
				},
			},
			want: false,
		}, {
			name: "Valid GET Readiness Check Request",
			args: args{
				req: &http.Request{
					Method: http.MethodGet,
					URL:    &url.URL{Path: healthCheckPathPrefix + healthCheckReadinessPath},
				},
			},
			want: true,
		}, {
			name: "Valid HEAD Readiness Check Request",
			args: args{
				req: &http.Request{
					Method: http.MethodHead,
					URL:    &url.URL{Path: healthCheckPathPrefix + healthCheckReadinessPath},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := guessIsHealthCheckReq(tt.args.req); got != tt.want {
				t.Errorf("guessIsHealthCheckReq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_guessIsMetricsReq(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type testCase struct {
		name string
		args args
		want bool
	}
	tests := []testCase{
		{
			name: "Nil request should return false",
			args: args{
				req: nil,
			},
			want: false,
		}, {
			name: "Empty url should return false",
			args: args{
				req: &http.Request{
					Method: http.MethodGet,
					URL:    &url.URL{},
				},
			},
			want: false,
		}, {
			name: "Valid url should return true",
			args: args{
				req: &http.Request{
					Method: http.MethodPost,
					URL:    &url.URL{Path: minioReservedBucketPath + prometheusMetricsPath},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := guessIsMetricsReq(tt.args.req); got != tt.want {
				t.Errorf("guessIsMetricsReq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isAdminReq(t *testing.T) {
	type args struct {
		r *http.Request
	}
	type testCase struct {
		name string
		args args
		want bool
	}
	tests := []testCase{
		{
			name: "Empty path should return false",
			args: args{
				r: &http.Request{
					Method: http.MethodGet,
					URL: &url.URL{
						Path: "",
					},
				},
			},
			want: false,
		}, {
			name: "Invalid path should return false",
			args: args{
				r: &http.Request{
					Method: http.MethodGet,
					URL: &url.URL{
						Path: "/minio/admi",
					},
				},
			},
			want: false,
		}, {
			name: "Invalid path should return false",
			args: args{
				r: &http.Request{
					Method: http.MethodGet,
					URL: &url.URL{
						Path: "fake/minio/admin",
					},
				},
			},
			want: false,
		}, {
			name: "Valid path should return true",
			args: args{
				r: &http.Request{
					Method: http.MethodGet,
					URL: &url.URL{
						Path: "/minio/admin",
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAdminReq(tt.args.r); got != tt.want {
				t.Errorf("isAdminReq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseAmzDate(t *testing.T) {
	type args struct {
		amzDateStr string
	}
	type testCase struct {
		name        string
		args        args
		wantAmzDate time.Time
		wantAPIErr  APIErrorCode
	}

	tests := []testCase{
		{
			name:        "Empty time should return ErrMalformedDate",
			args:        args{},
			wantAmzDate: time.Time{},
			wantAPIErr:  ErrMalformedDate,
		},
	}
	_time := time.Now()
	for _, dateFormat := range amzDateFormats {
		expected, _ := time.Parse(dateFormat, _time.UTC().Format(dateFormat))
		tests = append(tests, testCase{
			name: "Valid " + dateFormat,
			args: args{
				amzDateStr: _time.Format(dateFormat),
			},
			wantAmzDate: expected,
			wantAPIErr:  ErrNone,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAmzDate, gotAPIErr := parseAmzDate(tt.args.amzDateStr)
			if !gotAmzDate.Equal(tt.wantAmzDate) {
				t.Errorf("parseAmzDate() gotAmzDate = %v, want %v", gotAmzDate, tt.wantAmzDate)
			}
			if gotAPIErr != tt.wantAPIErr {
				t.Errorf("parseAmzDate() gotAPIErr = %v, want %v", gotAPIErr, tt.wantAPIErr)
			}
		})
	}
}

func Test_parseAmzDateHeader(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type testCase struct {
		name         string
		args         args
		want         time.Time
		apiErrorCode APIErrorCode
	}

	tests := []testCase{
		{
			name: "",
			args: args{
				req: &http.Request{},
			},
			want:         time.Time{},
			apiErrorCode: ErrMissingDateHeader,
		},
	}
	_time := time.Now()
	for _, dateFormat := range amzDateFormats {
		for _, amzDateHeader := range amzDateHeaders {
			expected, _ := time.Parse(dateFormat, _time.Format(dateFormat))
			header := http.Header{}
			header.Set(http.CanonicalHeaderKey(amzDateHeader), _time.Format(dateFormat))
			tests = append(tests, testCase{
				name: "Valid " + amzDateHeader + " " + dateFormat,
				args: args{
					req: &http.Request{
						Header: header,
					},
				},
				want:         expected,
				apiErrorCode: ErrNone,
			})

			// invalid headers
			invalidHeader := http.Header{}
			invalidHeader.Set(http.CanonicalHeaderKey(string(getRandomByte())), _time.Format(dateFormat))
			tests = append(tests, testCase{
				name: "Invalid " + amzDateHeader + " " + dateFormat,
				args: args{
					req: &http.Request{
						Header: invalidHeader,
					},
				},
				want:         time.Time{},
				apiErrorCode: ErrMissingDateHeader,
			})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTime, gotAPIErrorCode := parseAmzDateHeader(tt.args.req)
			if !gotTime.Equal(tt.want) {
				t.Errorf("parseAmzDateHeader() got = %v, want %v", gotTime, tt.want)
			}
			if gotAPIErrorCode != tt.apiErrorCode {
				t.Errorf("parseAmzDateHeader() apiErrorCode = %v, want %v", gotAPIErrorCode, tt.apiErrorCode)
			}
		})
	}
}

func Test_ignoreNotImplementedBucketResources(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type testCase struct {
		name string
		args args
		want bool
	}
	tests := []testCase{
		{
			name: "Empty request",
			args: args{
				req: &http.Request{
					URL: &url.URL{},
				},
			},
			want: false,
		},
	}

	enabledResources := map[string][]string{
		"acl":            {http.MethodGet},
		"cors":           {http.MethodGet},
		"website":        {http.MethodGet, http.MethodDelete},
		"accelerate":     {http.MethodGet},
		"requestPayment": {http.MethodGet},
		"logging":        {http.MethodGet},
		"lifecycle":      {http.MethodGet},
		"replication":    {http.MethodGet},
		"tagging":        {http.MethodGet, http.MethodDelete},
		"versioning":     {http.MethodGet},
	}

	for resource, methods := range enabledResources {
		for _, method := range methods {
			tests = append(tests, testCase{
				name: "Not ignore bucket resource " + resource + " " + method,
				args: args{
					req: &http.Request{
						Method: method,
						URL: &url.URL{
							RawQuery: resource,
						},
					},
				},
				want: false,
			})
		}
	}

	for resource := range notimplementedBucketResourceNames {
		tests = append(tests, testCase{
			name: "Ignore bucket resource " + resource,
			args: args{
				req: &http.Request{
					Method: http.MethodPost,
					URL: &url.URL{
						RawQuery: resource,
					},
				},
			},
			want: true,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ignoreNotImplementedBucketResources(tt.args.req); got != tt.want {
				t.Errorf("ignoreNotImplementedBucketResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ignoreNotImplementedObjectResources(t *testing.T) {
	type args struct {
		req *http.Request
	}
	type testCase struct {
		name string
		args args
		want bool
	}
	tests := []testCase{
		{
			name: "Empty request",
			args: args{
				req: &http.Request{
					URL: &url.URL{},
				},
			},
			want: false,
		},
	}

	enabledResources := map[string][]string{
		"acl":     {http.MethodGet},
		"tagging": {http.MethodGet},
	}

	for resource, methods := range enabledResources {
		for _, method := range methods {
			tests = append(tests, testCase{
				name: "Not ignore bucket resource " + resource + " " + method,
				args: args{
					req: &http.Request{
						Method: method,
						URL: &url.URL{
							RawQuery: resource,
						},
					},
				},
				want: false,
			})
		}
	}

	for resource := range notimplementedObjectResourceNames {
		tests = append(tests, testCase{
			name: "Ignore bucket resource " + resource,
			args: args{
				req: &http.Request{
					Method: http.MethodPost,
					URL: &url.URL{
						RawQuery: resource,
					},
				},
			},
			want: true,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ignoreNotImplementedObjectResources(tt.args.req); got != tt.want {
				t.Errorf("ignoreNotImplementedObjectResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasBadPathComponent(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty",
			args: args{},
			want: false,
		}, {
			name: "dotdotComponent",
			args: args{
				path: "../admin",
			},
			want: true,
		}, {
			name: "dotdotComponent",
			args: args{
				path: "/test/../admin",
			},
			want: true,
		}, {
			name: "dotComponent",
			args: args{
				path: "./admin",
			},
			want: true,
		}, {
			name: "dotComponent",
			args: args{
				path: "/test/./admin",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasBadPathComponent(tt.args.path); got != tt.want {
				t.Errorf("hasBadPathComponent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasMultipleAuth(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty",
			args: args{
				r: &http.Request{
					URL: &url.URL{},
				},
			},
			want: false,
		}, {
			name: "Single Auth jwt",
			args: args{
				r: &http.Request{
					Header: http.Header{
						xhttp.Authorization: {jwtAlgorithm},
					},
					URL: &url.URL{},
				},
			},
			want: false,
		}, {
			name: "Multiple Auth (jwt, PresignedSignatureV2)",
			args: args{
				r: &http.Request{
					Header: http.Header{
						xhttp.Authorization: {jwtAlgorithm},
					},
					URL: &url.URL{
						RawQuery: xhttp.AmzAccessKeyID,
					},
				},
			},
			want: true,
		}, {
			name: "Multiple Auth (signV4Algorithm, PresignedSignatureV2)",
			args: args{
				r: &http.Request{
					Header: http.Header{
						xhttp.Authorization: {signV4Algorithm},
					},
					URL: &url.URL{
						RawQuery: xhttp.AmzAccessKeyID,
					},
				},
			},
			want: true,
		}, {
			name: "Multiple Auth (signV2Algorithm, PresignedSignatureV2)",
			args: args{
				r: &http.Request{
					Header: http.Header{
						xhttp.Authorization: {signV2Algorithm},
					},
					URL: &url.URL{
						RawQuery: xhttp.AmzAccessKeyID,
					},
				},
			},
			want: true,
		}, {
			name: "Multiple Auth (signV2Algorithm, RequestSignatureV4)",
			args: args{
				r: &http.Request{
					Header: http.Header{
						xhttp.Authorization: {signV2Algorithm},
					},
					URL: &url.URL{
						RawQuery: xhttp.AmzCredential,
					},
				},
			},
			want: true,
		}, {
			name: "Multiple Auth (signV2Algorithm, PostPolicySignatureV4)",
			args: args{
				r: &http.Request{
					Method: http.MethodPost,
					Header: http.Header{
						xhttp.ContentType: {"multipart/form-data"},
					},
					URL: &url.URL{
						RawQuery: xhttp.AmzCredential,
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasMultipleAuth(tt.args.r); got != tt.want {
				t.Errorf("hasMultipleAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_guessIsLoginSTSReq(t *testing.T) {
	type args struct {
		req *http.Request
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty",
			args: args{
				req: &http.Request{URL: &url.URL{}},
			},
			want: false,
		}, {
			name: "Nil request",
			args: args{
				req: nil,
			},
			want: false,
		}, {
			name: "loginPathPrefix",
			args: args{
				req: &http.Request{
					URL: &url.URL{
						Path: loginPathPrefix,
					},
				},
			},
			want: true,
		}, {
			name: "Not valid STS",
			args: args{
				req: &http.Request{
					Method: http.MethodPost,
					URL:    &url.URL{},
				},
			},
			want: false,
		}, {
			name: "Valid STS",
			args: args{
				req: &http.Request{
					Method: http.MethodPost,
					URL: &url.URL{
						Path:     SlashSeparator,
						RawQuery: xhttp.Action,
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := guessIsLoginSTSReq(tt.args.req); got != tt.want {
				t.Errorf("guessIsLoginSTSReq() = %v, want %v", got, tt.want)
			}
		})
	}
}
