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
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	xcors "github.com/minio/pkg/v3/cors"
	rscors "github.com/rs/cors"
)

type corsRuleMatcherTest struct {
	name       string
	reqMethod  string
	reqHeaders http.Header
	wantOpts   *rscors.Options
}

func TestCorsRuleMatcher(t *testing.T) {
	testRule := []struct {
		name   string
		config []xcors.Rule
		tests  []corsRuleMatcherTest
	}{
		{
			name: "rule with wildcard origin and get method",
			config: []xcors.Rule{
				{
					AllowedOrigin: []string{"*"},
					AllowedMethod: []string{"GET"},
				},
			},
			tests: []corsRuleMatcherTest{
				{
					name:      "get method match",
					reqMethod: http.MethodGet,
					reqHeaders: http.Header{
						"Origin": []string{"http://example.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"*"},
						AllowedMethods:   []string{"GET"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "post method no match",
					reqMethod: http.MethodPost,
					reqHeaders: http.Header{
						"Origin": []string{"http://example.com"},
					},
					wantOpts: nil,
				},
				{
					name:      "preflight match",
					reqMethod: http.MethodOptions,
					reqHeaders: http.Header{
						"Origin":                        []string{"http://example.com"},
						"Access-Control-Request-Method": []string{"GET"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"*"},
						AllowedMethods:   []string{"GET"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "preflight no match wrong headers",
					reqMethod: http.MethodOptions,
					reqHeaders: http.Header{
						"Origin":                         []string{"http://example.com"},
						"Access-Control-Request-Method":  []string{"GET"},
						"Access-Control-Request-Headers": []string{"X-Custom-Header"},
					},
					wantOpts: nil,
				},
				{
					name:      "preflight no match wrong method",
					reqMethod: http.MethodOptions,
					reqHeaders: http.Header{
						"Origin":                        []string{"http://example.com"},
						"Access-Control-Request-Method": []string{"POST"},
					},
					wantOpts: nil,
				},
			},
		},
		{
			// example 1 from https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html#cors-allowed-origin
			name: "aws example 1",
			config: []xcors.Rule{
				{
					AllowedOrigin: []string{"http://www.example1.com"},
					AllowedMethod: []string{"PUT", "POST", "DELETE"},
					AllowedHeader: []string{"*"},
				},
				{
					AllowedOrigin: []string{"http://www.example2.com"},
					AllowedMethod: []string{"PUT", "POST", "DELETE"},
					AllowedHeader: []string{"*"},
				},
				{
					AllowedOrigin: []string{"*"},
					AllowedMethod: []string{"GET"},
				},
			},
			tests: []corsRuleMatcherTest{
				{
					name:      "put match example1 origin",
					reqMethod: http.MethodPut,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example1.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example1.com"},
						AllowedMethods:   []string{"PUT", "POST", "DELETE"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "put match example2 origin",
					reqMethod: http.MethodPut,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example2.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example2.com"},
						AllowedMethods:   []string{"PUT", "POST", "DELETE"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "get match wildcard origin",
					reqMethod: http.MethodGet,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example3.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"*"},
						AllowedMethods:   []string{"GET"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "post no match wrong method",
					reqMethod: http.MethodPost,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example3.com"},
					},
					wantOpts: nil,
				},
				{
					name:      "preflight example1 origin",
					reqMethod: http.MethodOptions,
					reqHeaders: http.Header{
						"Origin":                         []string{"http://www.example1.com"},
						"Access-Control-Request-Method":  []string{"PUT"},
						"Access-Control-Request-Headers": []string{"x-custom-header,x-custom-header2"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example1.com"},
						AllowedMethods:   []string{"PUT", "POST", "DELETE"},
						AllowedHeaders:   []string{"x-custom-header", "x-custom-header2"},
						AllowCredentials: true,
					},
				},
			},
		},
		{
			// example 2 from https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html#cors-allowed-origin
			name: "aws example 2",
			config: []xcors.Rule{
				{
					AllowedOrigin: []string{"http://www.example.com"},
					AllowedMethod: []string{"PUT", "POST", "DELETE"},
					AllowedHeader: []string{"*"},
					MaxAgeSeconds: 3000,
					ExposeHeader:  []string{"x-amz-server-side-encryption", "x-amz-request-id", "x-amz-id-2"},
				},
			},
			tests: []corsRuleMatcherTest{
				{
					name:      "put match example",
					reqMethod: http.MethodPut,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example.com"},
						AllowedMethods:   []string{"PUT", "POST", "DELETE"},
						AllowedHeaders:   []string{},
						MaxAge:           3000,
						AllowCredentials: true,
						ExposedHeaders:   []string{"x-amz-server-side-encryption", "x-amz-request-id", "x-amz-id-2"},
					},
				},
				{
					name: "preflight match",
					reqHeaders: http.Header{
						"Origin":                        []string{"http://www.example.com"},
						"Access-Control-Request-Method": []string{"PUT"},
					},
					reqMethod: http.MethodOptions,
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example.com"},
						AllowedMethods:   []string{"PUT", "POST", "DELETE"},
						AllowedHeaders:   []string{},
						MaxAge:           3000,
						AllowCredentials: true,
						ExposedHeaders:   []string{"x-amz-server-side-encryption", "x-amz-request-id", "x-amz-id-2"},
					},
				},
			},
		},
		{
			name: "wildcard headers rule",
			config: []xcors.Rule{
				{
					AllowedOrigin: []string{"http://www.example5.com"},
					AllowedMethod: []string{"PUT", "POST"},
					AllowedHeader: []string{"x-custom-*", "x-zzz-*"},
				},
			},
			tests: []corsRuleMatcherTest{
				{
					name:      "put match example",
					reqMethod: http.MethodPut,
					reqHeaders: http.Header{
						"Origin": []string{"http://www.example5.com"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example5.com"},
						AllowedMethods:   []string{"PUT", "POST"},
						AllowedHeaders:   []string{},
						AllowCredentials: true,
					},
				},
				{
					name:      "put no match wrong requested headers",
					reqMethod: http.MethodPut,
					reqHeaders: http.Header{
						"Origin":                         []string{"http://www.example5.com"},
						"Access-Control-Request-Headers": []string{"x-custom-header,x-def-header,x-zzz-header"},
					},
					wantOpts: nil,
				},
				{
					name:      "preflight match",
					reqMethod: http.MethodOptions,
					reqHeaders: http.Header{
						"Origin":                         []string{"http://www.example5.com"},
						"Access-Control-Request-Method":  []string{"PUT"},
						"Access-Control-Request-Headers": []string{"x-custom-header,x-zzz-header"},
					},
					wantOpts: &rscors.Options{
						AllowedOrigins:   []string{"http://www.example5.com"},
						AllowedMethods:   []string{"PUT", "POST"},
						AllowedHeaders:   []string{"x-custom-header", "x-zzz-header"},
						AllowCredentials: true,
					},
				},
			},
		},
	}

	for _, trule := range testRule {
		for _, test := range trule.tests {
			t.Run(trule.name+"/"+test.name, func(t *testing.T) {
				req := httptest.NewRequest(test.reqMethod, "http://localhost", nil)
				req.Header = test.reqHeaders

				opts := findCorsRule(req, &xcors.Config{CORSRules: trule.config})
				if test.wantOpts == nil {
					if opts != nil {
						t.Errorf("want no match, got %+v", opts)
					}
					return
				}
				// Generic requirement for all options
				test.wantOpts.OptionsSuccessStatus = http.StatusOK
				if !reflect.DeepEqual(opts, test.wantOpts) {
					t.Fatalf("want %+v, got %+v", test.wantOpts, opts)
				}
			})
		}
	}
}
