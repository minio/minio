// Copyright (c) 2015-2025 MinIO, Inc.
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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/lambda"
	levent "github.com/minio/minio/internal/config/lambda/event"
	xhttp "github.com/minio/minio/internal/http"
)

func TestGetObjectLambdaHandler(t *testing.T) {
	testCases := []struct {
		name         string
		statusCode   int
		body         string
		contentType  string
		expectStatus int
	}{
		{
			name:         "Success 206 Partial Content",
			statusCode:   206,
			body:         "partial-object-data",
			contentType:  "text/plain",
			expectStatus: 206,
		},
		{
			name:         "Success 200 OK",
			statusCode:   200,
			body:         "full-object-data",
			contentType:  "application/json",
			expectStatus: 200,
		},
		{
			name:         "Client Error 400",
			statusCode:   400,
			body:         "bad-request",
			contentType:  "application/xml",
			expectStatus: 400,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runObjectLambdaTest(t, tc.statusCode, tc.body, tc.contentType, tc.expectStatus)
		})
	}
}

func runObjectLambdaTest(t *testing.T, lambdaStatus int, lambdaBody, contentType string, expectStatus int) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		objAPITest: func(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler, credentials auth.Credentials, t *testing.T) {
			objectName := "dummy-object"
			functionID := "lambda1"
			functionToken := "token123"

			// Lambda mock server
			lambdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(xhttp.AmzRequestRoute, functionID)
				w.Header().Set(xhttp.AmzRequestToken, functionToken)
				w.Header().Set(xhttp.AmzFwdHeaderContentType, contentType)
				w.Header().Set(xhttp.AmzFwdStatus, strconv.Itoa(lambdaStatus))
				w.WriteHeader(lambdaStatus)
				w.Write([]byte(lambdaBody))
			}))
			defer lambdaServer.Close()

			lambdaARN := "arn:minio:s3-object-lambda::lambda1:webhook"

			cfg := config.New()
			cfg[config.LambdaWebhookSubSys] = map[string]config.KVS{
				functionID: {
					{Key: "endpoint", Value: lambdaServer.URL},
					{Key: "enable", Value: config.EnableOn},
				},
			}
			cfg[config.APISubSys] = map[string]config.KVS{
				"api": {
					{Key: "gzip", Value: config.EnableOff},
				},
			}

			var err error
			globalLambdaTargetList, err = lambda.FetchEnabledTargets(context.Background(), cfg, http.DefaultTransport.(*http.Transport))
			if err != nil {
				t.Fatalf("failed to load lambda targets: %v", err)
			}

			getLambdaEventData = func(_, _ string, _ auth.Credentials, _ *http.Request) (levent.Event, error) {
				return levent.Event{
					GetObjectContext: &levent.GetObjectContext{
						OutputRoute: functionID,
						OutputToken: functionToken,
						InputS3URL:  "http://localhost/dummy",
					},
					UserRequest: levent.UserRequest{
						Headers: map[string][]string{},
					},
					UserIdentity: levent.Identity{
						PrincipalID: "test-user",
					},
				}, nil
			}

			body := []byte{}
			req := httptest.NewRequest("GET", "/objectlambda/"+bucketName+"/"+objectName+"?lambdaArn="+url.QueryEscape(lambdaARN), bytes.NewReader(body))
			req.Form = url.Values{"lambdaArn": []string{lambdaARN}}
			req.Header.Set("Host", "localhost")
			req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
			sum := sha256.Sum256(body)
			req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
			req = signer.SignV4(*req, credentials.AccessKey, credentials.SecretKey, "", "us-east-1")

			rec := httptest.NewRecorder()
			api := objectAPIHandlers{
				ObjectAPI: func() ObjectLayer {
					return obj
				},
			}
			api.GetObjectLambdaHandler(rec, req)

			res := rec.Result()
			defer res.Body.Close()
			respBody, _ := io.ReadAll(res.Body)

			if res.StatusCode != expectStatus {
				t.Errorf("Expected status %d, got %d", expectStatus, res.StatusCode)
			}

			if contentType != "" {
				if ct := res.Header.Get("Content-Type"); ct != contentType {
					t.Errorf("Expected Content-Type %q, got %q", contentType, ct)
				}
			}

			if res.StatusCode < 400 {
				if string(respBody) != lambdaBody {
					t.Errorf("Expected body %q, got %q", lambdaBody, string(respBody))
				}
			}
		},
		endpoints: []string{"GetObject"},
	})
}
