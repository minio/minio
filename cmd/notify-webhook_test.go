/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/Sirupsen/logrus"
)

// Custom post handler to handle POST requests.
type postHandler struct{}

func (p postHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, fmt.Sprintf("Unexpected method %s", r.Method), http.StatusBadRequest)
		return
	}
	io.Copy(w, r.Body)
}

type errorHandler struct{}

func (e errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, fmt.Sprintf("Unexpected method %s", r.Method), http.StatusBadRequest)
}

// Tests web hook initialization.
func TestNewWebHookNotify(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	server := httptest.NewServer(postHandler{})
	defer server.Close()

	_, err = newWebhookNotify("1")
	if err == nil {
		t.Fatal("Unexpected should fail")
	}

	globalServerConfig.Notify.SetWebhookByID("10", webhookNotify{Enable: true, Endpoint: server.URL})
	_, err = newWebhookNotify("10")
	if err != nil {
		t.Fatal("Unexpected should not fail with lookupEndpoint", err)
	}

	globalServerConfig.Notify.SetWebhookByID("15", webhookNotify{Enable: true, Endpoint: "http://%"})
	_, err = newWebhookNotify("15")
	if err == nil {
		t.Fatal("Unexpected should fail with invalid URL escape")
	}

	globalServerConfig.Notify.SetWebhookByID("20", webhookNotify{Enable: true, Endpoint: server.URL})
	webhook, err := newWebhookNotify("20")
	if err != nil {
		t.Fatal("Unexpected shouldn't fail", err)
	}

	webhook.WithFields(logrus.Fields{
		"Key":       path.Join("bucket", "object"),
		"EventType": "s3:ObjectCreated:Put",
	}).Info()
}

// Add tests for lookup endpoint.
func TestLookupEndpoint(t *testing.T) {
	server := httptest.NewServer(errorHandler{})
	defer server.Close()

	testCases := []struct {
		endpoint string
		err      error
	}{
		// Ignore endpoints which don't exist.
		{
			endpoint: "http://unknown",
			err:      nil,
		},
		{
			endpoint: "%%%",
			err:      errors.New("parse %%%: invalid URL escape \"%%%\""),
		},
		{
			endpoint: server.URL,
			err:      fmt.Errorf("Unable to lookup webhook endpoint %s response(400 Bad Request)", server.URL),
		},
	}
	for _, test := range testCases {
		if err := lookupEndpoint(test.endpoint); err != nil {
			if err.Error() != test.err.Error() {
				t.Errorf("Expected %s, got %s", test.err, err)
			}
		}
	}
}
