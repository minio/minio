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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

// Test cross domain xml handler.
func TestCrossXMLHandler(t *testing.T) {
	// Server initialization.
	router := mux.NewRouter().SkipClean(true)
	handler := setCrossDomainPolicy(router)
	srv := httptest.NewServer(handler)

	resp, err := http.Get(srv.URL + crossDomainXMLEntity)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Unexpected http status received", resp.Status)
	}
}
