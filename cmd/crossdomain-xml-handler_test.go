// Copyright (c) 2015-2021 MinIO, Inc.
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
	"testing"

	"github.com/minio/mux"
)

// Test cross domain xml handler.
func TestCrossXMLHandler(t *testing.T) {
	// Server initialization.
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()
	handler := setCrossDomainPolicyMiddleware(router)
	srv := httptest.NewServer(handler)

	resp, err := http.Get(srv.URL + crossDomainXMLEntity)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Unexpected http status received", resp.Status)
	}
}
