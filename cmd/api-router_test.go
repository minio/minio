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
)

// Test that CORS credentials are properly handled for wildcard origins
func TestCORSCredentialsWithWildcard(t *testing.T) {
	// Save original config and restore after test
	originalOrigins := globalAPIConfig.getCorsAllowOrigins()
	defer func() {
		globalAPIConfig.mu.Lock()
		globalAPIConfig.corsAllowOrigins = originalOrigins
		globalAPIConfig.mu.Unlock()
	}()

	// Setup wildcard CORS config
	globalAPIConfig.mu.Lock()
	globalAPIConfig.corsAllowOrigins = []string{"*"}
	globalAPIConfig.mu.Unlock()

	// Create a simple handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with CORS handler
	corsWrappedHandler := corsHandler(handler)

	// Test preflight request
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	rr := httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// Verify specific origin is echoed back (rs/cors library behavior)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Expected Access-Control-Allow-Origin: https://example.com, got: %s", got)
	}

	// Verify credentials header is NOT present (security fix)
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Expected no Access-Control-Allow-Credentials header with wildcard origin, got: %s", got)
	}

	// Test actual GET request
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://example.com")

	rr = httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// Verify specific origin is echoed back (rs/cors library behavior)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Expected Access-Control-Allow-Origin: https://example.com, got: %s", got)
	}

	// Verify credentials header is NOT present
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Expected no Access-Control-Allow-Credentials header with wildcard origin, got: %s", got)
	}
}

// Test that CORS credentials are allowed for specific origins
func TestCORSCredentialsWithSpecificOrigin(t *testing.T) {
	// Save original config and restore after test
	originalOrigins := globalAPIConfig.getCorsAllowOrigins()
	defer func() {
		globalAPIConfig.mu.Lock()
		globalAPIConfig.corsAllowOrigins = originalOrigins
		globalAPIConfig.mu.Unlock()
	}()

	// Setup specific origin CORS config
	allowedOrigin := "https://example.com"
	globalAPIConfig.mu.Lock()
	globalAPIConfig.corsAllowOrigins = []string{allowedOrigin}
	globalAPIConfig.mu.Unlock()

	// Create a simple handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with CORS handler
	corsWrappedHandler := corsHandler(handler)

	// Test preflight request
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", allowedOrigin)
	req.Header.Set("Access-Control-Request-Method", "GET")

	rr := httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// Verify specific origin is echoed back
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != allowedOrigin {
		t.Errorf("Expected Access-Control-Allow-Origin: %s, got: %s", allowedOrigin, got)
	}

	// Verify credentials header IS present for specific origins
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Expected Access-Control-Allow-Credentials: true with specific origin, got: %s", got)
	}

	// Test actual GET request
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", allowedOrigin)

	rr = httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// Verify specific origin is echoed back
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != allowedOrigin {
		t.Errorf("Expected Access-Control-Allow-Origin: %s, got: %s", allowedOrigin, got)
	}

	// Verify credentials header IS present
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Expected Access-Control-Allow-Credentials: true with specific origin, got: %s", got)
	}
}

// Test that unauthorized origins are rejected properly
func TestCORSUnauthorizedOrigin(t *testing.T) {
	// Save original config and restore after test
	originalOrigins := globalAPIConfig.getCorsAllowOrigins()
	defer func() {
		globalAPIConfig.mu.Lock()
		globalAPIConfig.corsAllowOrigins = originalOrigins
		globalAPIConfig.mu.Unlock()
	}()

	// Setup specific origin CORS config (no wildcard)
	allowedOrigin := "https://example.com"
	globalAPIConfig.mu.Lock()
	globalAPIConfig.corsAllowOrigins = []string{allowedOrigin}
	globalAPIConfig.mu.Unlock()

	// Create a simple handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with CORS handler
	corsWrappedHandler := corsHandler(handler)

	// Test preflight request from unauthorized origin
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	rr := httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// Verify no CORS headers are set for unauthorized origin
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Expected no Access-Control-Allow-Origin header for unauthorized origin, got: %s", got)
	}

	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Expected no Access-Control-Allow-Credentials header for unauthorized origin, got: %s", got)
	}
}

// Test mixed configuration with both wildcard and specific origins
func TestCORSMixedConfiguration(t *testing.T) {
	// Save original config and restore after test
	originalOrigins := globalAPIConfig.getCorsAllowOrigins()
	defer func() {
		globalAPIConfig.mu.Lock()
		globalAPIConfig.corsAllowOrigins = originalOrigins
		globalAPIConfig.mu.Unlock()
	}()

	// Setup mixed CORS config (wildcard + specific origins)
	globalAPIConfig.mu.Lock()
	globalAPIConfig.corsAllowOrigins = []string{"*", "https://example.com"}
	globalAPIConfig.mu.Unlock()

	// Create a simple handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with CORS handler
	corsWrappedHandler := corsHandler(handler)

	// Test request that should match wildcard
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	rr := httptest.NewRecorder()
	corsWrappedHandler.ServeHTTP(rr, req)

	// The rs/cors library echoes back the specific origin even with wildcard config
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Expected Access-Control-Allow-Origin: https://example.com, got: %s", got)
	}

	// Verify credentials header is NOT present due to wildcard in config
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Expected no Access-Control-Allow-Credentials header with wildcard in config, got: %s", got)
	}
}
