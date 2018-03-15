/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"fmt"
	"net/http"
	"runtime"
)

const (
	minioHealthGoroutineThreshold = 1000
)

// ReadinessCheckHandler -- checks if there are more than threshold number of goroutines running,
// returns service unavailable.
// Readiness probes are used to detect situations where application is under heavy load
// and temporarily unable to serve. In a orchestrated setup like Kubernetes, containers reporting
// that they are not ready do not receive traffic through Kubernetes Services.
func ReadinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	if err := goroutineCountCheck(minioHealthGoroutineThreshold); err != nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// LivenessCheckHandler -- checks if server can ListBuckets internally. If not, server is
// considered to have failed and needs to be restarted.
// Liveness probes are used to detect situations where application (minio)
// has gone into a state where it can not recover except by being restarted.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}
	// List buckets is unsuccessful, means server is having issues, send 503 service unavailable
	if _, err := objLayer.ListBuckets(context.Background()); err != nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// checks threshold against total number of go-routines in the system and throws error if
// more than threshold go-routines are running.
func goroutineCountCheck(threshold int) error {
	count := runtime.NumGoroutine()
	if count > threshold {
		return fmt.Errorf("too many goroutines (%d > %d)", count, threshold)
	}
	return nil
}
