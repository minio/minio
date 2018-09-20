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
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/minio/minio/cmd/logger"
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

// LivenessCheckHandler -- checks if server can reach its disks internally.
// If not, server is considered to have failed and needs to be restarted.
// Liveness probes are used to detect situations where application (minio)
// has gone into a state where it can not recover except by being restarted.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "LivenessCheckHandler")

	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	s := objLayer.StorageInfo(ctx)
	// Gateways don't provide disk info, also handle special case for NAS gateway.
	if s.Backend.Type == Unknown || s.Backend.Type == BackendFS {
		// ListBuckets to confirm gateway backend is up
		if _, err := objLayer.ListBuckets(ctx); err != nil {
			writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
			return
		}
		writeResponse(w, http.StatusOK, nil, mimeNone)
		return
	}

	var totalLocalDisks int
	var erroredDisks int
	for _, endpoint := range globalEndpoints {
		// Check only if local disks are accessible, we do not have
		// to reach to rest of the other servers in a distributed setup.
		if endpoint.IsLocal {
			totalLocalDisks++
			// Attempt a stat to backend, any error resulting
			// from this Stat() operation is considered as backend
			// is not available, count them as errors.
			if _, err := os.Stat(endpoint.Path); err != nil {
				logger.LogIf(ctx, err)
				erroredDisks++
			}
		}
	}

	// If all exported local disks have errored, we simply let kubernetes
	// take us down.
	if totalLocalDisks == erroredDisks {
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
