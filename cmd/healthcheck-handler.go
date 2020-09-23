/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"net/http"
	"strconv"
)

// ClusterCheckHandler returns if the server is ready for requests.
func ClusterCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ClusterCheckHandler")

	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getClusterDeadline())
	defer cancel()

	opts := HealthOptions{Maintenance: r.URL.Query().Get("maintenance") == "true"}
	result := objLayer.Health(ctx, opts)
	if result.WriteQuorum > 0 {
		w.Header().Set("X-Minio-Write-Quorum", strconv.Itoa(result.WriteQuorum))
	}
	if !result.Healthy {
		// return how many drives are being healed if any
		if result.HealingDrives > 0 {
			w.Header().Set("X-Minio-Healing-Drives", strconv.Itoa(result.HealingDrives))
		}
		// As a maintenance call we are purposefully asked to be taken
		// down, this is for orchestrators to know if we can safely
		// take this server down, return appropriate error.
		if opts.Maintenance {
			writeResponse(w, http.StatusPreconditionFailed, nil, mimeNone)
		} else {
			writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		}
		return
	}
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// ReadinessCheckHandler Checks if the process is up. Always returns success.
func ReadinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: only implement this function to notify that this pod is
	// busy, at a local scope in future, for now '200 OK'.
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// LivenessCheckHandler - Checks if the process is up. Always returns success.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}
