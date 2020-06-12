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
)

// ReadinessCheckHandler returns if the server is ready to receive requests.
// For FS - Checks if the backend disk is available
// For Erasure backend - Checks if all the erasure sets are writable
func ReadinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ReadinessCheckHandler")

	objLayer := newObjectLayerWithoutSafeModeFn()
	// Service not initialized yet
	if objLayer == nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getReadyDeadline())
	defer cancel()

	if !objLayer.IsReady(ctx) && newObjectLayerFn() == nil {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// LivenessCheckHandler - Checks if the process is up. Always returns success.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}
