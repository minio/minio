/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

import "net/http"

const (
	minioAdminOpHeader = "X-Minio-Operation"
)

func (adminAPI adminAPIHandlers) ServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	adminErr := checkRequestAuthType(r, "", "", "")
	if adminErr != ErrNone {
		writeErrorResponse(w, r, adminErr, r.URL.Path)
		return
	}
	// FIXME: Servicestatus command is not yet implemented in handleServiceSignals
	globalServiceSignalCh <- serviceStatus
	w.WriteHeader(http.StatusOK)
}

func (adminAPI adminAPIHandlers) ServiceStopHandler(w http.ResponseWriter, r *http.Request) {
	adminErr := checkRequestAuthType(r, "", "", "")
	if adminErr != ErrNone {
		writeErrorResponse(w, r, adminErr, r.URL.Path)
		return
	}
	// Reply to the client before stopping minio server.
	w.WriteHeader(http.StatusOK)
	globalServiceSignalCh <- serviceStop
}

func (adminAPI adminAPIHandlers) ServiceRestartHandler(w http.ResponseWriter, r *http.Request) {
	adminErr := checkRequestAuthType(r, "", "", "")
	if adminErr != ErrNone {
		writeErrorResponse(w, r, adminErr, r.URL.Path)
		return
	}
	// Reply to the client before restarting minio server.
	w.WriteHeader(http.StatusOK)
	globalServiceSignalCh <- serviceRestart
}
