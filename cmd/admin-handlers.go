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

import (
	"encoding/json"
	"net/http"
)

const (
	minioAdminOpHeader = "X-Minio-Operation"
)

// ServiceStatusHandler - GET /?service
// HTTP header x-minio-operation: status
// ----------
// This implementation of the GET operation fetches server status information.
// provides total disk space available to use, online disks, offline disks and
// quorum threshold.
func (adminAPI adminAPIHandlers) ServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}
	storageInfo := newObjectLayerFn().StorageInfo()
	jsonBytes, err := json.Marshal(storageInfo)
	if err != nil {
		writeErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		errorIf(err, "Failed to marshal storage info into json.")
		return
	}
	writeSuccessResponse(w, jsonBytes)
}

// ServiceStopHandler - POST /?service
// HTTP header x-minio-operation: stop
// ----------
// This implementation of the POST operation stops minio server gracefully,
// in a distributed setup stops all the servers in the cluster. Body sent
// if any on client request is ignored.
func (adminAPI adminAPIHandlers) ServiceStopHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}
	// Reply to the client before stopping minio server.
	w.WriteHeader(http.StatusOK)
	sendServiceCmd(globalAdminPeers, serviceStop)
}

// ServiceRestartHandler - POST /?service
// HTTP header x-minio-operation: restart
// ----------
// This implementation of the POST operation restarts minio server gracefully,
// in a distributed setup restarts all the servers in the cluster. Body sent
// if any on client request is ignored.
func (adminAPI adminAPIHandlers) ServiceRestartHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}
	// Reply to the client before restarting minio server.
	w.WriteHeader(http.StatusOK)
	sendServiceCmd(globalAdminPeers, serviceRestart)
}
