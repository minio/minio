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
	"net/url"
	"time"
)

const (
	minioAdminOpHeader = "X-Minio-Operation"
)

// ServiceStatusHandler - GET /?service
// HTTP header x-minio-operation: status
// ----------
// Fetches server status information like total disk space available
// to use, online disks, offline disks and quorum threshold.
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
	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponse(w, jsonBytes)
}

// ServiceStopHandler - POST /?service
// HTTP header x-minio-operation: stop
// ----------
// Stops minio server gracefully. In a distributed setup, stops all the
// servers in the cluster.
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
// Restarts minio server gracefully. In a distributed setup,  restarts
// all the servers in the cluster.
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

// Type-safe lock query params.
type lockQueryKey string

// Only valid query params for list/clear locks management APIs.
const (
	lockBucket    lockQueryKey = "bucket"
	lockPrefix    lockQueryKey = "prefix"
	lockOlderThan lockQueryKey = "older-than"
)

// validateLockQueryParams - Validates query params for list/clear locks management APIs.
func validateLockQueryParams(vars url.Values) (string, string, time.Duration, APIErrorCode) {
	bucket := vars.Get(string(lockBucket))
	prefix := vars.Get(string(lockPrefix))
	relTimeStr := vars.Get(string(lockOlderThan))

	// N B empty bucket name is invalid
	if !IsValidBucketName(bucket) {
		return "", "", time.Duration(0), ErrInvalidBucketName
	}

	// empty prefix is valid.
	if !IsValidObjectPrefix(prefix) {
		return "", "", time.Duration(0), ErrInvalidObjectName
	}

	// If older-than parameter was empty then set it to 0s to list
	// all locks older than now.
	if relTimeStr == "" {
		relTimeStr = "0s"
	}
	relTime, err := time.ParseDuration(relTimeStr)
	if err != nil {
		errorIf(err, "Failed to parse duration passed as query value.")
		return "", "", time.Duration(0), ErrInvalidDuration
	}

	return bucket, prefix, relTime, ErrNone
}

// ListLocksHandler - GET /?lock&bucket=mybucket&prefix=myprefix&older-than=rel_time
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// HTTP header x-minio-operation: list
// ---------
// Lists locks held on a given bucket, prefix and relative time.
func (adminAPI adminAPIHandlers) ListLocksHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, relTime, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are available since relTime.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix, relTime)
	if err != nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}

	// Reply with list of locks held on bucket, matching prefix
	// older than relTime supplied, as json.
	writeSuccessResponse(w, jsonBytes)
}

// ClearLocksHandler - POST /?lock&bucket=mybucket&prefix=myprefix&older-than=relTime
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// HTTP header x-minio-operation: clear
// ---------
// Clear locks held on a given bucket, prefix and relative time.
func (adminAPI adminAPIHandlers) ClearLocksHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, relTime, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, r, adminAPIErr, r.URL.Path)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are available since relTime.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix, relTime)
	if err != nil {
		writeErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}
	// Remove lock matching bucket/prefix older than relTime.
	for _, volLock := range volLocks {
		globalNSMutex.ForceUnlock(volLock.Bucket, volLock.Object)
	}
	// Reply with list of locks cleared, as json.
	writeSuccessResponse(w, jsonBytes)
}
