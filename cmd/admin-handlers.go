/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	minioAdminOpHeader = "X-Minio-Operation"
)

// Type-safe query params.
type mgmtQueryKey string

// Only valid query params for list/clear locks management APIs.
const (
	mgmtBucket    mgmtQueryKey = "bucket"
	mgmtObject    mgmtQueryKey = "object"
	mgmtPrefix    mgmtQueryKey = "prefix"
	mgmtOlderThan mgmtQueryKey = "older-than"
	mgmtDelimiter mgmtQueryKey = "delimiter"
	mgmtMarker    mgmtQueryKey = "marker"
	mgmtMaxKey    mgmtQueryKey = "max-key"
	mgmtDryRun    mgmtQueryKey = "dry-run"
)

// ServerVersion - server version
type ServerVersion struct {
	Version  string `json:"version"`
	CommitID string `json:"commitID"`
}

// ServerStatus - contains the response of service status API
type ServerStatus struct {
	StorageInfo   StorageInfo   `json:"storageInfo"`
	ServerVersion ServerVersion `json:"serverVersion"`
}

// ServiceStatusHandler - GET /?service
// HTTP header x-minio-operation: status
// ----------
// Fetches server status information like total disk space available
// to use, online disks, offline disks and quorum threshold.
func (adminAPI adminAPIHandlers) ServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Fetch storage backend information
	storageInfo := newObjectLayerFn().StorageInfo()
	// Fetch server version
	serverVersion := ServerVersion{Version: Version, CommitID: CommitID}

	// Create API response
	serverStatus := ServerStatus{
		StorageInfo:   storageInfo,
		ServerVersion: serverVersion,
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(serverStatus)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal storage info into json.")
		return
	}
	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// ServiceRestartHandler - POST /?service
// HTTP header x-minio-operation: restart
// ----------
// Restarts minio server gracefully. In a distributed setup,  restarts
// all the servers in the cluster.
func (adminAPI adminAPIHandlers) ServiceRestartHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Reply to the client before restarting minio server.
	writeSuccessResponseHeadersOnly(w)

	sendServiceCmd(globalAdminPeers, serviceRestart)
}

// setCredsReq request
type setCredsReq struct {
	Username string `xml:"username"`
	Password string `xml:"password"`
}

// ServiceCredsHandler - POST /?service
// HTTP header x-minio-operation: creds
// ----------
// Update credentials in a minio server. In a distributed setup, update all the servers
// in the cluster.
func (adminAPI adminAPIHandlers) ServiceCredentialsHandler(w http.ResponseWriter, r *http.Request) {
	// Authenticate request
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Avoid setting new credentials when they are already passed
	// by the environment.
	if globalIsEnvCreds {
		writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
		return
	}

	// Load request body
	inputData, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// Unmarshal request body
	var req setCredsReq
	err = xml.Unmarshal(inputData, &req)
	if err != nil {
		errorIf(err, "Cannot unmarshal credentials request")
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}

	// Check passed credentials
	cred, err := getCredential(req.Username, req.Password)
	switch err {
	case errInvalidAccessKeyLength:
		writeErrorResponse(w, ErrAdminInvalidAccessKey, r.URL)
		return
	case errInvalidSecretKeyLength:
		writeErrorResponse(w, ErrAdminInvalidSecretKey, r.URL)
		return
	}

	// Notify all other Minio peers to update credentials
	updateErrs := updateCredsOnPeers(cred)
	for peer, err := range updateErrs {
		errorIf(err, "Unable to update credentials on peer %s.", peer)
	}

	// Update local credentials
	serverConfig.SetCredential(cred)
	if err = serverConfig.Save(); err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// At this stage, the operation is successful, return 200 OK
	w.WriteHeader(http.StatusOK)
}

// validateLockQueryParams - Validates query params for list/clear locks management APIs.
func validateLockQueryParams(vars url.Values) (string, string, time.Duration, APIErrorCode) {
	bucket := vars.Get(string(mgmtBucket))
	prefix := vars.Get(string(mgmtPrefix))
	relTimeStr := vars.Get(string(mgmtOlderThan))

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
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, relTime, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are available since relTime.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix, relTime)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}

	// Reply with list of locks held on bucket, matching prefix
	// older than relTime supplied, as json.
	writeSuccessResponseJSON(w, jsonBytes)
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
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, relTime, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are available since relTime.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix, relTime)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}

	// Remove lock matching bucket/prefix older than relTime.
	for _, volLock := range volLocks {
		globalNSMutex.ForceUnlock(volLock.Bucket, volLock.Object)
	}

	// Reply with list of locks cleared, as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// validateHealQueryParams - Validates query params for heal list management API.
func validateHealQueryParams(vars url.Values) (string, string, string, string, int, APIErrorCode) {
	bucket := vars.Get(string(mgmtBucket))
	prefix := vars.Get(string(mgmtPrefix))
	marker := vars.Get(string(mgmtMarker))
	delimiter := vars.Get(string(mgmtDelimiter))
	maxKeyStr := vars.Get(string(mgmtMaxKey))

	// N B empty bucket name is invalid
	if !IsValidBucketName(bucket) {
		return "", "", "", "", 0, ErrInvalidBucketName
	}

	// empty prefix is valid.
	if !IsValidObjectPrefix(prefix) {
		return "", "", "", "", 0, ErrInvalidObjectName
	}

	// check if maxKey is a valid integer.
	maxKey, err := strconv.Atoi(maxKeyStr)
	if err != nil {
		return "", "", "", "", 0, ErrInvalidMaxKeys
	}

	// Validate prefix, marker, delimiter and maxKey.
	apiErr := validateListObjectsArgs(prefix, marker, delimiter, maxKey)
	if apiErr != ErrNone {
		return "", "", "", "", 0, apiErr
	}

	return bucket, prefix, marker, delimiter, maxKey, ErrNone
}

// ListObjectsHealHandler - GET /?heal&bucket=mybucket&prefix=myprefix&marker=mymarker&delimiter=&mydelimiter&maxKey=1000
// - bucket is mandatory query parameter
// - rest are optional query parameters
// List upto maxKey objects that need healing in a given bucket matching the given prefix.
func (adminAPI adminAPIHandlers) ListObjectsHealHandler(w http.ResponseWriter, r *http.Request) {
	// Get object layer instance.
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Validate query params.
	vars := r.URL.Query()
	bucket, prefix, marker, delimiter, maxKey, adminAPIErr := validateHealQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Get the list objects to be healed.
	objectInfos, err := objLayer.ListObjectsHeal(bucket, prefix, marker, delimiter, maxKey)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	listResponse := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKey, objectInfos)
	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(listResponse))
}

// ListBucketsHealHandler - GET /?heal
func (adminAPI adminAPIHandlers) ListBucketsHealHandler(w http.ResponseWriter, r *http.Request) {
	// Get object layer instance.
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Get the list buckets to be healed.
	bucketsInfo, err := objLayer.ListBucketsHeal()
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	listResponse := generateListBucketsResponse(bucketsInfo)
	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(listResponse))
}

// HealBucketHandler - POST /?heal&bucket=mybucket&dry-run
// - x-minio-operation = bucket
// - bucket is mandatory query parameter
// Heal a given bucket, if present.
func (adminAPI adminAPIHandlers) HealBucketHandler(w http.ResponseWriter, r *http.Request) {
	// Get object layer instance.
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Validate bucket name and check if it exists.
	vars := r.URL.Query()
	bucket := vars.Get(string(mgmtBucket))
	if err := checkBucketExist(bucket, objLayer); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// if dry-run is present in query-params, then only perform validations and return success.
	if isDryRun(vars) {
		writeSuccessResponseHeadersOnly(w)
		return
	}

	// Heal the given bucket.
	err := objLayer.HealBucket(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Return 200 on success.
	writeSuccessResponseHeadersOnly(w)
}

// isDryRun - returns true if dry-run query param was set and false otherwise.
// otherwise.
func isDryRun(qval url.Values) bool {
	if _, dryRun := qval[string(mgmtDryRun)]; dryRun {
		return true
	}
	return false
}

// HealObjectHandler - POST /?heal&bucket=mybucket&object=myobject&dry-run
// - x-minio-operation = object
// - bucket and object are both mandatory query parameters
// Heal a given object, if present.
func (adminAPI adminAPIHandlers) HealObjectHandler(w http.ResponseWriter, r *http.Request) {
	// Get object layer instance.
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	vars := r.URL.Query()
	bucket := vars.Get(string(mgmtBucket))
	object := vars.Get(string(mgmtObject))

	// Validate bucket and object names.
	if err := checkBucketAndObjectNames(bucket, object); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Check if object exists.
	if _, err := objLayer.GetObjectInfo(bucket, object); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// if dry-run is set in query params then perform validations
	// and return success.
	if isDryRun(vars) {
		writeSuccessResponseHeadersOnly(w)
		return
	}

	err := objLayer.HealObject(bucket, object)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Return 200 on success.
	writeSuccessResponseHeadersOnly(w)
}

// HealFormatHandler - POST /?heal&dry-run
// - x-minio-operation = format
// - bucket and object are both mandatory query parameters
// Heal a given object, if present.
func (adminAPI adminAPIHandlers) HealFormatHandler(w http.ResponseWriter, r *http.Request) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkRequestAuthType(r, "", "", "")
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	// Check if this setup is an erasure code backend, since
	// heal-format is only applicable to single node XL and
	// distributed XL setup.
	if !globalIsXL {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	// if dry-run is set in query-params, return success as
	// validations are successful so far.
	vars := r.URL.Query()
	if isDryRun(vars) {
		writeSuccessResponseHeadersOnly(w)
		return
	}

	// Create a new set of storage instances to heal format.json.
	bootstrapDisks, err := initStorageDisks(globalEndpoints)
	if err != nil {
		fmt.Println(traceError(err))
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Heal format.json on available storage.
	err = healFormatXL(bootstrapDisks)
	if err != nil {
		fmt.Println(traceError(err))
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Instantiate new object layer with newly formatted storage.
	newObjectAPI, err := newXLObjects(bootstrapDisks)
	if err != nil {
		fmt.Println(traceError(err))
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Set object layer with newly formatted storage to globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = newObjectAPI
	globalObjLayerMutex.Unlock()

	// Shutdown storage belonging to old object layer instance.
	objectAPI.Shutdown()

	// Inform peers to reinitialize storage with newly formatted storage.
	reInitPeerDisks(globalAdminPeers)

	// Return 200 on success.
	writeSuccessResponseHeadersOnly(w)
}
