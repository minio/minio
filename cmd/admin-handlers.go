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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
)

const (
	minioConfigTmpFormat = "config-%s.json"

	maxConfigJSONSize = 256 * 1024 // 256KiB
)

// Type-safe query params.
type mgmtQueryKey string

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket        mgmtQueryKey = "bucket"
	mgmtPrefix        mgmtQueryKey = "prefix"
	mgmtLockOlderThan mgmtQueryKey = "older-than"
	mgmtClientToken   mgmtQueryKey = "clientToken"
	mgmtForceStart    mgmtQueryKey = "forceStart"
)

var (
	// This struct literal represents the Admin API version that
	// the server uses.
	adminAPIVersionInfo = madmin.AdminAPIVersionInfo{
		Version: "1",
	}
)

// VersionHandler - GET /minio/admin/version
// -----------
// Returns Administration API version
func (a adminAPIHandlers) VersionHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponse(w, adminAPIErr, r.URL)
		return
	}

	jsonBytes, err := json.Marshal(adminAPIVersionInfo)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal Admin API Version to JSON.")
		return
	}

	writeSuccessResponseJSON(w, jsonBytes)
}

// ServiceStatusHandler - GET /minio/admin/v1/service
// ----------
// Returns server version and uptime.
func (a adminAPIHandlers) ServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Fetch server version
	serverVersion := madmin.ServerVersion{
		Version:  Version,
		CommitID: CommitID,
	}

	// Fetch uptimes from all peers. This may fail to due to lack
	// of read-quorum availability.
	uptime, err := getPeerUptimes(globalAdminPeers)
	if err != nil {
		writeErrorResponseJSON(w, toAPIErrorCode(err), r.URL)
		errorIf(err, "Possibly failed to get uptime from majority of servers.")
		return
	}

	// Create API response
	serverStatus := madmin.ServiceStatus{
		ServerVersion: serverVersion,
		Uptime:        uptime,
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(serverStatus)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal storage info into json.")
		return
	}
	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// ServiceStopNRestartHandler - POST /minio/admin/v1/service
// Body: {"action": <restart-action>}
// ----------
// Restarts/Stops minio server gracefully. In a distributed setup,
// restarts all the servers in the cluster.
func (a adminAPIHandlers) ServiceStopNRestartHandler(w http.ResponseWriter, r *http.Request) {
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	var sa madmin.ServiceAction
	err := json.NewDecoder(r.Body).Decode(&sa)
	if err != nil {
		errorIf(err, "Error parsing body JSON")
		writeErrorResponseJSON(w, ErrRequestBodyParse, r.URL)
		return
	}

	var serviceSig serviceSignal
	switch sa.Action {
	case madmin.ServiceActionValueRestart:
		serviceSig = serviceRestart
	case madmin.ServiceActionValueStop:
		serviceSig = serviceStop
	default:
		writeErrorResponseJSON(w, ErrMalformedPOSTRequest, r.URL)
		errorIf(err, "Invalid service action received")
		return
	}

	// Reply to the client before restarting minio server.
	writeSuccessResponseHeadersOnly(w)

	sendServiceCmd(globalAdminPeers, serviceSig)
}

// ServerProperties holds some server information such as, version, region
// uptime, etc..
type ServerProperties struct {
	Uptime   time.Duration `json:"uptime"`
	Version  string        `json:"version"`
	CommitID string        `json:"commitID"`
	Region   string        `json:"region"`
	SQSARN   []string      `json:"sqsARN"`
}

// ServerConnStats holds transferred bytes from/to the server
type ServerConnStats struct {
	TotalInputBytes  uint64 `json:"transferred"`
	TotalOutputBytes uint64 `json:"received"`
	Throughput       uint64 `json:"throughput,omitempty"`
}

// ServerHTTPMethodStats holds total number of HTTP operations from/to the server,
// including the average duration the call was spent.
type ServerHTTPMethodStats struct {
	Count       uint64 `json:"count"`
	AvgDuration string `json:"avgDuration"`
}

// ServerHTTPStats holds all type of http operations performed to/from the server
// including their average execution time.
type ServerHTTPStats struct {
	TotalHEADStats     ServerHTTPMethodStats `json:"totalHEADs"`
	SuccessHEADStats   ServerHTTPMethodStats `json:"successHEADs"`
	TotalGETStats      ServerHTTPMethodStats `json:"totalGETs"`
	SuccessGETStats    ServerHTTPMethodStats `json:"successGETs"`
	TotalPUTStats      ServerHTTPMethodStats `json:"totalPUTs"`
	SuccessPUTStats    ServerHTTPMethodStats `json:"successPUTs"`
	TotalPOSTStats     ServerHTTPMethodStats `json:"totalPOSTs"`
	SuccessPOSTStats   ServerHTTPMethodStats `json:"successPOSTs"`
	TotalDELETEStats   ServerHTTPMethodStats `json:"totalDELETEs"`
	SuccessDELETEStats ServerHTTPMethodStats `json:"successDELETEs"`
}

// ServerInfoData holds storage, connections and other
// information of a given server.
type ServerInfoData struct {
	StorageInfo StorageInfo      `json:"storage"`
	ConnStats   ServerConnStats  `json:"network"`
	HTTPStats   ServerHTTPStats  `json:"http"`
	Properties  ServerProperties `json:"server"`
}

// ServerInfo holds server information result of one node
type ServerInfo struct {
	Error string          `json:"error"`
	Addr  string          `json:"addr"`
	Data  *ServerInfoData `json:"data"`
}

// ServerInfoHandler - GET /minio/admin/v1/info
// ----------
// Get server information
func (a adminAPIHandlers) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	// Authenticate request
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Web service response
	reply := make([]ServerInfo, len(globalAdminPeers))

	var wg sync.WaitGroup

	// Gather server information for all nodes
	for i, p := range globalAdminPeers {
		wg.Add(1)

		// Gather information from a peer in a goroutine
		go func(idx int, peer adminPeer) {
			defer wg.Done()

			// Initialize server info at index
			reply[idx] = ServerInfo{Addr: peer.addr}

			serverInfoData, err := peer.cmdRunner.ServerInfoData()
			if err != nil {
				errorIf(err, "Unable to get server info from %s.", peer.addr)
				reply[idx].Error = err.Error()
				return
			}

			reply[idx].Data = &serverInfoData
		}(i, p)
	}

	wg.Wait()

	// Marshal API response
	jsonBytes, err := json.Marshal(reply)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal storage info into json.")
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// validateLockQueryParams - Validates query params for list/clear
// locks management APIs.
func validateLockQueryParams(vars url.Values) (string, string, time.Duration,
	APIErrorCode) {

	bucket := vars.Get(string(mgmtBucket))
	prefix := vars.Get(string(mgmtPrefix))
	olderThanStr := vars.Get(string(mgmtLockOlderThan))

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
	if olderThanStr == "" {
		olderThanStr = "0s"
	}
	duration, err := time.ParseDuration(olderThanStr)
	if err != nil {
		errorIf(err, "Failed to parse duration passed as query value.")
		return "", "", time.Duration(0), ErrInvalidDuration
	}

	return bucket, prefix, duration, ErrNone
}

// ListLocksHandler - GET /minio/admin/v1/locks?bucket=mybucket&prefix=myprefix&older-than=10s
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// ---------
// Lists locks held on a given bucket, prefix and duration it was held for.
func (a adminAPIHandlers) ListLocksHandler(w http.ResponseWriter, r *http.Request) {

	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, duration, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are available for longer than duration.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix,
		duration)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}

	// Reply with list of locks held on bucket, matching prefix
	// held longer than duration supplied, as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// ClearLocksHandler - DELETE /minio/admin/v1/locks?bucket=mybucket&prefix=myprefix&duration=duration
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// ---------
// Clear locks held on a given bucket, prefix and duration it was held for.
func (a adminAPIHandlers) ClearLocksHandler(w http.ResponseWriter, r *http.Request) {

	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	vars := r.URL.Query()
	bucket, prefix, duration, adminAPIErr := validateLockQueryParams(vars)
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Fetch lock information of locks matching bucket/prefix that
	// are held for longer than duration.
	volLocks, err := listPeerLocksInfo(globalAdminPeers, bucket, prefix,
		duration)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to fetch lock information from remote nodes.")
		return
	}

	// Marshal list of locks as json.
	jsonBytes, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		errorIf(err, "Failed to marshal lock information into json.")
		return
	}
	newObjectLayerFn().ClearLocks(volLocks)

	// Reply with list of locks cleared, as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// extractHealInitParams - Validates params for heal init API.
func extractHealInitParams(r *http.Request) (bucket, objPrefix string,
	hs madmin.HealOpts, clientToken string, forceStart bool,
	err APIErrorCode) {

	vars := mux.Vars(r)
	bucket = vars[string(mgmtBucket)]
	objPrefix = vars[string(mgmtPrefix)]

	if bucket == "" {
		if objPrefix != "" {
			// Bucket is required if object-prefix is given
			err = ErrHealMissingBucket
			return
		}
	} else if !IsValidBucketName(bucket) {
		err = ErrInvalidBucketName
		return
	}

	// empty prefix is valid.
	if !IsValidObjectPrefix(objPrefix) {
		err = ErrInvalidObjectName
		return
	}

	qParms := r.URL.Query()
	if len(qParms[string(mgmtClientToken)]) > 0 {
		clientToken = qParms[string(mgmtClientToken)][0]
	}
	if _, ok := qParms[string(mgmtForceStart)]; ok {
		forceStart = true
	}

	// ignore body if clientToken is provided
	if clientToken == "" {
		jerr := json.NewDecoder(r.Body).Decode(&hs)
		if jerr != nil {
			errorIf(jerr, "Error parsing body JSON")
			err = ErrRequestBodyParse
			return
		}
	}

	err = ErrNone
	return
}

// HealHandler - POST /minio/admin/v1/heal/
// -----------
// Start heal processing and return heal status items.
//
// On a successful heal sequence start, a unique client token is
// returned. Subsequent requests to this endpoint providing the client
// token will receive heal status records from the running heal
// sequence.
//
// If no client token is provided, and a heal sequence is in progress
// an error is returned with information about the running heal
// sequence. However, if the force-start flag is provided, the server
// aborts the running heal sequence and starts a new one.
func (a adminAPIHandlers) HealHandler(w http.ResponseWriter, r *http.Request) {
	// Get object layer instance.
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		writeErrorResponseJSON(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Check if this setup has an erasure coded backend.
	if !globalIsXL {
		writeErrorResponseJSON(w, ErrHealNotImplemented, r.URL)
		return
	}

	bucket, objPrefix, hs, clientToken, forceStart, apiErr := extractHealInitParams(r)
	if apiErr != ErrNone {
		writeErrorResponseJSON(w, apiErr, r.URL)
		return
	}

	// Helper function to fetch client address - we use the
	// X-forwarded-for header if one is present.
	getClientAddress := func() string {
		addr := r.RemoteAddr
		fwdFor := r.Header.Get("X-Forwarded-For")
		if fwdFor == "" {
			return addr
		}
		return fwdFor
	}

	type healResp struct {
		respBytes []byte
		errCode   APIErrorCode
		errBody   string
	}

	// Define a closure to start sending whitespace to client
	// after 10s unless a response item comes in
	keepConnLive := func(w http.ResponseWriter, respCh chan healResp) {
		ticker := time.NewTicker(time.Second * 10)
		started := false
	forLoop:
		for {
			select {
			case <-ticker.C:
				if !started {
					// Start writing response to client
					started = true
					setCommonHeaders(w)
					w.Header().Set("Content-Type", string(mimeJSON))
					// Set 200 OK status
					w.WriteHeader(200)
				}
				// Send whitespace and keep connection open
				w.Write([]byte("\n\r"))
				w.(http.Flusher).Flush()
			case hr := <-respCh:
				switch {
				case hr.errCode == ErrNone:
					writeSuccessResponseJSON(w, hr.respBytes)
				case hr.errBody == "":
					writeErrorResponseJSON(w, hr.errCode, r.URL)
				default:
					writeCustomErrorResponseJSON(w, hr.errCode, hr.errBody, r.URL)
				}
				break forLoop
			}
		}
		ticker.Stop()
	}

	// find number of disks in the setup
	info := objLayer.StorageInfo()
	numDisks := info.Backend.OfflineDisks + info.Backend.OnlineDisks

	if clientToken == "" {
		// Not a status request
		nh := newHealSequence(bucket, objPrefix, getClientAddress(),
			numDisks, hs, forceStart)

		respCh := make(chan healResp)
		go func() {
			respBytes, errCode, errMsg := globalAllHealState.LaunchNewHealSequence(nh)
			hr := healResp{respBytes, errCode, errMsg}
			respCh <- hr
		}()

		// Due to the force-starting functionality, the Launch
		// call above can take a long time - to keep the
		// connection alive, we start sending whitespace
		keepConnLive(w, respCh)
	} else {
		// Since clientToken is given, fetch heal status from running
		// heal sequence.
		path := bucket + "/" + objPrefix
		respBytes, errCode := globalAllHealState.PopHealStatusJSON(
			path, clientToken)
		if errCode != ErrNone {
			writeErrorResponseJSON(w, errCode, r.URL)
		} else {
			writeSuccessResponseJSON(w, respBytes)
		}
	}
}

// GetConfigHandler - GET /minio/admin/v1/config
// Get config.json of this minio setup.
func (a adminAPIHandlers) GetConfigHandler(w http.ResponseWriter, r *http.Request) {

	// Validate request signature.
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// check if objectLayer is initialized, if not return.
	if newObjectLayerFn() == nil {
		writeErrorResponseJSON(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Take a read lock on minio/config.json. NB minio is a
	// reserved bucket name and wouldn't conflict with normal
	// object operations.
	configLock := globalNSMutex.NewNSLock(minioReservedBucket, minioConfigFile)
	if configLock.GetRLock(globalObjectTimeout) != nil {
		writeErrorResponseJSON(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer configLock.RUnlock()

	// Get config.json - in distributed mode, the configuration
	// occurring on a quorum of the servers is returned.
	configBytes, err := getPeerConfig(globalAdminPeers)
	if err != nil {
		errorIf(err, "Failed to get config from peers")
		writeErrorResponseJSON(w, toAdminAPIErrCode(err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, configBytes)
}

// toAdminAPIErrCode - converts errXLWriteQuorum error to admin API
// specific error.
func toAdminAPIErrCode(err error) APIErrorCode {
	switch err {
	case errXLWriteQuorum:
		return ErrAdminConfigNoQuorum
	}
	return toAPIErrorCode(err)
}

// SetConfigResult - represents detailed results of a set-config
// operation.
type nodeSummary struct {
	Name   string `json:"name"`
	ErrSet bool   `json:"errSet"`
	ErrMsg string `json:"errMsg"`
}

type setConfigResult struct {
	NodeResults []nodeSummary `json:"nodeResults"`
	Status      bool          `json:"status"`
}

// writeSetConfigResponse - writes setConfigResult value as json
// depending on the status.
func writeSetConfigResponse(w http.ResponseWriter, peers adminPeers,
	errs []error, status bool, reqURL *url.URL) {

	var nodeResults []nodeSummary
	// Build nodeResults based on error values received during
	// set-config operation.
	for i := range errs {
		nodeResults = append(nodeResults, nodeSummary{
			Name:   peers[i].addr,
			ErrSet: errs[i] != nil,
			ErrMsg: fmt.Sprintf("%v", errs[i]),
		})

	}

	result := setConfigResult{
		Status:      status,
		NodeResults: nodeResults,
	}

	// The following elaborate json encoding is to avoid escaping
	// '<', '>' in <nil>. Note: json.Encoder.Encode() adds a
	// gratuitous "\n".
	var resultBuf bytes.Buffer
	enc := json.NewEncoder(&resultBuf)
	enc.SetEscapeHTML(false)
	jsonErr := enc.Encode(result)
	if jsonErr != nil {
		writeErrorResponseJSON(w, toAPIErrorCode(jsonErr), reqURL)
		return
	}

	writeSuccessResponseJSON(w, resultBuf.Bytes())
	return
}

// SetConfigHandler - PUT /minio/admin/v1/config
func (a adminAPIHandlers) SetConfigHandler(w http.ResponseWriter, r *http.Request) {

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Validate request signature.
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(w, adminAPIErr, r.URL)
		return
	}

	// Read configuration bytes from request body.
	configBuf := make([]byte, maxConfigJSONSize+1)
	n, err := io.ReadFull(r.Body, configBuf)
	if err == nil {
		// More than maxConfigSize bytes were available
		writeErrorResponse(w, ErrAdminConfigTooLarge, r.URL)
		return
	}
	if err != io.ErrUnexpectedEOF {
		errorIf(err, "Failed to read config from request body.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	configBytes := configBuf[:n]

	// Validate JSON provided in the request body: check the
	// client has not sent JSON objects with duplicate keys.
	if err = checkDupJSONKeys(string(configBytes)); err != nil {
		errorIf(err, "config contains duplicate JSON entries.")
		writeErrorResponse(w, ErrAdminConfigBadJSON, r.URL)
		return
	}

	var config serverConfig
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		errorIf(err, "Failed to unmarshal JSON configuration", err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// If credentials for the server are provided via environment,
	// then credentials in the provided configuration must match.
	if globalIsEnvCreds {
		creds := globalServerConfig.GetCredential()
		if config.Credential.AccessKey != creds.AccessKey ||
			config.Credential.SecretKey != creds.SecretKey {
			writeErrorResponseJSON(w, ErrAdminCredentialsMismatch, r.URL)
			return
		}
	}

	// Write config received from request onto a temporary file on
	// all nodes.
	tmpFileName := fmt.Sprintf(minioConfigTmpFormat, mustGetUUID())
	errs := writeTmpConfigPeers(globalAdminPeers, tmpFileName, configBytes)

	// Check if the operation succeeded in quorum or more nodes.
	rErr := reduceWriteQuorumErrs(errs, nil, len(globalAdminPeers)/2+1)
	if rErr != nil {
		writeSetConfigResponse(w, globalAdminPeers, errs, false, r.URL)
		return
	}

	// Take a lock on minio/config.json. NB minio is a reserved
	// bucket name and wouldn't conflict with normal object
	// operations.
	configLock := globalNSMutex.NewNSLock(minioReservedBucket, minioConfigFile)
	if configLock.GetLock(globalObjectTimeout) != nil {
		writeErrorResponseJSON(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer configLock.Unlock()

	// Rename the temporary config file to config.json
	errs = commitConfigPeers(globalAdminPeers, tmpFileName)
	rErr = reduceWriteQuorumErrs(errs, nil, len(globalAdminPeers)/2+1)
	if rErr != nil {
		writeSetConfigResponse(w, globalAdminPeers, errs, false, r.URL)
		return
	}

	// serverMux (cmd/server-mux.go) implements graceful shutdown,
	// where all listeners are closed and process restart/shutdown
	// happens after 5s or completion of all ongoing http
	// requests, whichever is earlier.
	writeSetConfigResponse(w, globalAdminPeers, errs, true, r.URL)

	// Restart all node for the modified config to take effect.
	sendServiceCmd(globalAdminPeers, serviceRestart)
}

// ConfigCredsHandler - POST /minio/admin/v1/config/credential
// ----------
// Update credentials in a minio server. In a distributed setup,
// update all the servers in the cluster.
func (a adminAPIHandlers) UpdateCredentialsHandler(w http.ResponseWriter,
	r *http.Request) {

	// Authenticate request
	adminAPIErr := checkAdminRequestAuthType(r, globalServerConfig.GetRegion())
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

	// Decode request body
	var req madmin.SetCredsReq
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		errorIf(err, "Error parsing body JSON")
		writeErrorResponseJSON(w, ErrRequestBodyParse, r.URL)
		return
	}

	creds, err := auth.CreateCredentials(req.AccessKey, req.SecretKey)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Take a lock on minio/config.json. Prevents concurrent
	// config file updates.
	configLock := globalNSMutex.NewNSLock(minioReservedBucket, minioConfigFile)
	if configLock.GetLock(globalObjectTimeout) != nil {
		writeErrorResponseJSON(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer configLock.Unlock()

	// Acquire lock before updating global configuration.
	globalServerConfigMu.Lock()
	defer globalServerConfigMu.Unlock()

	// Notify all other Minio peers to update credentials
	updateErrs := updateCredsOnPeers(creds)
	for peer, err := range updateErrs {
		errorIf(err, "Unable to update credentials on peer %s.", peer)
	}

	// Update local credentials in memory.
	globalServerConfig.SetCredential(creds)
	if err = globalServerConfig.Save(); err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// At this stage, the operation is successful, return 200 OK
	w.WriteHeader(http.StatusOK)
}
