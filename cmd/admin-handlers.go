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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/quick"
	"github.com/minio/minio/pkg/quorum"
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
		logger.LogIf(context.Background(), err)
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

	uptime, err := globalAdminClients.GetLatestUptime()
	if err != nil {
		writeErrorResponseJSON(w, toAPIErrorCode(err), r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	// Create API response
	serverStatus := madmin.ServiceStatus{
		Uptime: uptime,
		ServerVersion: madmin.ServerVersion{
			Version:  Version,
			CommitID: CommitID,
		},
	}

	data, err := json.Marshal(serverStatus)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	writeSuccessResponseJSON(w, data)
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
		logger.LogIf(context.Background(), err)
		writeErrorResponseJSON(w, ErrRequestBodyParse, r.URL)
		return
	}

	var signal serviceSignal
	switch sa.Action {
	case madmin.ServiceActionValueRestart:
		signal = serviceRestart
	case madmin.ServiceActionValueStop:
		signal = serviceStop
	default:
		writeErrorResponseJSON(w, ErrMalformedPOSTRequest, r.URL)
		logger.LogIf(context.Background(), fmt.Errorf("invalid service action %v received", sa.Action))
		return
	}

	// Reply to the client before stopping/restarting minio server.
	writeSuccessResponseHeadersOnly(w)

	globalAdminClients.SendSignal(signal)
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

	reply := globalAdminClients.GetServerInfo()
	data, err := json.Marshal(reply)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	writeSuccessResponseJSON(w, data)
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
		logger.LogIf(context.Background(), err)
		return "", "", time.Duration(0), ErrInvalidDuration
	}

	return bucket, prefix, duration, ErrNone
}

func (a adminAPIHandlers) listClearLocks(w http.ResponseWriter, r *http.Request, clearLocks bool) {
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

	volLocks, err := globalAdminClients.ListLocks(bucket, prefix, duration)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	data, err := json.Marshal(volLocks)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	if clearLocks {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			writeErrorResponseJSON(w, ErrServerNotInitialized, r.URL)
			return
		}

		objLayer.ClearLocks(newContext(r, "ClearLocks"), volLocks)
	}

	writeSuccessResponseJSON(w, data)
}

// ListLocksHandler - GET /minio/admin/v1/locks?bucket=mybucket&prefix=myprefix&older-than=10s
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// ---------
// Lists locks held on a given bucket, prefix and duration it was held for.
func (a adminAPIHandlers) ListLocksHandler(w http.ResponseWriter, r *http.Request) {
	a.listClearLocks(w, r, false)
}

// ClearLocksHandler - DELETE /minio/admin/v1/locks?bucket=mybucket&prefix=myprefix&duration=duration
// - bucket is a mandatory query parameter
// - prefix and older-than are optional query parameters
// ---------
// Clear locks held on a given bucket, prefix and duration it was held for.
func (a adminAPIHandlers) ClearLocksHandler(w http.ResponseWriter, r *http.Request) {
	a.listClearLocks(w, r, true)
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
			logger.LogIf(context.Background(), jerr)
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
	ctx := newContext(r, "Heal")

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

	type healResp struct {
		respBytes []byte
		errCode   APIErrorCode
		errBody   string
	}

	// Define a closure to start sending whitespace to client
	// after 10s unless a response item comes in
	keepConnLive := func(w http.ResponseWriter, respCh chan healResp) {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
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
	}

	// find number of disks in the setup
	info := objLayer.StorageInfo(ctx)
	numDisks := info.Backend.OfflineDisks + info.Backend.OnlineDisks

	if clientToken == "" {
		// Not a status request
		nh := newHealSequence(bucket, objPrefix, handlers.GetSourceIP(r),
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

	// Take a read lock on minio/config.json. NB minio is a
	// reserved bucket name and wouldn't conflict with normal
	// object operations.
	configLock := globalNSMutex.NewNSLock(minioReservedBucket, minioConfigFile)
	if configLock.GetRLock(globalObjectTimeout) != nil {
		writeErrorResponseJSON(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer configLock.RUnlock()

	config, err := globalAdminClients.GetConfig()
	if err != nil {
		logger.LogIf(context.Background(), err)
		writeErrorResponseJSON(w, toAdminAPIErrCode(err), r.URL)
		return
	}

	data, err := json.Marshal(config)
	if err != nil {
		writeErrorResponseJSON(w, ErrInternalError, r.URL)
		logger.LogIf(context.Background(), err)
		return
	}

	writeSuccessResponseJSON(w, data)
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
func writeSetConfigResponse(w http.ResponseWriter, errs []*quorum.Error, status bool, reqURL *url.URL) {
	var nodeResults []nodeSummary
	for i := range errs {
		nodeResults = append(nodeResults, nodeSummary{
			Name:   errs[i].ID,
			ErrSet: errs[i].Err != nil,
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
	ctx := context.Background()

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
		logger.LogIf(ctx, err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	configBytes := configBuf[:n]

	// Validate JSON provided in the request body: check the
	// client has not sent JSON objects with duplicate keys.
	if err = quick.CheckDuplicateKeys(string(configBytes)); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrAdminConfigBadJSON, r.URL)
		return
	}

	var config serverConfig
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		logger.LogIf(ctx, err)
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

	errs, err := globalAdminClients.SetConfig(&config)
	if errs == nil && err != nil {
		writeErrorResponseJSON(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Reply to the client before stopping/restarting minio server.
	writeSetConfigResponse(w, errs, (err == nil), r.URL)

	// Restart all node for the modified config to take effect.
	globalAdminClients.SendSignal(serviceRestart)
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
		logger.LogIf(context.Background(), err)
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

	// Update local credentials in memory.
	globalServerConfig.SetCredential(creds)
	if err = globalServerConfig.Save(); err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// Update credentials in other peers.
	if err := globalNotificationSys.SetCredentials(creds); err != nil {
		logger.LogIf(context.Background(), err)
	}

	// At this stage, the operation is successful, return 200 OK
	w.WriteHeader(http.StatusOK)
}
