/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/notify"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/message/log"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/handlers"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	trace "github.com/minio/minio/pkg/trace"
)

const (
	maxEConfigJSONSize = 262272
)

// Type-safe query params.
type mgmtQueryKey string

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket      mgmtQueryKey = "bucket"
	mgmtPrefix                   = "prefix"
	mgmtClientToken              = "clientToken"
	mgmtForceStart               = "forceStart"
	mgmtForceStop                = "forceStop"
)

func updateServer(updateURL, sha256Hex string, latestReleaseTime time.Time) (us madmin.ServerUpdateStatus, err error) {
	minioMode := getMinioMode()
	// No inputs provided we should try to update using the default URL.
	if updateURL == "" && sha256Hex == "" && latestReleaseTime.IsZero() {
		var updateMsg string
		updateMsg, sha256Hex, _, latestReleaseTime, err = getUpdateInfo(updateTimeout, minioMode)
		if err != nil {
			return us, err
		}
		if updateMsg == "" {
			us.CurrentVersion = Version
			us.UpdatedVersion = Version
			return us, nil
		}
		if runtime.GOOS == "windows" {
			updateURL = minioReleaseURL + "minio.exe"
		} else {
			updateURL = minioReleaseURL + "minio"
		}
	}
	if err = doUpdate(updateURL, sha256Hex, minioMode); err != nil {
		return us, err
	}
	us.CurrentVersion = Version
	us.UpdatedVersion = latestReleaseTime.Format(minioReleaseTagTimeLayout)
	return us, nil
}

// ServerUpdateHandler - POST /minio/admin/v3/update?updateURL={updateURL}
// ----------
// updates all minio servers and restarts them gracefully.
func (a adminAPIHandlers) ServerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ServerUpdate")

	defer logger.AuditLog(w, r, "ServerUpdate", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if globalInplaceUpdateDisabled {
		// if MINIO_UPDATE=off - inplace update is disabled, mostly
		// in containers.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	updateURL := vars[peerRESTUpdateURL]
	mode := getMinioMode()
	var sha256Hex string
	var latestReleaseTime time.Time
	if updateURL != "" {
		u, err := url.Parse(updateURL)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		content, err := downloadReleaseURL(updateURL, updateTimeout, mode)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		sha256Hex, latestReleaseTime, err = parseReleaseData(content)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		if runtime.GOOS == "windows" {
			u.Path = path.Dir(u.Path) + SlashSeparator + "minio.exe"
		} else {
			u.Path = path.Dir(u.Path) + SlashSeparator + "minio"
		}

		updateURL = u.String()
	}

	for _, nerr := range globalNotificationSys.ServerUpdate(updateURL, sha256Hex, latestReleaseTime) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	updateStatus, err := updateServer(updateURL, sha256Hex, latestReleaseTime)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(updateStatus)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, jsonBytes)

	if updateStatus.CurrentVersion != updateStatus.UpdatedVersion {
		// We did upgrade - restart all services.
		globalServiceSignalCh <- serviceRestart
	}
}

// ServiceHandler - POST /minio/admin/v3/service?action={action}
// ----------
// restarts/stops minio server gracefully. In a distributed setup,
func (a adminAPIHandlers) ServiceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "Service")

	defer logger.AuditLog(w, r, "Service", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	action := vars["action"]

	var serviceSig serviceSignal
	switch madmin.ServiceAction(action) {
	case madmin.ServiceActionRestart:
		serviceSig = serviceRestart
	case madmin.ServiceActionStop:
		serviceSig = serviceStop
	default:
		logger.LogIf(ctx, fmt.Errorf("Unrecognized service action %s requested", action), logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	var objectAPI ObjectLayer
	if serviceSig == serviceRestart {
		objectAPI, _ = validateAdminReq(ctx, w, r, iampolicy.ServiceRestartAdminAction)
	} else {
		objectAPI, _ = validateAdminReq(ctx, w, r, iampolicy.ServiceStopAdminAction)
	}
	if objectAPI == nil {
		return
	}

	// Notify all other MinIO peers signal service.
	for _, nerr := range globalNotificationSys.SignalService(serviceSig) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	// Reply to the client before restarting, stopping MinIO server.
	writeSuccessResponseHeadersOnly(w)

	globalServiceSignalCh <- serviceSig
}

// ServerProperties holds some server information such as, version, region
// uptime, etc..
type ServerProperties struct {
	Uptime       int64    `json:"uptime"`
	Version      string   `json:"version"`
	CommitID     string   `json:"commitID"`
	DeploymentID string   `json:"deploymentID"`
	Region       string   `json:"region"`
	SQSARN       []string `json:"sqsARN"`
}

// ServerConnStats holds transferred bytes from/to the server
type ServerConnStats struct {
	TotalInputBytes  uint64 `json:"transferred"`
	TotalOutputBytes uint64 `json:"received"`
	Throughput       uint64 `json:"throughput,omitempty"`
	S3InputBytes     uint64 `json:"transferredS3"`
	S3OutputBytes    uint64 `json:"receivedS3"`
}

// ServerHTTPAPIStats holds total number of HTTP operations from/to the server,
// including the average duration the call was spent.
type ServerHTTPAPIStats struct {
	APIStats map[string]int `json:"apiStats"`
}

// ServerHTTPStats holds all type of http operations performed to/from the server
// including their average execution time.
type ServerHTTPStats struct {
	CurrentS3Requests ServerHTTPAPIStats `json:"currentS3Requests"`
	TotalS3Requests   ServerHTTPAPIStats `json:"totalS3Requests"`
	TotalS3Errors     ServerHTTPAPIStats `json:"totalS3Errors"`
}

// ServerInfoData holds storage, connections and other
// information of a given server.
type ServerInfoData struct {
	ConnStats  ServerConnStats  `json:"network"`
	HTTPStats  ServerHTTPStats  `json:"http"`
	Properties ServerProperties `json:"server"`
}

// ServerInfo holds server information result of one node
type ServerInfo struct {
	Error string          `json:"error"`
	Addr  string          `json:"addr"`
	Data  *ServerInfoData `json:"data"`
}

// StorageInfoHandler - GET /minio/admin/v3/storageinfo
// ----------
// Get server information
func (a adminAPIHandlers) StorageInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StorageInfo")

	defer logger.AuditLog(w, r, "StorageInfo", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.StorageInfoAdminAction)
	if objectAPI == nil {
		return
	}

	// ignores any errors here.
	storageInfo, _ := objectAPI.StorageInfo(ctx, false)

	// Marshal API response
	jsonBytes, err := json.Marshal(storageInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)

}

// DataUsageInfoHandler - GET /minio/admin/v3/datausage
// ----------
// Get server/cluster data usage info
func (a adminAPIHandlers) DataUsageInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DataUsageInfo")

	defer logger.AuditLog(w, r, "DataUsageInfo", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DataUsageInfoAdminAction)
	if objectAPI == nil {
		return
	}

	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	dataUsageInfoJSON, err := json.Marshal(dataUsageInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, dataUsageInfoJSON)
}

func lriToLockEntry(l lockRequesterInfo, resource, server string) *madmin.LockEntry {
	entry := &madmin.LockEntry{
		Timestamp:  l.Timestamp,
		Resource:   resource,
		ServerList: []string{server},
		Source:     l.Source,
		ID:         l.UID,
	}
	if l.Writer {
		entry.Type = "WRITE"
	} else {
		entry.Type = "READ"
	}
	return entry
}

func topLockEntries(peerLocks []*PeerLocks, count int) madmin.LockEntries {
	entryMap := make(map[string]*madmin.LockEntry)
	for _, peerLock := range peerLocks {
		if peerLock == nil {
			continue
		}
		for _, locks := range peerLock.Locks {
			for k, v := range locks {
				for _, lockReqInfo := range v {
					if val, ok := entryMap[lockReqInfo.UID]; ok {
						val.ServerList = append(val.ServerList, peerLock.Addr)
					} else {
						entryMap[lockReqInfo.UID] = lriToLockEntry(lockReqInfo, k, peerLock.Addr)
					}
				}
			}
		}
	}
	var lockEntries = make(madmin.LockEntries, 0, len(entryMap))
	for _, v := range entryMap {
		lockEntries = append(lockEntries, *v)
	}
	sort.Sort(lockEntries)
	if len(lockEntries) > count {
		lockEntries = lockEntries[:count]
	}
	return lockEntries
}

// PeerLocks holds server information result of one node
type PeerLocks struct {
	Addr  string
	Locks GetLocksResp
}

// TopLocksHandler Get list of locks in use
func (a adminAPIHandlers) TopLocksHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "TopLocks")

	defer logger.AuditLog(w, r, "TopLocks", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.TopLocksAdminAction)
	if objectAPI == nil {
		return
	}

	count := 10 // by default list only top 10 entries
	if countStr := r.URL.Query().Get("count"); countStr != "" {
		var err error
		count, err = strconv.Atoi(countStr)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	peerLocks := globalNotificationSys.GetLocks(ctx)
	// Once we have received all the locks currently used from peers
	// add the local peer locks list as well.
	var getRespLocks GetLocksResp
	for _, llocker := range globalLockServers {
		getRespLocks = append(getRespLocks, llocker.DupLockMap())
	}

	peerLocks = append(peerLocks, &PeerLocks{
		Addr:  getHostName(r),
		Locks: getRespLocks,
	})

	topLocks := topLockEntries(peerLocks, count)

	// Marshal API response
	jsonBytes, err := json.Marshal(topLocks)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// StartProfilingResult contains the status of the starting
// profiling action in a given server
type StartProfilingResult struct {
	NodeName string `json:"nodeName"`
	Success  bool   `json:"success"`
	Error    string `json:"error"`
}

// StartProfilingHandler - POST /minio/admin/v3/profiling/start?profilerType={profilerType}
// ----------
// Enable server profiling
func (a adminAPIHandlers) StartProfilingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StartProfiling")

	defer logger.AuditLog(w, r, "StartProfiling", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ProfilingAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	profiles := strings.Split(vars["profilerType"], ",")
	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	globalProfilerMu.Lock()
	defer globalProfilerMu.Unlock()

	if globalProfiler == nil {
		globalProfiler = make(map[string]minioProfiler, 10)
	}

	// Stop profiler of all types if already running
	for k, v := range globalProfiler {
		for _, p := range profiles {
			if p == k {
				v.Stop()
				delete(globalProfiler, k)
			}
		}
	}

	// Start profiling on remote servers.
	var hostErrs []NotificationPeerErr
	for _, profiler := range profiles {
		hostErrs = append(hostErrs, globalNotificationSys.StartProfiling(profiler)...)

		// Start profiling locally as well.
		prof, err := startProfiler(profiler)
		if err != nil {
			hostErrs = append(hostErrs, NotificationPeerErr{
				Host: *thisAddr,
				Err:  err,
			})
		} else {
			globalProfiler[profiler] = prof
			hostErrs = append(hostErrs, NotificationPeerErr{
				Host: *thisAddr,
			})
		}
	}

	var startProfilingResult []StartProfilingResult

	for _, nerr := range hostErrs {
		result := StartProfilingResult{NodeName: nerr.Host.String()}
		if nerr.Err != nil {
			result.Error = nerr.Err.Error()
		} else {
			result.Success = true
		}
		startProfilingResult = append(startProfilingResult, result)
	}

	// Create JSON result and send it to the client
	startProfilingResultInBytes, err := json.Marshal(startProfilingResult)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, startProfilingResultInBytes)
}

// dummyFileInfo represents a dummy representation of a profile data file
// present only in memory, it helps to generate the zip stream.
type dummyFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (f dummyFileInfo) Name() string       { return f.name }
func (f dummyFileInfo) Size() int64        { return f.size }
func (f dummyFileInfo) Mode() os.FileMode  { return f.mode }
func (f dummyFileInfo) ModTime() time.Time { return f.modTime }
func (f dummyFileInfo) IsDir() bool        { return f.isDir }
func (f dummyFileInfo) Sys() interface{}   { return f.sys }

// DownloadProfilingHandler - POST /minio/admin/v3/profiling/download
// ----------
// Download profiling information of all nodes in a zip format
func (a adminAPIHandlers) DownloadProfilingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DownloadProfiling")

	defer logger.AuditLog(w, r, "DownloadProfiling", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ProfilingAdminAction)
	if objectAPI == nil {
		return
	}

	if !globalNotificationSys.DownloadProfilingData(ctx, w) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminProfilerNotEnabled), r.URL)
		return
	}
}

type healInitParams struct {
	bucket, objPrefix     string
	hs                    madmin.HealOpts
	clientToken           string
	forceStart, forceStop bool
}

// extractHealInitParams - Validates params for heal init API.
func extractHealInitParams(vars map[string]string, qParms url.Values, r io.Reader) (hip healInitParams, err APIErrorCode) {
	hip.bucket = vars[string(mgmtBucket)]
	hip.objPrefix = vars[string(mgmtPrefix)]

	if hip.bucket == "" {
		if hip.objPrefix != "" {
			// Bucket is required if object-prefix is given
			err = ErrHealMissingBucket
			return
		}
	} else if isReservedOrInvalidBucket(hip.bucket, false) {
		err = ErrInvalidBucketName
		return
	}

	// empty prefix is valid.
	if !IsValidObjectPrefix(hip.objPrefix) {
		err = ErrInvalidObjectName
		return
	}

	if len(qParms[string(mgmtClientToken)]) > 0 {
		hip.clientToken = qParms[string(mgmtClientToken)][0]
	}
	if _, ok := qParms[string(mgmtForceStart)]; ok {
		hip.forceStart = true
	}
	if _, ok := qParms[string(mgmtForceStop)]; ok {
		hip.forceStop = true
	}

	// Invalid request conditions:
	//
	//   Cannot have both forceStart and forceStop in the same
	//   request; If clientToken is provided, request can only be
	//   to continue receiving logs, so it cannot be start or
	//   stop;
	if (hip.forceStart && hip.forceStop) ||
		(hip.clientToken != "" && (hip.forceStart || hip.forceStop)) {
		err = ErrInvalidRequest
		return
	}

	// ignore body if clientToken is provided
	if hip.clientToken == "" {
		jerr := json.NewDecoder(r).Decode(&hip.hs)
		if jerr != nil {
			logger.LogIf(GlobalContext, jerr, logger.Application)
			err = ErrRequestBodyParse
			return
		}
	}

	err = ErrNone
	return
}

// HealHandler - POST /minio/admin/v3/heal/
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
	ctx := newContext(r, w, "Heal")

	defer logger.AuditLog(w, r, "Heal", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	// Check if this setup has an erasure coded backend.
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrHealNotImplemented), r.URL)
		return
	}

	hip, errCode := extractHealInitParams(mux.Vars(r), r.URL.Query(), r.Body)
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	type healResp struct {
		respBytes []byte
		apiErr    APIError
		errBody   string
	}

	// Define a closure to start sending whitespace to client
	// after 10s unless a response item comes in
	keepConnLive := func(w http.ResponseWriter, r *http.Request, respCh chan healResp) {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		started := false
	forLoop:
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if !started {
					// Start writing response to client
					started = true
					setCommonHeaders(w)
					w.Header().Set(xhttp.ContentType, "text/event-stream")
					// Set 200 OK status
					w.WriteHeader(200)
				}
				// Send whitespace and keep connection open
				w.Write([]byte(" "))
				w.(http.Flusher).Flush()
			case hr := <-respCh:
				switch hr.apiErr {
				case noError:
					if started {
						w.Write(hr.respBytes)
						w.(http.Flusher).Flush()
					} else {
						writeSuccessResponseJSON(w, hr.respBytes)
					}
				default:
					var errorRespJSON []byte
					if hr.errBody == "" {
						errorRespJSON = encodeResponseJSON(getAPIErrorResponse(ctx, hr.apiErr,
							r.URL.Path, w.Header().Get(xhttp.AmzRequestID),
							globalDeploymentID))
					} else {
						errorRespJSON = encodeResponseJSON(APIErrorResponse{
							Code:      hr.apiErr.Code,
							Message:   hr.errBody,
							Resource:  r.URL.Path,
							RequestID: w.Header().Get(xhttp.AmzRequestID),
							HostID:    globalDeploymentID,
						})
					}
					if !started {
						setCommonHeaders(w)
						w.Header().Set(xhttp.ContentType, string(mimeJSON))
						w.WriteHeader(hr.apiErr.HTTPStatusCode)
					}
					w.Write(errorRespJSON)
					w.(http.Flusher).Flush()
				}
				break forLoop
			}
		}
	}

	// find number of disks in the setup, ignore any errors here.
	info, _ := objectAPI.StorageInfo(ctx, false)
	numDisks := info.Backend.OfflineDisks.Sum() + info.Backend.OnlineDisks.Sum()

	healPath := pathJoin(hip.bucket, hip.objPrefix)
	if hip.clientToken == "" && !hip.forceStart && !hip.forceStop {
		nh, exists := globalAllHealState.getHealSequence(healPath)
		if exists && !nh.hasEnded() && len(nh.currentStatus.Items) > 0 {
			b, err := json.Marshal(madmin.HealStartSuccess{
				ClientToken:   nh.clientToken,
				ClientAddress: nh.clientAddress,
				StartTime:     nh.startTime,
			})
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			// Client token not specified but a heal sequence exists on a path,
			// Send the token back to client.
			writeSuccessResponseJSON(w, b)
			return
		}
	}

	if hip.clientToken != "" && !hip.forceStart && !hip.forceStop {
		// Since clientToken is given, fetch heal status from running
		// heal sequence.
		respBytes, errCode := globalAllHealState.PopHealStatusJSON(
			healPath, hip.clientToken)
		if errCode != ErrNone {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		} else {
			writeSuccessResponseJSON(w, respBytes)
		}
		return
	}

	respCh := make(chan healResp)
	switch {
	case hip.forceStop:
		go func() {
			respBytes, apiErr := globalAllHealState.stopHealSequence(healPath)
			hr := healResp{respBytes: respBytes, apiErr: apiErr}
			respCh <- hr
		}()
	case hip.clientToken == "":
		nh := newHealSequence(GlobalContext, hip.bucket, hip.objPrefix, handlers.GetSourceIP(r), numDisks, hip.hs, hip.forceStart)
		go func() {
			respBytes, apiErr, errMsg := globalAllHealState.LaunchNewHealSequence(nh)
			hr := healResp{respBytes, apiErr, errMsg}
			respCh <- hr
		}()
	}

	// Due to the force-starting functionality, the Launch
	// call above can take a long time - to keep the
	// connection alive, we start sending whitespace
	keepConnLive(w, r, respCh)
}

func (a adminAPIHandlers) BackgroundHealStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HealBackgroundStatus")

	defer logger.AuditLog(w, r, "HealBackgroundStatus", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	// Check if this setup has an erasure coded backend.
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrHealNotImplemented), r.URL)
		return
	}

	var bgHealStates []madmin.BgHealState

	// Get local heal status first
	bgHealStates = append(bgHealStates, getLocalBackgroundHealStatus())

	if globalIsDistErasure {
		// Get heal status from other peers
		peersHealStates := globalNotificationSys.BackgroundHealStatus()
		bgHealStates = append(bgHealStates, peersHealStates...)
	}

	// Aggregate healing result
	var aggregatedHealStateResult = madmin.BgHealState{
		ScannedItemsCount: bgHealStates[0].ScannedItemsCount,
		LastHealActivity:  bgHealStates[0].LastHealActivity,
		NextHealRound:     bgHealStates[0].NextHealRound,
	}

	bgHealStates = bgHealStates[1:]

	for _, state := range bgHealStates {
		aggregatedHealStateResult.ScannedItemsCount += state.ScannedItemsCount
		if !state.LastHealActivity.IsZero() && aggregatedHealStateResult.LastHealActivity.Before(state.LastHealActivity) {
			aggregatedHealStateResult.LastHealActivity = state.LastHealActivity
			// The node which has the last heal activity means its
			// is the node that is orchestrating self healing operations,
			// which also means it is the same node which decides when
			// the next self healing operation will be done.
			aggregatedHealStateResult.NextHealRound = state.NextHealRound
		}
	}

	if err := json.NewEncoder(w).Encode(aggregatedHealStateResult); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

func validateAdminReq(ctx context.Context, w http.ResponseWriter, r *http.Request, action iampolicy.AdminAction) (ObjectLayer, auth.Credentials) {
	var cred auth.Credentials
	var adminAPIErr APIErrorCode
	// Get current object layer instance.
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil, cred
	}

	// Validate request signature.
	cred, adminAPIErr = checkAdminRequestAuthType(ctx, r, action, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return nil, cred
	}

	return objectAPI, cred
}

// AdminError - is a generic error for all admin APIs.
type AdminError struct {
	Code       string
	Message    string
	StatusCode int
}

func (ae AdminError) Error() string {
	return ae.Message
}

// Admin API errors
const (
	AdminUpdateUnexpectedFailure = "XMinioAdminUpdateUnexpectedFailure"
	AdminUpdateURLNotReachable   = "XMinioAdminUpdateURLNotReachable"
	AdminUpdateApplyFailure      = "XMinioAdminUpdateApplyFailure"
)

// toAdminAPIErrCode - converts errErasureWriteQuorum error to admin API
// specific error.
func toAdminAPIErrCode(ctx context.Context, err error) APIErrorCode {
	switch err {
	case errErasureWriteQuorum:
		return ErrAdminConfigNoQuorum
	default:
		return toAPIErrorCode(ctx, err)
	}
}

func toAdminAPIErr(ctx context.Context, err error) APIError {
	if err == nil {
		return noError
	}

	var apiErr APIError
	switch e := err.(type) {
	case iampolicy.Error:
		apiErr = APIError{
			Code:           "XMinioMalformedIAMPolicy",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case config.Error:
		apiErr = APIError{
			Code:           "XMinioConfigError",
			Description:    e.Error(),
			HTTPStatusCode: http.StatusBadRequest,
		}
	case AdminError:
		apiErr = APIError{
			Code:           e.Code,
			Description:    e.Message,
			HTTPStatusCode: e.StatusCode,
		}
	default:
		switch {
		case errors.Is(err, errConfigNotFound):
			apiErr = APIError{
				Code:           "XMinioConfigError",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusNotFound,
			}
		case errors.Is(err, errIAMActionNotAllowed):
			apiErr = APIError{
				Code:           "XMinioIAMActionNotAllowed",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusForbidden,
			}
		case errors.Is(err, errIAMNotInitialized):
			apiErr = APIError{
				Code:           "XMinioIAMNotInitialized",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusServiceUnavailable,
			}
		default:
			apiErr = errorCodes.ToAPIErrWithErr(toAdminAPIErrCode(ctx, err), err)
		}
	}
	return apiErr
}

// Returns true if the trace.Info should be traced,
// false if certain conditions are not met.
// - input entry is not of the type *trace.Info*
// - errOnly entries are to be traced, not status code 2xx, 3xx.
// - all entries to be traced, if not trace only S3 API requests.
func mustTrace(entry interface{}, trcAll, errOnly bool) bool {
	trcInfo, ok := entry.(trace.Info)
	if !ok {
		return false
	}
	trace := trcAll || !HasPrefix(trcInfo.ReqInfo.Path, minioReservedBucketPath+SlashSeparator)
	if errOnly {
		return trace && trcInfo.RespInfo.StatusCode >= http.StatusBadRequest
	}
	return trace
}

// TraceHandler - POST /minio/admin/v3/trace
// ----------
// The handler sends http trace to the connected HTTP client.
func (a adminAPIHandlers) TraceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HTTPTrace")

	trcAll := r.URL.Query().Get("all") == "true"
	trcErr := r.URL.Query().Get("err") == "true"

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuthType(ctx, r, iampolicy.TraceAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	w.Header().Set(xhttp.ContentType, "text/event-stream")

	// Trace Publisher and peer-trace-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	traceCh := make(chan interface{}, 4000)

	peers := newPeerRestClients(globalEndpoints)

	globalHTTPTrace.Subscribe(traceCh, ctx.Done(), func(entry interface{}) bool {
		return mustTrace(entry, trcAll, trcErr)
	})

	for _, peer := range peers {
		if peer == nil {
			continue
		}
		peer.Trace(traceCh, ctx.Done(), trcAll, trcErr)
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := json.NewEncoder(w)
	for {
		select {
		case entry := <-traceCh:
			if err := enc.Encode(entry); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-keepAliveTicker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-ctx.Done():
			return
		}
	}
}

// The handler sends console logs to the connected HTTP client.
func (a adminAPIHandlers) ConsoleLogHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ConsoleLog")

	defer logger.AuditLog(w, r, "ConsoleLog", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConsoleLogAdminAction)
	if objectAPI == nil {
		return
	}
	node := r.URL.Query().Get("node")
	// limit buffered console entries if client requested it.
	limitStr := r.URL.Query().Get("limit")
	limitLines, err := strconv.Atoi(limitStr)
	if err != nil {
		limitLines = 10
	}

	logKind := r.URL.Query().Get("logType")
	if logKind == "" {
		logKind = string(logger.All)
	}
	logKind = strings.ToUpper(logKind)

	// Avoid reusing tcp connection if read timeout is hit
	// This is needed to make r.Context().Done() work as
	// expected in case of read timeout
	w.Header().Add("Connection", "close")
	w.Header().Set(xhttp.ContentType, "text/event-stream")

	logCh := make(chan interface{}, 4000)

	peers := newPeerRestClients(globalEndpoints)

	globalConsoleSys.Subscribe(logCh, ctx.Done(), node, limitLines, logKind, nil)

	for _, peer := range peers {
		if peer == nil {
			continue
		}
		if node == "" || strings.EqualFold(peer.host.Name, node) {
			peer.ConsoleLog(logCh, ctx.Done())
		}
	}

	enc := json.NewEncoder(w)

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	for {
		select {
		case entry := <-logCh:
			log, ok := entry.(log.Info)
			if ok && log.SendLog(node, logKind) {
				if err := enc.Encode(log); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		case <-keepAliveTicker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-ctx.Done():
			return
		}
	}
}

// KMSKeyStatusHandler - GET /minio/admin/v3/kms/key/status?key-id=<master-key-id>
func (a adminAPIHandlers) KMSKeyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSKeyStatus")

	defer logger.AuditLog(w, r, "KMSKeyStatus", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSKeyStatusAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	keyID := r.URL.Query().Get("key-id")
	if keyID == "" {
		keyID = GlobalKMS.KeyID()
	}
	var response = madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := crypto.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, sealedKey, err := GlobalKMS.GenerateKey(keyID, kmsContext)
	if err != nil {
		response.EncryptionErr = err.Error()
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	// 2. Verify that we can indeed decrypt the (encrypted) key
	decryptedKey, err := GlobalKMS.UnsealKey(keyID, sealedKey, kmsContext)
	if err != nil {
		response.DecryptionErr = err.Error()
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	// 3. Compare generated key with decrypted key
	if subtle.ConstantTimeCompare(key[:], decryptedKey[:]) != 1 {
		response.DecryptionErr = "The generated and the decrypted data key do not match"
		resp, err := json.Marshal(response)
		if err != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
			return
		}
		writeSuccessResponseJSON(w, resp)
		return
	}

	resp, err := json.Marshal(response)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, resp)
}

// OBDInfoHandler - GET /minio/admin/v3/obdinfo
// ----------
// Get server on-board diagnostics
func (a adminAPIHandlers) OBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "OBDInfo")

	defer logger.AuditLog(w, r, "OBDInfo", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.OBDInfoAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	obdInfo := madmin.OBDInfo{}
	obdInfoCh := make(chan madmin.OBDInfo)

	enc := json.NewEncoder(w)
	partialWrite := func(oinfo madmin.OBDInfo) {
		obdInfoCh <- oinfo
	}

	setCommonHeaders(w)
	w.Header().Set(xhttp.ContentType, "text/event-stream")
	w.WriteHeader(http.StatusOK)

	errResp := func(err error) {
		errorResponse := getAPIErrorResponse(ctx, toAdminAPIErr(ctx, err), r.URL.String(),
			w.Header().Get(xhttp.AmzRequestID), globalDeploymentID)
		encodedErrorResponse := encodeResponse(errorResponse)
		obdInfo.Error = string(encodedErrorResponse)
		logger.LogIf(ctx, enc.Encode(obdInfo))
	}

	deadline := 3600 * time.Second
	if dstr := r.URL.Query().Get("deadline"); dstr != "" {
		var err error
		deadline, err = time.ParseDuration(dstr)
		if err != nil {
			errResp(err)
			return
		}
	}

	deadlinedCtx, cancel := context.WithTimeout(ctx, deadline)

	defer cancel()

	nsLock := objectAPI.NewNSLock(deadlinedCtx, minioMetaBucket, "obd-in-progress")
	if err := nsLock.GetLock(newDynamicTimeout(deadline, deadline)); err != nil { // returns a locked lock
		errResp(err)
		return
	}
	defer nsLock.Unlock()

	go func() {
		defer close(obdInfoCh)

		if cpu, ok := vars["syscpu"]; ok && cpu == "true" {
			cpuInfo := getLocalCPUOBDInfo(deadlinedCtx, r)

			obdInfo.Sys.CPUInfo = append(obdInfo.Sys.CPUInfo, cpuInfo)
			obdInfo.Sys.CPUInfo = append(obdInfo.Sys.CPUInfo, globalNotificationSys.CPUOBDInfo(deadlinedCtx)...)
			partialWrite(obdInfo)
		}

		if diskHw, ok := vars["sysdiskhw"]; ok && diskHw == "true" {
			diskHwInfo := getLocalDiskHwOBD(deadlinedCtx, r)

			obdInfo.Sys.DiskHwInfo = append(obdInfo.Sys.DiskHwInfo, diskHwInfo)
			obdInfo.Sys.DiskHwInfo = append(obdInfo.Sys.DiskHwInfo, globalNotificationSys.DiskHwOBDInfo(deadlinedCtx)...)
			partialWrite(obdInfo)
		}

		if osInfo, ok := vars["sysosinfo"]; ok && osInfo == "true" {
			osInfo := getLocalOsInfoOBD(deadlinedCtx, r)

			obdInfo.Sys.OsInfo = append(obdInfo.Sys.OsInfo, osInfo)
			obdInfo.Sys.OsInfo = append(obdInfo.Sys.OsInfo, globalNotificationSys.OsOBDInfo(deadlinedCtx)...)
			partialWrite(obdInfo)
		}

		if mem, ok := vars["sysmem"]; ok && mem == "true" {
			memInfo := getLocalMemOBD(deadlinedCtx, r)

			obdInfo.Sys.MemInfo = append(obdInfo.Sys.MemInfo, memInfo)
			obdInfo.Sys.MemInfo = append(obdInfo.Sys.MemInfo, globalNotificationSys.MemOBDInfo(deadlinedCtx)...)
			partialWrite(obdInfo)
		}

		if proc, ok := vars["sysprocess"]; ok && proc == "true" {
			procInfo := getLocalProcOBD(deadlinedCtx, r)

			obdInfo.Sys.ProcInfo = append(obdInfo.Sys.ProcInfo, procInfo)
			obdInfo.Sys.ProcInfo = append(obdInfo.Sys.ProcInfo, globalNotificationSys.ProcOBDInfo(deadlinedCtx)...)
			partialWrite(obdInfo)
		}

		if config, ok := vars["minioconfig"]; ok && config == "true" {
			cfg, err := readServerConfig(ctx, objectAPI)
			logger.LogIf(ctx, err)
			obdInfo.Minio.Config = cfg
			partialWrite(obdInfo)
		}

		if drive, ok := vars["perfdrive"]; ok && drive == "true" {
			// Get drive obd details from local server's drive(s)
			driveOBDSerial := getLocalDrivesOBD(deadlinedCtx, false, globalEndpoints, r)
			driveOBDParallel := getLocalDrivesOBD(deadlinedCtx, true, globalEndpoints, r)

			errStr := ""
			if driveOBDSerial.Error != "" {
				errStr = "serial: " + driveOBDSerial.Error
			}
			if driveOBDParallel.Error != "" {
				errStr = errStr + " parallel: " + driveOBDParallel.Error
			}

			driveOBD := madmin.ServerDrivesOBDInfo{
				Addr:     driveOBDSerial.Addr,
				Serial:   driveOBDSerial.Serial,
				Parallel: driveOBDParallel.Parallel,
				Error:    errStr,
			}
			obdInfo.Perf.DriveInfo = append(obdInfo.Perf.DriveInfo, driveOBD)
			partialWrite(obdInfo)

			// Notify all other MinIO peers to report drive obd numbers
			driveOBDs := globalNotificationSys.DriveOBDInfoChan(deadlinedCtx)
			for obd := range driveOBDs {
				obdInfo.Perf.DriveInfo = append(obdInfo.Perf.DriveInfo, obd)
				partialWrite(obdInfo)
			}
			partialWrite(obdInfo)
		}

		if net, ok := vars["perfnet"]; ok && net == "true" && globalIsDistErasure {
			obdInfo.Perf.Net = append(obdInfo.Perf.Net, globalNotificationSys.NetOBDInfo(deadlinedCtx))
			partialWrite(obdInfo)

			netOBDs := globalNotificationSys.DispatchNetOBDChan(deadlinedCtx)
			for obd := range netOBDs {
				obdInfo.Perf.Net = append(obdInfo.Perf.Net, obd)
				partialWrite(obdInfo)
			}
			partialWrite(obdInfo)

			obdInfo.Perf.NetParallel = globalNotificationSys.NetOBDParallelInfo(deadlinedCtx)
			partialWrite(obdInfo)
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case oinfo, ok := <-obdInfoCh:
			if !ok {
				return
			}
			logger.LogIf(ctx, enc.Encode(oinfo))
			w.(http.Flusher).Flush()
		case <-ticker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-deadlinedCtx.Done():
			w.(http.Flusher).Flush()
			return
		}
	}

}

// ServerInfoHandler - GET /minio/admin/v3/info
// ----------
// Get server information
func (a adminAPIHandlers) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ServerInfo")

	defer logger.AuditLog(w, r, "ServerInfo", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerInfoAdminAction)
	if objectAPI == nil {
		return
	}

	cfg, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	buckets := madmin.Buckets{}
	objects := madmin.Objects{}
	usage := madmin.Usage{}

	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err == nil {
		buckets = madmin.Buckets{Count: dataUsageInfo.BucketsCount}
		objects = madmin.Objects{Count: dataUsageInfo.ObjectsTotalCount}
		usage = madmin.Usage{Size: dataUsageInfo.ObjectsTotalSize}
	}

	vault := fetchVaultStatus(cfg)

	ldap := madmin.LDAP{}
	if globalLDAPConfig.Enabled {
		ldapConn, err := globalLDAPConfig.Connect()
		if err != nil {
			ldap.Status = "offline"
		} else if ldapConn == nil {
			ldap.Status = "Not Configured"
		} else {
			// Close ldap connection to avoid leaks.
			ldapConn.Close()
			ldap.Status = "online"
		}
	}

	log, audit := fetchLoggerInfo(cfg)

	// Get the notification target info
	notifyTarget := fetchLambdaInfo(cfg)

	// Fetching the Storage information, ignore any errors.
	storageInfo, _ := objectAPI.StorageInfo(ctx, false)

	var OnDisks int
	var OffDisks int
	var backend interface{}

	if storageInfo.Backend.Type == BackendType(madmin.Erasure) {

		for _, v := range storageInfo.Backend.OnlineDisks {
			OnDisks += v
		}
		for _, v := range storageInfo.Backend.OfflineDisks {
			OffDisks += v
		}

		backend = madmin.ErasureBackend{
			Type:             madmin.ErasureType,
			OnlineDisks:      OnDisks,
			OfflineDisks:     OffDisks,
			StandardSCData:   storageInfo.Backend.StandardSCData,
			StandardSCParity: storageInfo.Backend.StandardSCParity,
			RRSCData:         storageInfo.Backend.RRSCData,
			RRSCParity:       storageInfo.Backend.RRSCParity,
		}
	} else {
		backend = madmin.FSBackend{
			Type: madmin.FsType,
		}
	}

	mode := ""
	if globalSafeMode {
		mode = "safe"
	} else {
		mode = "online"
	}

	server := getLocalServerProperty(globalEndpoints, r)
	servers := globalNotificationSys.ServerInfo()
	servers = append(servers, server)

	for _, sp := range servers {
		for i, di := range sp.Disks {
			path := ""
			if globalIsErasure {
				path = di.DrivePath
			}
			if globalIsDistErasure {
				path = sp.Endpoint + di.DrivePath
			}
			// For distributed
			for a := range storageInfo.Backend.Sets {
				for b := range storageInfo.Backend.Sets[a] {
					ep := storageInfo.Backend.Sets[a][b].Endpoint

					if globalIsDistErasure {
						if strings.Replace(ep, "http://", "", -1) == path || strings.Replace(ep, "https://", "", -1) == path {
							sp.Disks[i].State = storageInfo.Backend.Sets[a][b].State
							sp.Disks[i].UUID = storageInfo.Backend.Sets[a][b].UUID
						}
					}
					if globalIsErasure {
						if ep == path {
							sp.Disks[i].State = storageInfo.Backend.Sets[a][b].State
							sp.Disks[i].UUID = storageInfo.Backend.Sets[a][b].UUID
						}
					}
				}
			}

		}
	}

	domain := globalDomainNames
	services := madmin.Services{
		Vault:         vault,
		LDAP:          ldap,
		Logger:        log,
		Audit:         audit,
		Notifications: notifyTarget,
	}

	infoMsg := madmin.InfoMessage{
		Mode:         mode,
		Domain:       domain,
		Region:       globalServerRegion,
		SQSARN:       globalNotificationSys.GetARNList(false),
		DeploymentID: globalDeploymentID,
		Buckets:      buckets,
		Objects:      objects,
		Usage:        usage,
		Services:     services,
		Backend:      backend,
		Servers:      servers,
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(infoMsg)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	//Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

func fetchLambdaInfo(cfg config.Config) []map[string][]madmin.TargetIDStatus {

	// Fetch the configured targets
	tr := NewGatewayHTTPTransport()
	defer tr.CloseIdleConnections()
	targetList, err := notify.FetchRegisteredTargets(cfg, GlobalContext.Done(), tr, true, false)
	if err != nil && err != notify.ErrTargetsOffline {
		logger.LogIf(GlobalContext, err)
		return nil
	}

	lambdaMap := make(map[string][]madmin.TargetIDStatus)

	for targetID, target := range targetList.TargetMap() {
		targetIDStatus := make(map[string]madmin.Status)
		active, _ := target.IsActive()
		if active {
			targetIDStatus[targetID.ID] = madmin.Status{Status: "Online"}
		} else {
			targetIDStatus[targetID.ID] = madmin.Status{Status: "Offline"}
		}
		list := lambdaMap[targetID.Name]
		list = append(list, targetIDStatus)
		lambdaMap[targetID.Name] = list
		// Close any leaking connections
		_ = target.Close()
	}

	notify := make([]map[string][]madmin.TargetIDStatus, len(lambdaMap))
	counter := 0
	for key, value := range lambdaMap {
		v := make(map[string][]madmin.TargetIDStatus)
		v[key] = value
		notify[counter] = v
		counter++
	}
	return notify
}

// fetchVaultStatus fetches Vault Info
func fetchVaultStatus(cfg config.Config) madmin.Vault {
	vault := madmin.Vault{}
	if GlobalKMS == nil {
		vault.Status = "disabled"
		return vault
	}
	keyID := GlobalKMS.KeyID()
	kmsInfo := GlobalKMS.Info()

	if kmsInfo.Endpoint == "" {
		vault.Status = "KMS configured using master key"
		return vault
	}

	if err := checkConnection(kmsInfo.Endpoint, 15*time.Second); err != nil {
		vault.Status = "offline"
	} else {
		vault.Status = "online"

		kmsContext := crypto.Context{"MinIO admin API": "ServerInfoHandler"} // Context for a test key operation
		// 1. Generate a new key using the KMS.
		key, sealedKey, err := GlobalKMS.GenerateKey(keyID, kmsContext)
		if err != nil {
			vault.Encrypt = fmt.Sprintf("Encryption failed: %v", err)
		} else {
			vault.Encrypt = "Ok"
		}

		// 2. Verify that we can indeed decrypt the (encrypted) key
		decryptedKey, err := GlobalKMS.UnsealKey(keyID, sealedKey, kmsContext)
		switch {
		case err != nil:
			vault.Decrypt = fmt.Sprintf("Decryption failed: %v", err)
		case subtle.ConstantTimeCompare(key[:], decryptedKey[:]) != 1:
			vault.Decrypt = "Decryption failed: decrypted key does not match generated key"
		default:
			vault.Decrypt = "Ok"
		}
	}
	return vault
}

// fetchLoggerDetails return log info
func fetchLoggerInfo(cfg config.Config) ([]madmin.Logger, []madmin.Audit) {
	loggerCfg, _ := logger.LookupConfig(cfg)

	var logger []madmin.Logger
	var auditlogger []madmin.Audit
	for log, l := range loggerCfg.HTTP {
		if l.Enabled {
			err := checkConnection(l.Endpoint, 15*time.Second)
			if err == nil {
				mapLog := make(map[string]madmin.Status)
				mapLog[log] = madmin.Status{Status: "Online"}
				logger = append(logger, mapLog)
			} else {
				mapLog := make(map[string]madmin.Status)
				mapLog[log] = madmin.Status{Status: "offline"}
				logger = append(logger, mapLog)
			}
		}
	}

	for audit, l := range loggerCfg.Audit {
		if l.Enabled {
			err := checkConnection(l.Endpoint, 15*time.Second)
			if err == nil {
				mapAudit := make(map[string]madmin.Status)
				mapAudit[audit] = madmin.Status{Status: "Online"}
				auditlogger = append(auditlogger, mapAudit)
			} else {
				mapAudit := make(map[string]madmin.Status)
				mapAudit[audit] = madmin.Status{Status: "Offline"}
				auditlogger = append(auditlogger, mapAudit)
			}
		}
	}
	return logger, auditlogger
}

// checkConnection - ping an endpoint , return err in case of no connection
func checkConnection(endpointStr string, timeout time.Duration) error {
	tr := newCustomHTTPTransport(&tls.Config{RootCAs: globalRootCAs}, timeout)()
	defer tr.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(GlobalContext, timeout)
	defer cancel()

	req, err := http.NewRequest(http.MethodHead, endpointStr, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(resp.Body)
	resp.Body.Close()
	return nil
}
