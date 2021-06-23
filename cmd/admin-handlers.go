// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	"github.com/minio/kes"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/message/log"
	iampolicy "github.com/minio/pkg/iam/policy"
	xnet "github.com/minio/pkg/net"
)

const (
	maxEConfigJSONSize = 262272
)

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket      = "bucket"
	mgmtPrefix      = "prefix"
	mgmtClientToken = "clientToken"
	mgmtForceStart  = "forceStart"
	mgmtForceStop   = "forceStop"
)

func updateServer(u *url.URL, sha256Sum []byte, lrTime time.Time, releaseInfo string, mode string) (us madmin.ServerUpdateStatus, err error) {
	if err = doUpdate(u, lrTime, sha256Sum, releaseInfo, mode); err != nil {
		return us, err
	}

	us.CurrentVersion = Version
	us.UpdatedVersion = lrTime.Format(minioReleaseTagTimeLayout)
	return us, nil
}

// ServerUpdateHandler - POST /minio/admin/v3/update?updateURL={updateURL}
// ----------
// updates all minio servers and restarts them gracefully.
func (a adminAPIHandlers) ServerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ServerUpdate")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if globalInplaceUpdateDisabled {
		// if MINIO_UPDATE=off - inplace update is disabled, mostly in containers.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	updateURL := vars["updateURL"]
	mode := getMinioMode()
	if updateURL == "" {
		updateURL = minioReleaseInfoURL
		if runtime.GOOS == globalWindowsOSName {
			updateURL = minioReleaseWindowsInfoURL
		}
	}

	u, err := url.Parse(updateURL)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	content, err := downloadReleaseURL(u, updateTimeout, mode)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	sha256Sum, lrTime, releaseInfo, err := parseReleaseData(content)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	u.Path = path.Dir(u.Path) + SlashSeparator + releaseInfo
	crTime, err := GetCurrentReleaseTime()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if lrTime.Sub(crTime) <= 0 {
		updateStatus := madmin.ServerUpdateStatus{
			CurrentVersion: Version,
			UpdatedVersion: Version,
		}

		// Marshal API response
		jsonBytes, err := json.Marshal(updateStatus)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		writeSuccessResponseJSON(w, jsonBytes)
		return
	}

	for _, nerr := range globalNotificationSys.ServerUpdate(ctx, u, sha256Sum, lrTime, releaseInfo) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
			err = fmt.Errorf("Server update failed, please do not restart the servers yet: failed with %w", nerr.Err)
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	updateStatus, err := updateServer(u, sha256Sum, lrTime, releaseInfo, mode)
	if err != nil {
		err = fmt.Errorf("Server update failed, please do not restart the servers yet: failed with %w", err)
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

	// Notify all other MinIO peers signal service.
	for _, nerr := range globalNotificationSys.SignalService(serviceRestart) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	globalServiceSignalCh <- serviceRestart
}

// ServiceHandler - POST /minio/admin/v3/service?action={action}
// ----------
// restarts/stops minio server gracefully. In a distributed setup,
func (a adminAPIHandlers) ServiceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "Service")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
	switch serviceSig {
	case serviceRestart:
		objectAPI, _ = validateAdminReq(ctx, w, r, iampolicy.ServiceRestartAdminAction)
	case serviceStop:
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
	S3RequestsInQueue      int32              `json:"s3RequestsInQueue"`
	CurrentS3Requests      ServerHTTPAPIStats `json:"currentS3Requests"`
	TotalS3Requests        ServerHTTPAPIStats `json:"totalS3Requests"`
	TotalS3Errors          ServerHTTPAPIStats `json:"totalS3Errors"`
	TotalS3Canceled        ServerHTTPAPIStats `json:"totalS3Canceled"`
	TotalS3RejectedAuth    uint64             `json:"totalS3RejectedAuth"`
	TotalS3RejectedTime    uint64             `json:"totalS3RejectedTime"`
	TotalS3RejectedHeader  uint64             `json:"totalS3RejectedHeader"`
	TotalS3RejectedInvalid uint64             `json:"totalS3RejectedInvalid"`
}

// StorageInfoHandler - GET /minio/admin/v3/storageinfo
// ----------
// Get server information
func (a adminAPIHandlers) StorageInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StorageInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.StorageInfoAdminAction)
	if objectAPI == nil {
		return
	}

	// ignores any errors here.
	storageInfo, _ := objectAPI.StorageInfo(ctx)

	// Collect any disk healing.
	healing, _ := getAggregatedBackgroundHealState(ctx, nil)
	healDisks := make(map[string]struct{}, len(healing.HealDisks))
	for _, disk := range healing.HealDisks {
		healDisks[disk] = struct{}{}
	}

	// find all disks which belong to each respective endpoints
	for i, disk := range storageInfo.Disks {
		if _, ok := healDisks[disk.Endpoint]; ok {
			storageInfo.Disks[i].Healing = true
		}
	}

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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
		Owner:      l.Owner,
		ID:         l.UID,
		Quorum:     l.Quorum,
	}
	if l.Writer {
		entry.Type = "WRITE"
	} else {
		entry.Type = "READ"
	}
	return entry
}

func topLockEntries(peerLocks []*PeerLocks, stale bool) madmin.LockEntries {
	entryMap := make(map[string]*madmin.LockEntry)
	for _, peerLock := range peerLocks {
		if peerLock == nil {
			continue
		}
		for k, v := range peerLock.Locks {
			for _, lockReqInfo := range v {
				if val, ok := entryMap[lockReqInfo.UID]; ok {
					val.ServerList = append(val.ServerList, peerLock.Addr)
				} else {
					entryMap[lockReqInfo.UID] = lriToLockEntry(lockReqInfo, k, peerLock.Addr)
				}
			}
		}
	}
	var lockEntries madmin.LockEntries
	for _, v := range entryMap {
		if stale {
			lockEntries = append(lockEntries, *v)
			continue
		}
		if len(v.ServerList) >= v.Quorum {
			lockEntries = append(lockEntries, *v)
		}
	}
	sort.Sort(lockEntries)
	return lockEntries
}

// PeerLocks holds server information result of one node
type PeerLocks struct {
	Addr  string
	Locks map[string][]lockRequesterInfo
}

// ForceUnlockHandler force unlocks requested resource
func (a adminAPIHandlers) ForceUnlockHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ForceUnlock")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ForceUnlockAdminAction)
	if objectAPI == nil {
		return
	}

	z, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)

	var args dsync.LockArgs
	var lockers []dsync.NetLocker
	for _, path := range strings.Split(vars["paths"], ",") {
		if path == "" {
			continue
		}
		args.Resources = append(args.Resources, path)
	}

	for _, lks := range z.serverPools[0].erasureLockers {
		lockers = append(lockers, lks...)
	}

	for _, locker := range lockers {
		locker.ForceUnlock(ctx, args)
	}
}

// TopLocksHandler Get list of locks in use
func (a adminAPIHandlers) TopLocksHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "TopLocks")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
	stale := r.URL.Query().Get("stale") == "true" // list also stale locks

	peerLocks := globalNotificationSys.GetLocks(ctx, r)

	topLocks := topLockEntries(peerLocks, stale)

	// Marshal API response upto requested count.
	if len(topLocks) > count && count > 0 {
		topLocks = topLocks[:count]
	}

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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.ProfilingAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	if globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	profiles := strings.Split(vars["profilerType"], ",")
	thisAddr, err := xnet.ParseHost(globalLocalNodeName)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.ProfilingAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	if globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
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
	hip.bucket = vars[mgmtBucket]
	hip.objPrefix = vars[mgmtPrefix]

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

	if len(qParms[mgmtClientToken]) > 0 {
		hip.clientToken = qParms[mgmtClientToken][0]
	}
	if _, ok := qParms[mgmtForceStart]; ok {
		hip.forceStart = true
	}
	if _, ok := qParms[mgmtForceStop]; ok {
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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

	// Analyze the heal token and route the request accordingly
	token, success := proxyRequestByToken(ctx, w, r, hip.clientToken)
	if success {
		return
	}
	hip.clientToken = token
	// if request was not successful, try this server locally if token
	// is not found the call will fail anyways. if token is empty
	// try this server to generate a new token.

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
					setEventStreamHeaders(w)
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

	healPath := pathJoin(hip.bucket, hip.objPrefix)
	if hip.clientToken == "" && !hip.forceStart && !hip.forceStop {
		nh, exists := globalAllHealState.getHealSequence(healPath)
		if exists && !nh.hasEnded() && len(nh.currentStatus.Items) > 0 {
			clientToken := nh.clientToken
			if globalIsDistErasure {
				clientToken = fmt.Sprintf("%s@%d", nh.clientToken, GetProxyEndpointLocalIndex(globalProxyEndpoints))
			}
			b, err := json.Marshal(madmin.HealStartSuccess{
				ClientToken:   clientToken,
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
		nh := newHealSequence(GlobalContext, hip.bucket, hip.objPrefix, handlers.GetSourceIP(r), hip.hs, hip.forceStart)
		go func() {
			respBytes, apiErr, errMsg := globalAllHealState.LaunchNewHealSequence(nh, objectAPI)
			hr := healResp{respBytes, apiErr, errMsg}
			respCh <- hr
		}()
	}

	// Due to the force-starting functionality, the Launch
	// call above can take a long time - to keep the
	// connection alive, we start sending whitespace
	keepConnLive(w, r, respCh)
}

// getAggregatedBackgroundHealState returns the heal state of disks.
// If no ObjectLayer is provided no set status is returned.
func getAggregatedBackgroundHealState(ctx context.Context, o ObjectLayer) (madmin.BgHealState, error) {
	// Get local heal status first
	bgHealStates, ok := getBackgroundHealStatus(ctx, o)
	if !ok {
		return bgHealStates, errServerNotInitialized
	}

	if globalIsDistErasure {
		// Get heal status from other peers
		peersHealStates, nerrs := globalNotificationSys.BackgroundHealStatus()
		var errCount int
		for _, nerr := range nerrs {
			if nerr.Err != nil {
				logger.LogIf(ctx, nerr.Err)
				errCount++
			}
		}
		if errCount == len(nerrs) {
			return madmin.BgHealState{}, fmt.Errorf("all remote servers failed to report heal status, cluster is unhealthy")
		}
		bgHealStates.Merge(peersHealStates...)
	}

	return bgHealStates, nil
}

func (a adminAPIHandlers) BackgroundHealStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HealBackgroundStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	// Check if this setup has an erasure coded backend.
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrHealNotImplemented), r.URL)
		return
	}

	aggregateHealStateResult, err := getAggregatedBackgroundHealState(r.Context(), objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := json.NewEncoder(w).Encode(aggregateHealStateResult); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

func validateAdminReq(ctx context.Context, w http.ResponseWriter, r *http.Request, action iampolicy.AdminAction) (ObjectLayer, auth.Credentials) {
	var cred auth.Credentials
	var adminAPIErr APIErrorCode
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil, cred
	}

	// Validate request signature.
	cred, adminAPIErr = checkAdminRequestAuth(ctx, r, action, "")
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
		case errors.Is(err, kes.ErrKeyExists):
			apiErr = APIError{
				Code:           "XMinioKMSKeyExists",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusConflict,
			}

		// Tier admin API errors
		case errors.Is(err, madmin.ErrTierNameEmpty):
			apiErr = APIError{
				Code:           "XMinioAdminTierNameEmpty",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierInvalidConfig):
			apiErr = APIError{
				Code:           "XMinioAdminTierInvalidConfig",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierInvalidConfigVersion):
			apiErr = APIError{
				Code:           "XMinioAdminTierInvalidConfigVersion",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, madmin.ErrTierTypeUnsupported):
			apiErr = APIError{
				Code:           "XMinioAdminTierTypeUnsupported",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errors.Is(err, errTierBackendInUse):
			apiErr = APIError{
				Code:           "XMinioAdminTierBackendInUse",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusConflict,
			}
		case errors.Is(err, errTierInsufficientCreds):
			apiErr = APIError{
				Code:           "XMinioAdminTierInsufficientCreds",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		case errIsTierPermError(err):
			apiErr = APIError{
				Code:           "XMinioAdminTierInsufficientPermissions",
				Description:    err.Error(),
				HTTPStatusCode: http.StatusBadRequest,
			}
		default:
			apiErr = errorCodes.ToAPIErrWithErr(toAdminAPIErrCode(ctx, err), err)
		}
	}
	return apiErr
}

// Returns true if the madmin.TraceInfo should be traced,
// false if certain conditions are not met.
// - input entry is not of the type *madmin.TraceInfo*
// - errOnly entries are to be traced, not status code 2xx, 3xx.
// - madmin.TraceInfo type is asked by opts
func mustTrace(entry interface{}, opts madmin.ServiceTraceOpts) (shouldTrace bool) {
	trcInfo, ok := entry.(madmin.TraceInfo)
	if !ok {
		return false
	}

	// Override shouldTrace decision with errOnly filtering
	defer func() {
		if shouldTrace && opts.OnlyErrors {
			shouldTrace = trcInfo.RespInfo.StatusCode >= http.StatusBadRequest
		}
	}()

	if opts.Threshold > 0 {
		var latency time.Duration
		switch trcInfo.TraceType {
		case madmin.TraceOS:
			latency = trcInfo.OSStats.Duration
		case madmin.TraceStorage:
			latency = trcInfo.StorageStats.Duration
		case madmin.TraceHTTP:
			latency = trcInfo.CallStats.Latency
		}
		if latency < opts.Threshold {
			return false
		}
	}

	if opts.Internal && trcInfo.TraceType == madmin.TraceHTTP && HasPrefix(trcInfo.ReqInfo.Path, minioReservedBucketPath+SlashSeparator) {
		return true
	}

	if opts.S3 && trcInfo.TraceType == madmin.TraceHTTP && !HasPrefix(trcInfo.ReqInfo.Path, minioReservedBucketPath+SlashSeparator) {
		return true
	}

	if opts.Storage && trcInfo.TraceType == madmin.TraceStorage {
		return true
	}

	return opts.OS && trcInfo.TraceType == madmin.TraceOS
}

func extractTraceOptions(r *http.Request) (opts madmin.ServiceTraceOpts, err error) {
	q := r.URL.Query()

	opts.OnlyErrors = q.Get("err") == "true"
	opts.S3 = q.Get("s3") == "true"
	opts.Internal = q.Get("internal") == "true"
	opts.Storage = q.Get("storage") == "true"
	opts.OS = q.Get("os") == "true"

	// Support deprecated 'all' query
	if q.Get("all") == "true" {
		opts.S3 = true
		opts.Internal = true
		opts.Storage = true
		opts.OS = true
	}

	if t := q.Get("threshold"); t != "" {
		d, err := time.ParseDuration(t)
		if err != nil {
			return opts, err
		}
		opts.Threshold = d
	}
	return
}

// TraceHandler - POST /minio/admin/v3/trace
// ----------
// The handler sends http trace to the connected HTTP client.
func (a adminAPIHandlers) TraceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HTTPTrace")

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.TraceAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	traceOpts, err := extractTraceOptions(r)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	setEventStreamHeaders(w)

	// Trace Publisher and peer-trace-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	traceCh := make(chan interface{}, 4000)

	peers, _ := newPeerRestClients(globalEndpoints)

	globalTrace.Subscribe(traceCh, ctx.Done(), func(entry interface{}) bool {
		return mustTrace(entry, traceOpts)
	})

	for _, peer := range peers {
		if peer == nil {
			continue
		}
		peer.Trace(traceCh, ctx.Done(), traceOpts)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
	w.Header().Set("Connection", "close")

	setEventStreamHeaders(w)

	logCh := make(chan interface{}, 4000)

	peers, _ := newPeerRestClients(globalEndpoints)

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

// KMSCreateKeyHandler - POST /minio/admin/v3/kms/key/create?key-id=<master-key-id>
func (a adminAPIHandlers) KMSCreateKeyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSCreateKey")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSCreateKeyAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	if err := GlobalKMS.CreateKey(r.URL.Query().Get("key-id")); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSKeyStatusHandler - GET /minio/admin/v3/kms/status
func (a adminAPIHandlers) KMSStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSStatus")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSKeyStatusAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Stat()
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}

	status := madmin.KMSStatus{
		Name:         stat.Name,
		DefaultKeyID: stat.DefaultKey,
		Endpoints:    make(map[string]madmin.ItemState, len(stat.Endpoints)),
	}
	for _, endpoint := range stat.Endpoints {
		status.Endpoints[endpoint] = madmin.ItemOnline // TODO(aead): Implement an online check for mTLS
	}

	resp, err := json.Marshal(status)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, resp)
}

// KMSKeyStatusHandler - GET /minio/admin/v3/kms/key/status?key-id=<master-key-id>
func (a adminAPIHandlers) KMSKeyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "KMSKeyStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.KMSKeyStatusAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Stat()
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}

	keyID := r.URL.Query().Get("key-id")
	if keyID == "" {
		keyID = stat.DefaultKey
	}
	var response = madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := kms.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey(keyID, kmsContext)
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
	decryptedKey, err := GlobalKMS.DecryptKey(key.KeyID, key.Ciphertext, kmsContext)
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
	if subtle.ConstantTimeCompare(key.Plaintext, decryptedKey) != 1 {
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

func getServerInfo(ctx context.Context, r *http.Request) madmin.InfoMessage {
	kmsStat := fetchKMSStatus()

	ldap := madmin.LDAP{}
	if globalLDAPConfig.Enabled {
		ldapConn, err := globalLDAPConfig.Connect()
		if err != nil {
			ldap.Status = string(madmin.ItemOffline)
		} else if ldapConn == nil {
			ldap.Status = "Not Configured"
		} else {
			// Close ldap connection to avoid leaks.
			ldapConn.Close()
			ldap.Status = string(madmin.ItemOnline)
		}
	}

	log, audit := fetchLoggerInfo()

	// Get the notification target info
	notifyTarget := fetchLambdaInfo()

	local := getLocalServerProperty(globalEndpoints, r)
	servers := globalNotificationSys.ServerInfo()
	servers = append(servers, local)

	assignPoolNumbers(servers)

	var backend interface{}
	mode := madmin.ItemInitializing

	buckets := madmin.Buckets{}
	objects := madmin.Objects{}
	usage := madmin.Usage{}

	objectAPI := newObjectLayerFn()
	if objectAPI != nil {
		mode = madmin.ItemOnline

		// Load data usage
		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
		if err == nil {
			buckets = madmin.Buckets{Count: dataUsageInfo.BucketsCount}
			objects = madmin.Objects{Count: dataUsageInfo.ObjectsTotalCount}
			usage = madmin.Usage{Size: dataUsageInfo.ObjectsTotalSize}
		} else {
			buckets = madmin.Buckets{Error: err.Error()}
			objects = madmin.Objects{Error: err.Error()}
			usage = madmin.Usage{Error: err.Error()}
		}

		// Fetching the backend information
		backendInfo := objectAPI.BackendInfo()
		if backendInfo.Type == madmin.Erasure {
			// Calculate the number of online/offline disks of all nodes
			var allDisks []madmin.Disk
			for _, s := range servers {
				allDisks = append(allDisks, s.Disks...)
			}
			onlineDisks, offlineDisks := getOnlineOfflineDisksStats(allDisks)

			backend = madmin.ErasureBackend{
				Type:             madmin.ErasureType,
				OnlineDisks:      onlineDisks.Sum(),
				OfflineDisks:     offlineDisks.Sum(),
				StandardSCParity: backendInfo.StandardSCParity,
				RRSCParity:       backendInfo.RRSCParity,
			}
		} else {
			backend = madmin.FSBackend{
				Type: madmin.FsType,
			}
		}
	}

	domain := globalDomainNames
	services := madmin.Services{
		KMS:           kmsStat,
		LDAP:          ldap,
		Logger:        log,
		Audit:         audit,
		Notifications: notifyTarget,
	}

	return madmin.InfoMessage{
		Mode:         string(mode),
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
}

// HealthInfoHandler - GET /minio/admin/v3/healthinfo
// ----------
// Get server health info
func (a adminAPIHandlers) HealthInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HealthInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	query := r.URL.Query()
	healthInfo := madmin.HealthInfo{Version: madmin.HealthInfoVersion}
	healthInfoCh := make(chan madmin.HealthInfo)

	enc := json.NewEncoder(w)
	partialWrite := func(oinfo madmin.HealthInfo) {
		healthInfoCh <- oinfo
	}

	setCommonHeaders(w)

	setEventStreamHeaders(w)

	w.WriteHeader(http.StatusOK)

	errResp := func(err error) {
		errorResponse := getAPIErrorResponse(ctx, toAdminAPIErr(ctx, err), r.URL.String(),
			w.Header().Get(xhttp.AmzRequestID), globalDeploymentID)
		encodedErrorResponse := encodeResponse(errorResponse)
		healthInfo.Error = string(encodedErrorResponse)
		logger.LogIf(ctx, enc.Encode(healthInfo))
	}

	deadline := 1 * time.Hour
	if dstr := r.URL.Query().Get("deadline"); dstr != "" {
		var err error
		deadline, err = time.ParseDuration(dstr)
		if err != nil {
			errResp(err)
			return
		}
	}

	deadlinedCtx, deadlineCancel := context.WithTimeout(ctx, deadline)
	defer deadlineCancel()

	nsLock := objectAPI.NewNSLock(minioMetaBucket, "health-check-in-progress")
	lkctx, err := nsLock.GetLock(ctx, newDynamicTimeout(deadline, deadline))
	if err != nil { // returns a locked lock
		errResp(err)
		return
	}
	defer nsLock.Unlock(lkctx.Cancel)

	go func() {
		defer close(healthInfoCh)

		partialWrite(healthInfo) // Write first message with only version populated

		if query.Get("syscpu") == "true" {
			healthInfo.Sys.CPUInfo = append(healthInfo.Sys.CPUInfo, madmin.GetCPUs(deadlinedCtx, r.Host))
			healthInfo.Sys.CPUInfo = append(healthInfo.Sys.CPUInfo, globalNotificationSys.GetCPUs(deadlinedCtx)...)
			partialWrite(healthInfo)
		}

		if query.Get("sysdrivehw") == "true" {
			healthInfo.Sys.Partitions = append(healthInfo.Sys.Partitions, madmin.GetPartitions(deadlinedCtx, r.Host))
			healthInfo.Sys.Partitions = append(healthInfo.Sys.Partitions, globalNotificationSys.GetPartitions(deadlinedCtx)...)
			partialWrite(healthInfo)
		}

		if query.Get("sysosinfo") == "true" {
			healthInfo.Sys.OSInfo = append(healthInfo.Sys.OSInfo, madmin.GetOSInfo(deadlinedCtx, r.Host))
			healthInfo.Sys.OSInfo = append(healthInfo.Sys.OSInfo, globalNotificationSys.GetOSInfo(deadlinedCtx)...)
			partialWrite(healthInfo)
		}

		if query.Get("sysmem") == "true" {
			healthInfo.Sys.MemInfo = append(healthInfo.Sys.MemInfo, madmin.GetMemInfo(deadlinedCtx, r.Host))
			healthInfo.Sys.MemInfo = append(healthInfo.Sys.MemInfo, globalNotificationSys.GetMemInfo(deadlinedCtx)...)
			partialWrite(healthInfo)
		}

		if query.Get("sysprocess") == "true" {
			healthInfo.Sys.ProcInfo = append(healthInfo.Sys.ProcInfo, madmin.GetProcInfo(deadlinedCtx, r.Host))
			healthInfo.Sys.ProcInfo = append(healthInfo.Sys.ProcInfo, globalNotificationSys.GetProcInfo(deadlinedCtx)...)
			partialWrite(healthInfo)
		}

		if query.Get("minioconfig") == "true" {
			config, err := readServerConfig(ctx, objectAPI)
			if err != nil {
				healthInfo.Minio.Config = madmin.MinioConfig{
					Error: err.Error(),
				}
			} else {
				healthInfo.Minio.Config = madmin.MinioConfig{
					Config: config.RedactSensitiveInfo(),
				}
			}
			partialWrite(healthInfo)
		}

		if query.Get("perfdrive") == "true" {
			healthInfo.Perf.Drives = append(healthInfo.Perf.Drives, getDrivePerfInfos(deadlinedCtx, r.Host))
			partialWrite(healthInfo)

			perfCh := globalNotificationSys.GetDrivePerfInfos(deadlinedCtx)
			for perfInfo := range perfCh {
				healthInfo.Perf.Drives = append(healthInfo.Perf.Drives, perfInfo)
				partialWrite(healthInfo)
			}

			partialWrite(healthInfo)
		}

		if globalIsDistErasure && query.Get("perfnet") == "true" {
			healthInfo.Perf.Net = append(healthInfo.Perf.Net, globalNotificationSys.GetNetPerfInfo(deadlinedCtx))
			partialWrite(healthInfo)

			netInfos := globalNotificationSys.DispatchNetPerfChan(deadlinedCtx)
			for netInfo := range netInfos {
				healthInfo.Perf.Net = append(healthInfo.Perf.Net, netInfo)
				partialWrite(healthInfo)
			}
			partialWrite(healthInfo)

			healthInfo.Perf.NetParallel = globalNotificationSys.GetParallelNetPerfInfo(deadlinedCtx)
			partialWrite(healthInfo)
		}

		if query.Get("minioinfo") == "true" {
			infoMessage := getServerInfo(ctx, r)
			servers := []madmin.ServerInfo{}
			for _, server := range infoMessage.Servers {
				servers = append(servers, madmin.ServerInfo{
					State:      server.State,
					Endpoint:   server.Endpoint,
					Uptime:     server.Uptime,
					Version:    server.Version,
					CommitID:   server.CommitID,
					Network:    server.Network,
					Drives:     server.Disks,
					PoolNumber: server.PoolNumber,
					MemStats: madmin.MemStats{
						Alloc:      server.MemStats.Alloc,
						TotalAlloc: server.MemStats.TotalAlloc,
						Mallocs:    server.MemStats.Mallocs,
						Frees:      server.MemStats.Frees,
						HeapAlloc:  server.MemStats.HeapAlloc,
					},
				})
			}
			healthInfo.Minio.Info = madmin.MinioInfo{
				Mode:         infoMessage.Mode,
				Domain:       infoMessage.Domain,
				Region:       infoMessage.Region,
				SQSARN:       infoMessage.SQSARN,
				DeploymentID: infoMessage.DeploymentID,
				Buckets:      infoMessage.Buckets,
				Objects:      infoMessage.Objects,
				Usage:        infoMessage.Usage,
				Services:     infoMessage.Services,
				Backend:      infoMessage.Backend,
				Servers:      servers,
			}
			partialWrite(healthInfo)
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case oinfo, ok := <-healthInfoCh:
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

// BandwidthMonitorHandler - GET /minio/admin/v3/bandwidth
// ----------
// Get bandwidth consumption information
func (a adminAPIHandlers) BandwidthMonitorHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "BandwidthMonitor")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.BandwidthMonitorAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	setEventStreamHeaders(w)
	reportCh := make(chan madmin.BucketBandwidthReport)
	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()
	bucketsRequestedString := r.URL.Query().Get("buckets")
	bucketsRequested := strings.Split(bucketsRequestedString, ",")
	go func() {
		defer close(reportCh)
		for {
			select {
			case <-ctx.Done():
				return
			case reportCh <- globalNotificationSys.GetBandwidthReports(ctx, bucketsRequested...):
				time.Sleep(time.Duration(rnd.Float64() * float64(2*time.Second)))
			}
		}
	}()
	for {
		select {
		case report, ok := <-reportCh:
			if !ok {
				return
			}
			if err := json.NewEncoder(w).Encode(report); err != nil {
				writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
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

// ServerInfoHandler - GET /minio/admin/v3/info
// ----------
// Get server information
func (a adminAPIHandlers) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ServerInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.ServerInfoAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(getServerInfo(ctx, r))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

func assignPoolNumbers(servers []madmin.ServerProperties) {
	for i := range servers {
		for idx, ge := range globalEndpoints {
			for _, endpoint := range ge.Endpoints {
				if servers[i].Endpoint == endpoint.Host {
					servers[i].PoolNumber = idx + 1
				} else if host, err := xnet.ParseHost(servers[i].Endpoint); err == nil {
					if host.Name == endpoint.Hostname() {
						servers[i].PoolNumber = idx + 1
					}
				}
			}
		}
	}
}

func fetchLambdaInfo() []map[string][]madmin.TargetIDStatus {

	lambdaMap := make(map[string][]madmin.TargetIDStatus)

	for _, tgt := range globalConfigTargetList.Targets() {
		targetIDStatus := make(map[string]madmin.Status)
		active, _ := tgt.IsActive()
		targetID := tgt.ID()
		if active {
			targetIDStatus[targetID.ID] = madmin.Status{Status: string(madmin.ItemOnline)}
		} else {
			targetIDStatus[targetID.ID] = madmin.Status{Status: string(madmin.ItemOffline)}
		}
		list := lambdaMap[targetID.Name]
		list = append(list, targetIDStatus)
		lambdaMap[targetID.Name] = list
	}

	for _, tgt := range globalEnvTargetList.Targets() {
		targetIDStatus := make(map[string]madmin.Status)
		active, _ := tgt.IsActive()
		targetID := tgt.ID()
		if active {
			targetIDStatus[targetID.ID] = madmin.Status{Status: string(madmin.ItemOnline)}
		} else {
			targetIDStatus[targetID.ID] = madmin.Status{Status: string(madmin.ItemOffline)}
		}
		list := lambdaMap[targetID.Name]
		list = append(list, targetIDStatus)
		lambdaMap[targetID.Name] = list
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

// fetchKMSStatus fetches KMS-related status information.
func fetchKMSStatus() madmin.KMS {
	kmsStat := madmin.KMS{}
	if GlobalKMS == nil {
		kmsStat.Status = "disabled"
		return kmsStat
	}

	stat, err := GlobalKMS.Stat()
	if err != nil {
		kmsStat.Status = string(madmin.ItemOffline)
		return kmsStat
	}
	if len(stat.Endpoints) == 0 {
		kmsStat.Status = stat.Name
		return kmsStat
	}
	kmsStat.Status = string(madmin.ItemOnline)

	kmsContext := kms.Context{"MinIO admin API": "ServerInfoHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey("", kmsContext)
	if err != nil {
		kmsStat.Encrypt = fmt.Sprintf("Encryption failed: %v", err)
	} else {
		kmsStat.Encrypt = "success"
	}

	// 2. Verify that we can indeed decrypt the (encrypted) key
	decryptedKey, err := GlobalKMS.DecryptKey(key.KeyID, key.Ciphertext, kmsContext)
	switch {
	case err != nil:
		kmsStat.Decrypt = fmt.Sprintf("Decryption failed: %v", err)
	case subtle.ConstantTimeCompare(key.Plaintext, decryptedKey) != 1:
		kmsStat.Decrypt = "Decryption failed: decrypted key does not match generated key"
	default:
		kmsStat.Decrypt = "success"
	}
	return kmsStat
}

// fetchLoggerDetails return log info
func fetchLoggerInfo() ([]madmin.Logger, []madmin.Audit) {
	var loggerInfo []madmin.Logger
	var auditloggerInfo []madmin.Audit
	for _, target := range logger.Targets {
		if target.Endpoint() != "" {
			tgt := target.String()
			err := checkConnection(target.Endpoint(), 15*time.Second)
			if err == nil {
				mapLog := make(map[string]madmin.Status)
				mapLog[tgt] = madmin.Status{Status: string(madmin.ItemOnline)}
				loggerInfo = append(loggerInfo, mapLog)
			} else {
				mapLog := make(map[string]madmin.Status)
				mapLog[tgt] = madmin.Status{Status: string(madmin.ItemOffline)}
				loggerInfo = append(loggerInfo, mapLog)
			}
		}
	}

	for _, target := range logger.AuditTargets {
		if target.Endpoint() != "" {
			tgt := target.String()
			err := checkConnection(target.Endpoint(), 15*time.Second)
			if err == nil {
				mapAudit := make(map[string]madmin.Status)
				mapAudit[tgt] = madmin.Status{Status: string(madmin.ItemOnline)}
				auditloggerInfo = append(auditloggerInfo, mapAudit)
			} else {
				mapAudit := make(map[string]madmin.Status)
				mapAudit[tgt] = madmin.Status{Status: string(madmin.ItemOffline)}
				auditloggerInfo = append(auditloggerInfo, mapAudit)
			}
		}
	}

	return loggerInfo, auditloggerInfo
}

// checkConnection - ping an endpoint , return err in case of no connection
func checkConnection(endpointStr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(GlobalContext, timeout)
	defer cancel()

	client := &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           xhttp.NewCustomDialContext(timeout),
		ResponseHeaderTimeout: 5 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 5 * time.Second,
		TLSClientConfig:       &tls.Config{RootCAs: globalRootCAs},
		// Go net/http automatically unzip if content-type is
		// gzip disable this feature, as we are always interested
		// in raw stream.
		DisableCompression: true,
	}}
	defer client.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpointStr, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(resp.Body)
	return nil
}
