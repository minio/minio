// Copyright (c) 2015-2022 MinIO, Inc.
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
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/message/log"
	"github.com/minio/minio/internal/pubsub"
	iampolicy "github.com/minio/pkg/iam/policy"
	xnet "github.com/minio/pkg/net"
	"github.com/secure-io/sio-go"
)

const (
	maxEConfigJSONSize        = 262272
	kubernetesVersionEndpoint = "https://kubernetes.default.svc/version"
)

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket      = "bucket"
	mgmtPrefix      = "prefix"
	mgmtClientToken = "clientToken"
	mgmtForceStart  = "forceStart"
	mgmtForceStop   = "forceStop"
)

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

	for _, nerr := range globalNotificationSys.DownloadBinary(ctx, u, sha256Sum, releaseInfo) {
		if nerr.Err != nil {
			err := AdminError{
				Code:       AdminUpdateApplyFailure,
				Message:    nerr.Err.Error(),
				StatusCode: http.StatusInternalServerError,
			}
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, fmt.Errorf("server update failed with %w", err))
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	err = downloadBinary(u, sha256Sum, releaseInfo, mode)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("server update failed with %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	for _, nerr := range globalNotificationSys.CommitBinary(ctx) {
		if nerr.Err != nil {
			err := AdminError{
				Code:       AdminUpdateApplyFailure,
				Message:    nerr.Err.Error(),
				StatusCode: http.StatusInternalServerError,
			}
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, fmt.Errorf("server update failed with %w", err))
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	err = commitBinary()
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("server update failed with %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	updateStatus := madmin.ServerUpdateStatus{
		CurrentVersion: Version,
		UpdatedVersion: lrTime.Format(minioReleaseTagTimeLayout),
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
// Supports following actions:
// - restart (restarts all the MinIO instances in a setup)
// - stop (stops all the MinIO instances in a setup)
// - freeze (freezes all incoming S3 API calls)
// - unfreeze (unfreezes previously frozen S3 API calls)
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
	case madmin.ServiceActionFreeze:
		serviceSig = serviceFreeze
	case madmin.ServiceActionUnfreeze:
		serviceSig = serviceUnFreeze
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
	case serviceFreeze, serviceUnFreeze:
		objectAPI, _ = validateAdminReq(ctx, w, r, iampolicy.ServiceFreezeAdminAction)
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

	switch serviceSig {
	case serviceFreeze:
		freezeServices()
	case serviceUnFreeze:
		unfreezeServices()
	case serviceRestart, serviceStop:
		globalServiceSignalCh <- serviceSig
	}
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
	AdminInputBytes  uint64 `json:"transferredAdmin"`
	AdminOutputBytes uint64 `json:"receivedAdmin"`
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
	S3RequestsIncoming     uint64             `json:"s3RequestsIncoming"`
	CurrentS3Requests      ServerHTTPAPIStats `json:"currentS3Requests"`
	TotalS3Requests        ServerHTTPAPIStats `json:"totalS3Requests"`
	TotalS3Errors          ServerHTTPAPIStats `json:"totalS3Errors"`
	TotalS35xxErrors       ServerHTTPAPIStats `json:"totalS35xxErrors"`
	TotalS34xxErrors       ServerHTTPAPIStats `json:"totalS34xxErrors"`
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

// MetricsHandler - GET /minio/admin/v3/metrics
// ----------
// Get realtime server metrics
func (a adminAPIHandlers) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "Metrics")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerInfoAdminAction)
	if objectAPI == nil {
		return
	}
	const defaultMetricsInterval = time.Second

	interval, err := time.ParseDuration(r.Form.Get("interval"))
	if err != nil || interval < time.Second {
		interval = defaultMetricsInterval
	}
	n, err := strconv.Atoi(r.Form.Get("n"))
	if err != nil || n <= 0 {
		n = math.MaxInt32
	}
	var types madmin.MetricType
	if t, _ := strconv.ParseUint(r.Form.Get("types"), 10, 64); t != 0 {
		types = madmin.MetricType(t)
	} else {
		types = madmin.MetricsAll
	}
	hosts := strings.Split(r.Form.Get("hosts"), ",")
	byhost := strings.EqualFold(r.Form.Get("by-host"), "true")
	var hostMap map[string]struct{}
	if len(hosts) > 0 && hosts[0] != "" {
		hostMap = make(map[string]struct{}, len(hosts))
		for _, k := range hosts {
			if k != "" {
				hostMap[k] = struct{}{}
			}
		}
	}
	done := ctx.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	w.Header().Set(xhttp.ContentType, string(mimeJSON))

	for n > 0 {
		var m madmin.RealtimeMetrics
		mLocal := collectLocalMetrics(types, hostMap)
		m.Merge(&mLocal)

		// Allow half the interval for collecting remote...
		cctx, cancel := context.WithTimeout(ctx, interval/2)
		mRemote := collectRemoteMetrics(cctx, types, hostMap)
		cancel()
		m.Merge(&mRemote)
		if !byhost {
			m.ByHost = nil
		}

		m.Final = n <= 1
		// Marshal API response
		jsonBytes, err := json.Marshal(m)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		_, err = w.Write(jsonBytes)
		if err != nil {
			n = 0
		}

		n--
		if n <= 0 {
			break
		}

		// Flush before waiting for next...
		w.(http.Flusher).Flush()

		select {
		case <-ticker.C:
		case <-done:
			return
		}
	}
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
				if val, ok := entryMap[lockReqInfo.Name]; ok {
					val.ServerList = append(val.ServerList, peerLock.Addr)
				} else {
					entryMap[lockReqInfo.Name] = lriToLockEntry(lockReqInfo, k, peerLock.Addr)
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
	if countStr := r.Form.Get("count"); countStr != "" {
		var err error
		count, err = strconv.Atoi(countStr)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	stale := r.Form.Get("stale") == "true" // list also stale locks

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
// profiling action in a given server - deprecated API
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

// ProfileHandler - POST /minio/admin/v3/profile/?profilerType={profilerType}
// ----------
// Enable server profiling
func (a adminAPIHandlers) ProfileHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "Profile")

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
	profileStr := r.Form.Get("profilerType")
	profiles := strings.Split(profileStr, ",")
	duration := time.Minute
	if dstr := r.Form.Get("duration"); dstr != "" {
		var err error
		duration, err = time.ParseDuration(dstr)
		if err != nil {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
			return
		}
	}
	// read request body
	io.CopyN(ioutil.Discard, r.Body, 1)

	globalProfilerMu.Lock()

	if globalProfiler == nil {
		globalProfiler = make(map[string]minioProfiler, 10)
	}

	// Stop profiler of all types if already running
	for k, v := range globalProfiler {
		v.Stop()
		delete(globalProfiler, k)
	}

	// Start profiling on remote servers.
	for _, profiler := range profiles {
		globalNotificationSys.StartProfiling(profiler)

		// Start profiling locally as well.
		prof, err := startProfiler(profiler)
		if err == nil {
			globalProfiler[profiler] = prof
		}
	}
	globalProfilerMu.Unlock()

	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			for k, v := range globalProfiler {
				v.Stop()
				delete(globalProfiler, k)
			}
			return
		case <-timer.C:
			if !globalNotificationSys.DownloadProfilingData(ctx, w) {
				writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminProfilerNotEnabled), r.URL)
				return
			}
			return
		}
	}
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
// Download profiling information of all nodes in a zip format - deprecated API
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

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrHealNotImplemented), r.URL)
		return
	}

	hip, errCode := extractHealInitParams(mux.Vars(r), r.Form, r.Body)
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
				if _, err := w.Write([]byte(" ")); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			case hr := <-respCh:
				switch hr.apiErr {
				case noError:
					if started {
						if _, err := w.Write(hr.respBytes); err != nil {
							return
						}
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
					if _, err := w.Write(errorRespJSON); err != nil {
						return
					}
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
	if globalIsGateway {
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
}

// NetperfHandler - perform mesh style network throughput test
func (a adminAPIHandlers) NetperfHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NetperfHandler")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	if !globalIsDistErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	nsLock := objectAPI.NewNSLock(minioMetaBucket, "netperf")
	lkctx, err := nsLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(toAPIErrorCode(ctx, err)), r.URL)
		return
	}
	defer nsLock.Unlock(lkctx.Cancel)

	durationStr := r.Form.Get(peerRESTDuration)
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = globalNetPerfMinDuration
	}

	if duration < globalNetPerfMinDuration {
		// We need sample size of minimum 10 secs.
		duration = globalNetPerfMinDuration
	}

	duration = duration.Round(time.Second)

	results := globalNotificationSys.Netperf(ctx, duration)
	enc := json.NewEncoder(w)
	if err := enc.Encode(madmin.NetperfResult{NodeResults: results}); err != nil {
		return
	}
}

// SpeedtestHandler - Deprecated. See ObjectSpeedTestHandler
func (a adminAPIHandlers) SpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	a.ObjectSpeedTestHandler(w, r)
}

// ObjectSpeedTestHandler - reports maximum speed of a cluster by performing PUT and
// GET operations on the server, supports auto tuning by default by automatically
// increasing concurrency and stopping when we have reached the limits on the
// system.
func (a adminAPIHandlers) ObjectSpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ObjectSpeedTestHandler")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	sizeStr := r.Form.Get(peerRESTSize)
	durationStr := r.Form.Get(peerRESTDuration)
	concurrentStr := r.Form.Get(peerRESTConcurrent)
	storageClass := strings.TrimSpace(r.Form.Get(peerRESTStorageClass))
	customBucket := strings.TrimSpace(r.Form.Get(peerRESTBucket))
	autotune := r.Form.Get("autotune") == "true"

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		size = 64 * humanize.MiByte
	}

	concurrent, err := strconv.Atoi(concurrentStr)
	if err != nil {
		concurrent = 32
	}

	if runtime.GOMAXPROCS(0) < concurrent {
		concurrent = runtime.GOMAXPROCS(0)
	}

	// if we have less drives than concurrency then choose
	// only the concurrency to be number of drives to start
	// with - since default '32' might be big and may not
	// complete in total time of 10s.
	if globalEndpoints.NEndpoints() < concurrent {
		concurrent = globalEndpoints.NEndpoints()
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = time.Second * 10
	}

	storageInfo, _ := objectAPI.StorageInfo(ctx)

	sufficientCapacity, canAutotune, capacityErrMsg := validateObjPerfOptions(ctx, storageInfo, concurrent, size, autotune)
	if !sufficientCapacity {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, AdminError{
			Code:       "XMinioSpeedtestInsufficientCapacity",
			Message:    capacityErrMsg,
			StatusCode: http.StatusInsufficientStorage,
		}), r.URL)
		return
	}

	if autotune && !canAutotune {
		autotune = false
	}

	if customBucket == "" {
		customBucket = globalObjectPerfBucket

		bucketExists, err := makeObjectPerfBucket(ctx, objectAPI, customBucket)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		if !bucketExists {
			defer deleteObjectPerfBucket(objectAPI)
		}
	}

	defer objectAPI.DeleteObject(ctx, customBucket, speedTest+SlashSeparator, ObjectOptions{
		DeletePrefix: true,
	})

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// unfreeze all incoming S3 API calls after speedtest.
	defer globalNotificationSys.ServiceFreeze(ctx, false)

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := json.NewEncoder(w)
	ch := objectSpeedTest(ctx, speedTestOpts{
		objectSize:       size,
		concurrencyStart: concurrent,
		duration:         duration,
		autotune:         autotune,
		storageClass:     storageClass,
		bucketName:       customBucket,
	})
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAliveTicker.C:
			// Write a blank entry to prevent client from disconnecting
			if err := enc.Encode(madmin.SpeedTestResult{}); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case result, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(result); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

func makeObjectPerfBucket(ctx context.Context, objectAPI ObjectLayer, bucketName string) (bucketExists bool, err error) {
	if err = objectAPI.MakeBucketWithLocation(ctx, bucketName, MakeBucketOptions{}); err != nil {
		if _, ok := err.(BucketExists); !ok {
			// Only BucketExists error can be ignored.
			return false, err
		}
		bucketExists = true
	}
	return bucketExists, nil
}

func deleteObjectPerfBucket(objectAPI ObjectLayer) {
	objectAPI.DeleteBucket(context.Background(), globalObjectPerfBucket, DeleteBucketOptions{
		Force:      true,
		NoRecreate: true,
		SRDeleteOp: getSRBucketDeleteOp(globalSiteReplicationSys.isEnabled()),
	})
}

func validateObjPerfOptions(ctx context.Context, storageInfo madmin.StorageInfo, concurrent int, size int, autotune bool) (bool, bool, string) {
	capacityNeeded := uint64(concurrent * size)
	capacity := GetTotalUsableCapacityFree(storageInfo.Disks, storageInfo)

	if capacity < capacityNeeded {
		return false, false, fmt.Sprintf("not enough usable space available to perform speedtest - expected %s, got %s",
			humanize.IBytes(capacityNeeded), humanize.IBytes(capacity))
	}

	// Verify if we can employ autotune without running out of capacity,
	// if we do run out of capacity, make sure to turn-off autotuning
	// in such situations.
	if autotune {
		newConcurrent := concurrent + (concurrent+1)/2
		autoTunedCapacityNeeded := uint64(newConcurrent * size)
		if capacity < autoTunedCapacityNeeded {
			// Turn-off auto-tuning if next possible concurrency would reach beyond disk capacity.
			return true, false, ""
		}
	}

	return true, autotune, ""
}

// NetSpeedtestHandler - reports maximum network throughput
func (a adminAPIHandlers) NetSpeedtestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NetSpeedtestHandler")

	writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
}

// DriveSpeedtestHandler - reports throughput of drives available in the cluster
func (a adminAPIHandlers) DriveSpeedtestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DriveSpeedtestHandler")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// unfreeze all incoming S3 API calls after speedtest.
	defer globalNotificationSys.ServiceFreeze(ctx, false)

	serial := r.Form.Get("serial") == "true"
	blockSizeStr := r.Form.Get("blocksize")
	fileSizeStr := r.Form.Get("filesize")

	blockSize, err := strconv.ParseUint(blockSizeStr, 10, 64)
	if err != nil {
		blockSize = 4 * humanize.MiByte // default value
	}

	fileSize, err := strconv.ParseUint(fileSizeStr, 10, 64)
	if err != nil {
		fileSize = 1 * humanize.GiByte // default value
	}

	opts := madmin.DriveSpeedTestOpts{
		Serial:    serial,
		BlockSize: blockSize,
		FileSize:  fileSize,
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	ch := globalNotificationSys.DriveSpeedTest(ctx, opts)

	enc := json.NewEncoder(w)
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAliveTicker.C:
			// Write a blank entry to prevent client from disconnecting
			if err := enc.Encode(madmin.DriveSpeedTestResult{}); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case result, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(result); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

// Admin API errors
const (
	AdminUpdateUnexpectedFailure = "XMinioAdminUpdateUnexpectedFailure"
	AdminUpdateURLNotReachable   = "XMinioAdminUpdateURLNotReachable"
	AdminUpdateApplyFailure      = "XMinioAdminUpdateApplyFailure"
)

// Returns true if the madmin.TraceInfo should be traced,
// false if certain conditions are not met.
// - input entry is not of the type *madmin.TraceInfo*
// - errOnly entries are to be traced, not status code 2xx, 3xx.
// - madmin.TraceInfo type is asked by opts
func shouldTrace(trcInfo madmin.TraceInfo, opts madmin.ServiceTraceOpts) (shouldTrace bool) {
	// Reject all unwanted types.
	want := opts.TraceTypes()
	if !want.Contains(trcInfo.TraceType) {
		return false
	}

	isHTTP := trcInfo.TraceType.Overlaps(madmin.TraceInternal|madmin.TraceS3) && trcInfo.HTTP != nil

	// Check latency...
	if opts.Threshold > 0 && trcInfo.Duration < opts.Threshold {
		return false
	}

	// Check internal path
	isInternal := isHTTP && HasPrefix(trcInfo.HTTP.ReqInfo.Path, minioReservedBucketPath+SlashSeparator)
	if isInternal && !opts.Internal {
		return false
	}

	// Filter non-errors.
	if isHTTP && opts.OnlyErrors && trcInfo.HTTP.RespInfo.StatusCode < http.StatusBadRequest {
		return false
	}

	return true
}

func extractTraceOptions(r *http.Request) (opts madmin.ServiceTraceOpts, err error) {
	if err := opts.ParseParams(r); err != nil {
		return opts, err
	}
	// Support deprecated 'all' query
	if r.Form.Get("all") == "true" {
		opts.S3 = true
		opts.Internal = true
		opts.Storage = true
		opts.OS = true
		// Older mc - cannot deal with more types...
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
	traceCh := make(chan pubsub.Maskable, 4000)

	peers, _ := newPeerRestClients(globalEndpoints)
	mask := pubsub.MaskFromMaskable(traceOpts.TraceTypes())

	err = globalTrace.Subscribe(mask, traceCh, ctx.Done(), func(entry pubsub.Maskable) bool {
		if e, ok := entry.(madmin.TraceInfo); ok {
			return shouldTrace(e, traceOpts)
		}
		return false
	})
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrSlowDown), r.URL)
		return
	}

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
			if len(traceCh) == 0 {
				// Flush if nothing is queued
				w.(http.Flusher).Flush()
			}
		case <-keepAliveTicker.C:
			if len(traceCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-ctx.Done():
			return
		}
	}
}

// The ConsoleLogHandler handler sends console logs to the connected HTTP client.
func (a adminAPIHandlers) ConsoleLogHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ConsoleLog")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ConsoleLogAdminAction)
	if objectAPI == nil {
		return
	}
	node := r.Form.Get("node")
	// limit buffered console entries if client requested it.
	limitStr := r.Form.Get("limit")
	limitLines, err := strconv.Atoi(limitStr)
	if err != nil {
		limitLines = 10
	}

	logKind := madmin.LogKind(strings.ToUpper(r.Form.Get("logType"))).LogMask()
	if logKind == 0 {
		logKind = madmin.LogMaskAll
	}

	// Avoid reusing tcp connection if read timeout is hit
	// This is needed to make r.Context().Done() work as
	// expected in case of read timeout
	w.Header().Set("Connection", "close")

	setEventStreamHeaders(w)

	logCh := make(chan pubsub.Maskable, 4000)

	peers, _ := newPeerRestClients(globalEndpoints)

	err = globalConsoleSys.Subscribe(logCh, ctx.Done(), node, limitLines, logKind, nil)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrSlowDown), r.URL)
		return
	}

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
				if len(logCh) == 0 {
					// Flush if nothing is queued
					w.(http.Flusher).Flush()
				}
			}
		case <-keepAliveTicker.C:
			if len(logCh) > 0 {
				continue
			}
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

	if err := GlobalKMS.CreateKey(ctx, r.Form.Get("key-id")); err != nil {
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

	stat, err := GlobalKMS.Stat(ctx)
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

	stat, err := GlobalKMS.Stat(ctx)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}

	keyID := r.Form.Get("key-id")
	if keyID == "" {
		keyID = stat.DefaultKey
	}
	response := madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := kms.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey(ctx, keyID, kmsContext)
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
		//nolint:gocritic
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
	versions := madmin.Versions{}
	usage := madmin.Usage{}

	objectAPI := newObjectLayerFn()
	if objectAPI != nil {
		mode = madmin.ItemOnline

		// Load data usage
		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
		if err == nil {
			buckets = madmin.Buckets{Count: dataUsageInfo.BucketsCount}
			objects = madmin.Objects{Count: dataUsageInfo.ObjectsTotalCount}
			versions = madmin.Versions{Count: dataUsageInfo.VersionsTotalCount}
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
		Region:       globalSite.Region,
		SQSARN:       globalNotificationSys.GetARNList(false),
		DeploymentID: globalDeploymentID,
		Buckets:      buckets,
		Objects:      objects,
		Versions:     versions,
		Usage:        usage,
		Services:     services,
		Backend:      backend,
		Servers:      servers,
	}
}

func getKubernetesInfo(dctx context.Context) madmin.KubernetesInfo {
	ctx, cancel := context.WithCancel(dctx)
	defer cancel()

	ki := madmin.KubernetesInfo{}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, kubernetesVersionEndpoint, nil)
	if err != nil {
		ki.Error = err.Error()
		return ki
	}

	client := &http.Client{
		Transport: NewGatewayHTTPTransport(),
		Timeout:   10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		ki.Error = err.Error()
		return ki
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&ki); err != nil {
		ki.Error = err.Error()
	}
	return ki
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

	query := r.Form
	healthInfo := madmin.HealthInfo{
		Version: madmin.HealthInfoVersion,
		Minio: madmin.MinioHealthInfo{
			Info: madmin.MinioInfo{
				DeploymentID: globalDeploymentID,
			},
		},
	}
	healthInfoCh := make(chan madmin.HealthInfo)

	enc := json.NewEncoder(w)

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

	deadline := 10 * time.Second // Default deadline is 10secs for health diagnostics.
	if query.Get(string(madmin.HealthDataTypePerfNet)) != "" ||
		query.Get(string(madmin.HealthDataTypePerfDrive)) != "" ||
		query.Get(string(madmin.HealthDataTypePerfObj)) != "" {
		deadline = 1 * time.Hour
	}
	if dstr := r.Form.Get("deadline"); dstr != "" {
		var err error
		deadline, err = time.ParseDuration(dstr)
		if err != nil {
			errResp(err)
			return
		}
	}

	nsLock := objectAPI.NewNSLock(minioMetaBucket, "health-check-in-progress")
	lkctx, err := nsLock.GetLock(ctx, newDynamicTimeout(deadline, deadline))
	if err != nil { // returns a locked lock
		errResp(err)
		return
	}
	defer nsLock.Unlock(lkctx.Cancel)

	healthCtx, healthCancel := context.WithTimeout(lkctx.Context(), deadline)
	defer healthCancel()

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// unfreeze all incoming S3 API calls after speedtest.
	defer globalNotificationSys.ServiceFreeze(ctx, false)

	hostAnonymizer := createHostAnonymizer()
	// anonAddr - Anonymizes hosts in given input string.
	anonAddr := func(addr string) string {
		newAddr, found := hostAnonymizer[addr]
		if found {
			return newAddr
		}

		// If we reach here, it means that the given addr doesn't contain any of the hosts.
		// Return it as is. Can happen for drive paths in non-distributed mode
		return addr
	}

	// anonymizedAddr - Updated the addr of the node info with anonymized one
	anonymizeAddr := func(info madmin.NodeInfo) {
		info.SetAddr(anonAddr(info.GetAddr()))
	}

	partialWrite := func(oinfo madmin.HealthInfo) {
		select {
		case healthInfoCh <- oinfo:
		case <-healthCtx.Done():
		}
	}

	getAndWritePlatformInfo := func() {
		if IsKubernetes() {
			healthInfo.Sys.KubernetesInfo = getKubernetesInfo(healthCtx)
			partialWrite(healthInfo)
		}
	}

	getAndWriteCPUs := func() {
		if query.Get("syscpu") == "true" {
			localCPUInfo := madmin.GetCPUs(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localCPUInfo)
			healthInfo.Sys.CPUInfo = append(healthInfo.Sys.CPUInfo, localCPUInfo)

			peerCPUInfo := globalNotificationSys.GetCPUs(healthCtx)
			for _, cpuInfo := range peerCPUInfo {
				anonymizeAddr(&cpuInfo)
				healthInfo.Sys.CPUInfo = append(healthInfo.Sys.CPUInfo, cpuInfo)
			}

			partialWrite(healthInfo)
		}
	}

	getAndWritePartitions := func() {
		if query.Get("sysdrivehw") == "true" {
			localPartitions := madmin.GetPartitions(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localPartitions)
			healthInfo.Sys.Partitions = append(healthInfo.Sys.Partitions, localPartitions)

			peerPartitions := globalNotificationSys.GetPartitions(healthCtx)
			for _, p := range peerPartitions {
				anonymizeAddr(&p)
				healthInfo.Sys.Partitions = append(healthInfo.Sys.Partitions, p)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteOSInfo := func() {
		if query.Get("sysosinfo") == "true" {
			localOSInfo := madmin.GetOSInfo(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localOSInfo)
			healthInfo.Sys.OSInfo = append(healthInfo.Sys.OSInfo, localOSInfo)

			peerOSInfos := globalNotificationSys.GetOSInfo(healthCtx)
			for _, o := range peerOSInfos {
				anonymizeAddr(&o)
				healthInfo.Sys.OSInfo = append(healthInfo.Sys.OSInfo, o)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteMemInfo := func() {
		if query.Get("sysmem") == "true" {
			localMemInfo := madmin.GetMemInfo(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localMemInfo)
			healthInfo.Sys.MemInfo = append(healthInfo.Sys.MemInfo, localMemInfo)

			peerMemInfos := globalNotificationSys.GetMemInfo(healthCtx)
			for _, m := range peerMemInfos {
				anonymizeAddr(&m)
				healthInfo.Sys.MemInfo = append(healthInfo.Sys.MemInfo, m)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteSysErrors := func() {
		if query.Get(string(madmin.HealthDataTypeSysErrors)) == "true" {
			localSysErrors := madmin.GetSysErrors(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localSysErrors)
			healthInfo.Sys.SysErrs = append(healthInfo.Sys.SysErrs, localSysErrors)
			partialWrite(healthInfo)

			peerSysErrs := globalNotificationSys.GetSysErrors(healthCtx)
			for _, se := range peerSysErrs {
				anonymizeAddr(&se)
				healthInfo.Sys.SysErrs = append(healthInfo.Sys.SysErrs, se)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteSysConfig := func() {
		if query.Get(string(madmin.HealthDataTypeSysConfig)) == "true" {
			localSysConfig := madmin.GetSysConfig(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localSysConfig)
			healthInfo.Sys.SysConfig = append(healthInfo.Sys.SysConfig, localSysConfig)
			partialWrite(healthInfo)

			peerSysConfig := globalNotificationSys.GetSysConfig(healthCtx)
			for _, sc := range peerSysConfig {
				anonymizeAddr(&sc)
				healthInfo.Sys.SysConfig = append(healthInfo.Sys.SysConfig, sc)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteSysServices := func() {
		if query.Get(string(madmin.HealthDataTypeSysServices)) == "true" {
			localSysServices := madmin.GetSysServices(healthCtx, globalLocalNodeName)
			anonymizeAddr(&localSysServices)
			healthInfo.Sys.SysServices = append(healthInfo.Sys.SysServices, localSysServices)
			partialWrite(healthInfo)

			peerSysServices := globalNotificationSys.GetSysServices(healthCtx)
			for _, ss := range peerSysServices {
				anonymizeAddr(&ss)
				healthInfo.Sys.SysServices = append(healthInfo.Sys.SysServices, ss)
			}
			partialWrite(healthInfo)
		}
	}

	anonymizeCmdLine := func(cmdLine string) string {
		if !globalIsDistErasure {
			// FS mode - single server - hard code to `server1`
			anonCmdLine := strings.ReplaceAll(cmdLine, globalLocalNodeName, "server1")
			return strings.ReplaceAll(anonCmdLine, globalMinioConsoleHost, "server1")
		}

		// Server start command regex groups:
		// 1 - minio server
		// 2 - flags e.g. `--address :9000 --certs-dir /etc/minio/certs`
		// 3 - pool args e.g. `https://node{01...16}.domain/data/disk{001...204} https://node{17...32}.domain/data/disk{001...204}`
		re := regexp.MustCompile(`^(.*minio\s+server\s+)(--[^\s]+\s+[^\s]+\s+)*(.*)`)

		// stays unchanged in the anonymized version
		cmdLineWithoutPools := re.ReplaceAllString(cmdLine, `$1$2`)

		// to be anonymized
		poolsArgs := re.ReplaceAllString(cmdLine, `$3`)
		var anonPools []string

		if !(strings.Contains(poolsArgs, "{") && strings.Contains(poolsArgs, "}")) {
			// No ellipses pattern. Anonymize host name from every pool arg
			pools := strings.Fields(poolsArgs)
			anonPools = make([]string, len(pools))
			for _, arg := range pools {
				anonPools = append(anonPools, anonAddr(arg))
			}
			return cmdLineWithoutPools + strings.Join(anonPools, " ")
		}

		// Ellipses pattern in pool args. Regex groups:
		// 1 - server prefix
		// 2 - number sequence for servers
		// 3 - server suffix
		// 4 - drive prefix (starting with /)
		// 5 - number sequence for drives
		// 6 - drive suffix
		re = regexp.MustCompile(`([^\s^{]*)({\d+...\d+})?([^\s^{^/]*)(/[^\s^{]*)({\d+...\d+})?([^\s]*)`)
		poolsMatches := re.FindAllStringSubmatch(poolsArgs, -1)

		anonPools = make([]string, len(poolsMatches))
		idxMap := map[int]string{
			1: "spfx",
			3: "ssfx",
		}
		for pi, poolsMatch := range poolsMatches {
			// Replace the server prefix/suffix with anonymized ones
			for idx, lbl := range idxMap {
				if len(poolsMatch[idx]) > 0 {
					poolsMatch[idx] = fmt.Sprintf("%s%d", lbl, crc32.ChecksumIEEE([]byte(poolsMatch[idx])))
				}
			}

			// Remove the original pools args present at index 0
			anonPools[pi] = strings.Join(poolsMatch[1:], "")
		}
		return cmdLineWithoutPools + strings.Join(anonPools, " ")
	}

	anonymizeProcInfo := func(p *madmin.ProcInfo) {
		p.CmdLine = anonymizeCmdLine(p.CmdLine)
		anonymizeAddr(p)
	}

	getAndWriteProcInfo := func() {
		if query.Get("sysprocess") == "true" {
			localProcInfo := madmin.GetProcInfo(healthCtx, globalLocalNodeName)
			anonymizeProcInfo(&localProcInfo)
			healthInfo.Sys.ProcInfo = append(healthInfo.Sys.ProcInfo, localProcInfo)
			peerProcInfos := globalNotificationSys.GetProcInfo(healthCtx)
			for _, p := range peerProcInfos {
				anonymizeProcInfo(&p)
				healthInfo.Sys.ProcInfo = append(healthInfo.Sys.ProcInfo, p)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteMinioConfig := func() {
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
	}

	getAndWriteDrivePerfInfo := func() {
		if query.Get(string(madmin.HealthDataTypePerfDrive)) == "true" {
			opts := madmin.DriveSpeedTestOpts{
				Serial:    false,
				BlockSize: 4 * humanize.MiByte,
				FileSize:  1 * humanize.GiByte,
			}

			localDPI := driveSpeedTest(ctx, opts)
			healthInfo.Perf.DrivePerf = append(healthInfo.Perf.DrivePerf, localDPI)

			perfCh := globalNotificationSys.DriveSpeedTest(ctx, opts)
			for perfInfo := range perfCh {
				healthInfo.Perf.DrivePerf = append(healthInfo.Perf.DrivePerf, perfInfo)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteObjPerfInfo := func() {
		if query.Get(string(madmin.HealthDataTypePerfObj)) == "true" {
			concurrent := 32
			if runtime.GOMAXPROCS(0) < concurrent {
				concurrent = runtime.GOMAXPROCS(0)
			}

			// if we have less drives than concurrency then choose
			// only the concurrency to be number of drives to start
			// with - since default '32' might be big and may not
			// complete in total time of 10s.
			if globalEndpoints.NEndpoints() < concurrent {
				concurrent = globalEndpoints.NEndpoints()
			}

			storageInfo, _ := objectAPI.StorageInfo(ctx)

			size := 64 * humanize.MiByte
			autotune := true

			sufficientCapacity, canAutotune, capacityErrMsg := validateObjPerfOptions(ctx, storageInfo, concurrent, size, autotune)
			if !sufficientCapacity {
				healthInfo.Perf.Error = capacityErrMsg
				partialWrite(healthInfo)
				return
			}

			if !canAutotune {
				autotune = false
			}

			bucketExists, err := makeObjectPerfBucket(ctx, objectAPI, globalObjectPerfBucket)
			if err != nil {
				healthInfo.Perf.Error = "Unable to create bucket: " + err.Error()
				partialWrite(healthInfo)
				return
			}

			if !bucketExists {
				defer deleteObjectPerfBucket(objectAPI)
			}

			opts := speedTestOpts{
				objectSize:       size,
				concurrencyStart: concurrent,
				duration:         10 * time.Second,
				autotune:         autotune,
			}

			perfCh := objectSpeedTest(ctx, opts)
			for perfInfo := range perfCh {
				healthInfo.Perf.ObjPerf = append(healthInfo.Perf.ObjPerf, perfInfo)
			}
			partialWrite(healthInfo)
		}
	}

	getAndWriteNetPerfInfo := func() {
		if query.Get(string(madmin.HealthDataTypePerfObj)) == "true" {
			if !globalIsDistErasure {
				return
			}

			netPerf := globalNotificationSys.Netperf(ctx, time.Second*10)
			for _, np := range netPerf {
				np.Endpoint = anonAddr(np.Endpoint)
				healthInfo.Perf.NetPerf = append(healthInfo.Perf.NetPerf, np)
			}

			partialWrite(healthInfo)
		}
	}

	anonymizeNetwork := func(network map[string]string) map[string]string {
		anonNetwork := map[string]string{}
		for endpoint, status := range network {
			anonEndpoint := anonAddr(endpoint)
			anonNetwork[anonEndpoint] = status
		}
		return anonNetwork
	}

	anonymizeDrives := func(drives []madmin.Disk) []madmin.Disk {
		anonDrives := []madmin.Disk{}
		for _, drive := range drives {
			drive.Endpoint = anonAddr(drive.Endpoint)
			anonDrives = append(anonDrives, drive)
		}
		return anonDrives
	}

	go func() {
		defer close(healthInfoCh)

		partialWrite(healthInfo) // Write first message with only version and deployment id populated
		getAndWritePlatformInfo()
		getAndWriteCPUs()
		getAndWritePartitions()
		getAndWriteOSInfo()
		getAndWriteMemInfo()
		getAndWriteProcInfo()
		getAndWriteMinioConfig()
		getAndWriteDrivePerfInfo()
		getAndWriteObjPerfInfo()
		getAndWriteNetPerfInfo()
		getAndWriteSysErrors()
		getAndWriteSysServices()
		getAndWriteSysConfig()

		if query.Get("minioinfo") == "true" {
			infoMessage := getServerInfo(ctx, r)
			servers := []madmin.ServerInfo{}
			for _, server := range infoMessage.Servers {
				anonEndpoint := anonAddr(server.Endpoint)
				servers = append(servers, madmin.ServerInfo{
					State:      server.State,
					Endpoint:   anonEndpoint,
					Uptime:     server.Uptime,
					Version:    server.Version,
					CommitID:   server.CommitID,
					Network:    anonymizeNetwork(server.Network),
					Drives:     anonymizeDrives(server.Disks),
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

			tls := getTLSInfo()
			isK8s := IsKubernetes()
			isDocker := IsDocker()
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
				TLS:          &tls,
				IsKubernetes: &isK8s,
				IsDocker:     &isDocker,
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
			if err := enc.Encode(oinfo); err != nil {
				return
			}
			if len(healthInfoCh) == 0 {
				// Flush if nothing is queued
				w.(http.Flusher).Flush()
			}
		case <-ticker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-healthCtx.Done():
			return
		}
	}
}

func getTLSInfo() madmin.TLSInfo {
	tlsInfo := madmin.TLSInfo{
		TLSEnabled: globalIsTLS,
		Certs:      []madmin.TLSCert{},
	}

	if globalIsTLS {
		for _, c := range globalPublicCerts {
			tlsInfo.Certs = append(tlsInfo.Certs, madmin.TLSCert{
				PubKeyAlgo:    c.PublicKeyAlgorithm.String(),
				SignatureAlgo: c.SignatureAlgorithm.String(),
				NotBefore:     c.NotBefore,
				NotAfter:      c.NotAfter,
			})
		}
	}
	return tlsInfo
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
	bucketsRequestedString := r.Form.Get("buckets")
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

	enc := json.NewEncoder(w)
	for {
		select {
		case report, ok := <-reportCh:
			if !ok {
				return
			}
			if err := enc.Encode(report); err != nil {
				return
			}
			if len(reportCh) == 0 {
				// Flush if nothing is queued
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

	stat, err := GlobalKMS.Stat(context.Background())
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
	key, err := GlobalKMS.GenerateKey(context.Background(), "", kmsContext)
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
	for _, target := range logger.SystemTargets() {
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

	for _, target := range logger.AuditTargets() {
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

	client := &http.Client{
		Transport: globalProxyTransport,
	}

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

type clusterInfo struct {
	DeploymentID string `json:"deployment_id"`
	ClusterName  string `json:"cluster_name"`
	UsedCapacity uint64 `json:"used_capacity"`
	Info         struct {
		MinioVersion    string `json:"minio_version"`
		PoolsCount      int    `json:"pools_count"`
		ServersCount    int    `json:"servers_count"`
		DrivesCount     int    `json:"drives_count"`
		BucketsCount    uint64 `json:"buckets_count"`
		ObjectsCount    uint64 `json:"objects_count"`
		TotalDriveSpace uint64 `json:"total_drive_space"`
		UsedDriveSpace  uint64 `json:"used_drive_space"`
	} `json:"info"`
}

func embedFileInZip(zipWriter *zip.Writer, name string, data []byte) error {
	// Send profiling data to zip as file
	header, zerr := zip.FileInfoHeader(dummyFileInfo{
		name:    name,
		size:    int64(len(data)),
		mode:    0o600,
		modTime: UTCNow(),
		isDir:   false,
		sys:     nil,
	})
	if zerr != nil {
		return zerr
	}
	header.Method = zip.Deflate
	zwriter, zerr := zipWriter.CreateHeader(header)
	if zerr != nil {
		return zerr
	}
	_, err := io.Copy(zwriter, bytes.NewReader(data))
	return err
}

// appendClusterMetaInfoToZip gets information of the current cluster and embedded
// it in the passed zipwriter, This is not a critical function and it is allowed
// to fail with a ten seconds timeout.
func appendClusterMetaInfoToZip(ctx context.Context, zipWriter *zip.Writer) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return
	}

	// Add a ten seconds timeout because getting profiling data
	// is critical for debugging, in contrary to getting cluster info
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resultCh := make(chan clusterInfo)
	go func() {
		ci := clusterInfo{}
		ci.Info.PoolsCount = len(globalEndpoints)
		ci.Info.ServersCount = globalEndpoints.NEndpoints()
		ci.Info.MinioVersion = Version

		si, _ := objectAPI.StorageInfo(ctx)

		ci.Info.DrivesCount = len(si.Disks)
		for _, disk := range si.Disks {
			ci.Info.TotalDriveSpace += disk.TotalSpace
			ci.Info.UsedDriveSpace += disk.UsedSpace
		}

		dataUsageInfo, _ := loadDataUsageFromBackend(ctx, objectAPI)

		ci.UsedCapacity = dataUsageInfo.ObjectsTotalSize
		ci.Info.BucketsCount = dataUsageInfo.BucketsCount
		ci.Info.ObjectsCount = dataUsageInfo.ObjectsTotalCount

		ci.DeploymentID = globalDeploymentID
		ci.ClusterName = fmt.Sprintf("%d-servers-%d-disks-%s", ci.Info.ServersCount, ci.Info.DrivesCount, ci.Info.MinioVersion)
		resultCh <- ci
	}()

	select {
	case <-ctx.Done():
		return
	case ci := <-resultCh:
		out, err := json.MarshalIndent(ci, "", "   ")
		if err != nil {
			logger.LogIf(ctx, err)
			return
		}
		err = embedFileInZip(zipWriter, "cluster.info", out)
		if err != nil {
			logger.LogIf(ctx, err)
		}
	}
}

// getRawDataer provides an interface for getting raw FS files.
type getRawDataer interface {
	GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, info StatInfo) error) error
}

// InspectDataHandler - GET /minio/admin/v3/inspect-data
// ----------
// Download file from all nodes in a zip format
func (a adminAPIHandlers) InspectDataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "InspectData")

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, iampolicy.InspectDataAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objLayer := newObjectLayerFn()
	o, ok := objLayer.(getRawDataer)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	volume := r.Form.Get("volume")
	file := r.Form.Get("file")
	if len(volume) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketName), r.URL)
		return
	}
	if len(file) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	file = strings.ReplaceAll(file, string(os.PathSeparator), "/")

	// Reject attempts to traverse parent or absolute paths.
	if strings.Contains(file, "..") || strings.Contains(volume, "..") {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	var key [32]byte
	// MUST use crypto/rand
	n, err := crand.Read(key[:])
	if err != nil || n != len(key) {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}
	stream, err := sio.AES_256_GCM.Stream(key[:])
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}
	// Zero nonce, we only use each key once, and 32 bytes is plenty.
	nonce := make([]byte, stream.NonceSize())
	encw := stream.EncryptWriter(w, nonce, nil)

	defer encw.Close()

	// Write a version for making *incompatible* changes.
	// The AdminClient will reject any version it does not know.
	w.Write([]byte{1})

	// Write key first (without encryption)
	_, err = w.Write(key[:])
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}

	// Initialize a zip writer which will provide a zipped content
	// of profiling data of all nodes
	zipWriter := zip.NewWriter(encw)
	defer zipWriter.Close()
	rawDataFn := func(r io.Reader, host, disk, filename string, si StatInfo) error {
		// Prefix host+disk
		filename = path.Join(host, disk, filename)
		if si.Dir {
			filename += "/"
			si.Size = 0
		}
		if si.Mode == 0 {
			// Not, set it to default.
			si.Mode = 0o600
		}
		if si.ModTime.IsZero() {
			// Set time to now.
			si.ModTime = time.Now()
		}
		header, zerr := zip.FileInfoHeader(dummyFileInfo{
			name:    filename,
			size:    si.Size,
			mode:    os.FileMode(si.Mode),
			modTime: si.ModTime,
			isDir:   si.Dir,
			sys:     nil,
		})
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		header.Method = zip.Deflate
		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		if _, err = io.Copy(zwriter, r); err != nil {
			logger.LogIf(ctx, err)
		}
		return nil
	}
	err = o.GetRawData(ctx, volume, file, rawDataFn)
	if !errors.Is(err, errFileNotFound) {
		logger.LogIf(ctx, err)
	}

	// save the format.json as part of inspect by default
	if volume != minioMetaBucket && file != formatConfigFile {
		err = o.GetRawData(ctx, minioMetaBucket, formatConfigFile, rawDataFn)
	}
	if !errors.Is(err, errFileNotFound) {
		logger.LogIf(ctx, err)
	}
	// save args passed to inspect command
	inspectArgs := []string{fmt.Sprintf(" Inspect path: %s%s%s\n", volume, slashSeparator, file)}
	cmdLine := []string{"Server command line args: "}
	for _, pool := range globalEndpoints {
		cmdLine = append(cmdLine, pool.CmdLine)
	}
	cmdLine = append(cmdLine, "\n")
	inspectArgs = append(inspectArgs, cmdLine...)
	inspectArgsBytes := []byte(strings.Join(inspectArgs, " "))
	if err = rawDataFn(bytes.NewReader(inspectArgsBytes), "", "", "inspect-input.txt", StatInfo{
		Size: int64(len(inspectArgsBytes)),
	}); err != nil {
		logger.LogIf(ctx, err)
	}

	appendClusterMetaInfoToZip(ctx, zipWriter)
}

func createHostAnonymizerForFSMode() map[string]string {
	hostAnonymizer := map[string]string{
		globalLocalNodeName: "server1",
	}

	apiEndpoints := getAPIEndpoints()
	for _, ep := range apiEndpoints {
		if len(ep) == 0 {
			continue
		}
		if url, err := xnet.ParseHTTPURL(ep); err == nil {
			// In FS mode the drive names don't include the host.
			// So mapping just the host should be sufficient.
			hostAnonymizer[url.Host] = "server1"
		}
	}
	return hostAnonymizer
}

// anonymizeHost - Add entries related to given endpoint in the host anonymizer map
// The health report data can contain the hostname in various forms e.g. host, host:port,
// host:port/drivepath, full url (http://host:port/drivepath)
// The anonymizer map will have mappings for all these varients for efficiently replacing
// any of these strings to the anonymized versions at the time of health report generation.
func anonymizeHost(hostAnonymizer map[string]string, endpoint Endpoint, poolNum int, srvrNum int) {
	if len(endpoint.Host) == 0 {
		return
	}

	currentURL := endpoint.String()

	// mapIfNotPresent - Maps the given key to the value only if the key is not present in the map
	mapIfNotPresent := func(m map[string]string, key string, val string) {
		_, found := m[key]
		if !found {
			m[key] = val
		}
	}

	_, found := hostAnonymizer[currentURL]
	if !found {
		// In distributed setup, anonymized addr = 'poolNum.serverNum'
		newHost := fmt.Sprintf("pool%d.server%d", poolNum, srvrNum)
		schemePfx := endpoint.Scheme + "://"

		// Hostname
		mapIfNotPresent(hostAnonymizer, endpoint.Hostname(), newHost)

		newHostPort := newHost
		if len(endpoint.Port()) > 0 {
			// Host + port
			newHostPort = newHost + ":" + endpoint.Port()
			mapIfNotPresent(hostAnonymizer, endpoint.Host, newHostPort)
			mapIfNotPresent(hostAnonymizer, schemePfx+endpoint.Host, newHostPort)
		}

		newHostPortPath := newHostPort
		if len(endpoint.Path) > 0 {
			// Host + port + path
			currentHostPortPath := endpoint.Host + endpoint.Path
			newHostPortPath = newHostPort + endpoint.Path
			mapIfNotPresent(hostAnonymizer, currentHostPortPath, newHostPortPath)
			mapIfNotPresent(hostAnonymizer, schemePfx+currentHostPortPath, newHostPortPath)
		}

		// Full url
		hostAnonymizer[currentURL] = schemePfx + newHostPortPath
	}
}

// createHostAnonymizer - Creats a map of various strings to corresponding anonymized names
func createHostAnonymizer() map[string]string {
	if !globalIsDistErasure {
		return createHostAnonymizerForFSMode()
	}

	hostAnonymizer := map[string]string{}

	for poolIdx, pool := range globalEndpoints {
		for srvrIdx, endpoint := range pool.Endpoints {
			anonymizeHost(hostAnonymizer, endpoint, poolIdx+1, srvrIdx+1)
		}
	}
	return hostAnonymizer
}
