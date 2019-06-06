/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018, 2019 MinIO, Inc.
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/cpu"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/handlers"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mem"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/quick"
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
	ctx := newContext(r, w, "Version")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	jsonBytes, err := json.Marshal(adminAPIVersionInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, jsonBytes)
}

// ServiceStatusHandler - GET /minio/admin/v1/service
// ----------
// Returns server version and uptime.
func (a adminAPIHandlers) ServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ServiceStatus")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Fetch server version
	serverVersion := madmin.ServerVersion{
		Version:  Version,
		CommitID: CommitID,
	}

	// Fetch uptimes from all peers and pick the latest.
	uptime := getPeerUptimes(globalNotificationSys.ServerInfo(ctx))

	// Create API response
	serverStatus := madmin.ServiceStatus{
		ServerVersion: serverVersion,
		Uptime:        uptime,
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(serverStatus)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
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
	ctx := newContext(r, w, "ServiceStopNRestart")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	var sa madmin.ServiceAction
	err := json.NewDecoder(r.Body).Decode(&sa)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrRequestBodyParse), r.URL)
		return
	}

	var serviceSig serviceSignal
	switch sa.Action {
	case madmin.ServiceActionValueRestart:
		serviceSig = serviceRestart
	case madmin.ServiceActionValueStop:
		serviceSig = serviceStop
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		logger.LogIf(ctx, errors.New("Invalid service action received"))
		return
	}

	// Reply to the client before restarting minio server.
	writeSuccessResponseHeadersOnly(w)

	// Notify all other MinIO peers signal service.
	for _, nerr := range globalNotificationSys.SignalService(serviceSig) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	globalServiceSignalCh <- serviceSig
}

// ServerProperties holds some server information such as, version, region
// uptime, etc..
type ServerProperties struct {
	Uptime       time.Duration `json:"uptime"`
	Version      string        `json:"version"`
	CommitID     string        `json:"commitID"`
	DeploymentID string        `json:"deploymentID"`
	Region       string        `json:"region"`
	SQSARN       []string      `json:"sqsARN"`
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
	ctx := newContext(r, w, "ServerInfo")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	serverInfo := globalNotificationSys.ServerInfo(ctx)
	// Once we have received all the ServerInfo from peers
	// add the local peer server info as well.
	serverInfo = append(serverInfo, ServerInfo{
		Addr: thisAddr.String(),
		Data: &ServerInfoData{
			StorageInfo: objectAPI.StorageInfo(ctx),
			ConnStats:   globalConnStats.toServerConnStats(),
			HTTPStats:   globalHTTPStats.toServerHTTPStats(),
			Properties: ServerProperties{
				Uptime:       UTCNow().Sub(globalBootTime),
				Version:      Version,
				CommitID:     CommitID,
				DeploymentID: globalDeploymentID,
				SQSARN:       globalNotificationSys.GetARNList(),
				Region:       globalServerConfig.GetRegion(),
			},
		},
	})

	// Marshal API response
	jsonBytes, err := json.Marshal(serverInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

// ServerDrivesPerfInfo holds information about address, performance
// of all drives on one server. It also reports any errors if encountered
// while trying to reach this server.
type ServerDrivesPerfInfo struct {
	Addr  string             `json:"addr"`
	Error string             `json:"error,omitempty"`
	Perf  []disk.Performance `json:"perf"`
}

// ServerCPULoadInfo holds informantion about cpu utilization
// of one minio node. It also reports any errors if encountered
// while trying to reach this server.
type ServerCPULoadInfo struct {
	Addr         string     `json:"addr"`
	Error        string     `json:"error,omitempty"`
	Load         []cpu.Load `json:"load"`
	HistoricLoad []cpu.Load `json:"historicLoad"`
}

// ServerMemUsageInfo holds informantion about memory utilization
// of one minio node. It also reports any errors if encountered
// while trying to reach this server.
type ServerMemUsageInfo struct {
	Addr          string      `json:"addr"`
	Error         string      `json:"error,omitempty"`
	Usage         []mem.Usage `json:"usage"`
	HistoricUsage []mem.Usage `json:"historicUsage"`
}

// PerfInfoHandler - GET /minio/admin/v1/performance?perfType={perfType}
// ----------
// Get all performance information based on input type
// Supported types = drive
func (a adminAPIHandlers) PerfInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PerfInfo")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	switch perfType := vars["perfType"]; perfType {
	case "drive":
		info := objectAPI.StorageInfo(ctx)
		if !(info.Backend.Type == BackendFS || info.Backend.Type == BackendErasure) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
			return
		}
		// Get drive performance details from local server's drive(s)
		dp := localEndpointsDrivePerf(globalEndpoints)

		// Notify all other MinIO peers to report drive performance numbers
		dps := globalNotificationSys.DrivePerfInfo()
		dps = append(dps, dp)

		// Marshal API response
		jsonBytes, err := json.Marshal(dps)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		// Reply with performance information (across nodes in a
		// distributed setup) as json.
		writeSuccessResponseJSON(w, jsonBytes)
	case "cpu":
		// Get CPU load details from local server's cpu(s)
		cpu := localEndpointsCPULoad(globalEndpoints)
		// Notify all other MinIO peers to report cpu load numbers
		cpus := globalNotificationSys.CPULoadInfo()
		cpus = append(cpus, cpu)

		// Marshal API response
		jsonBytes, err := json.Marshal(cpus)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		// Reply with cpu load information (across nodes in a
		// distributed setup) as json.
		writeSuccessResponseJSON(w, jsonBytes)
	case "mem":
		// Get mem usage details from local server(s)
		m := localEndpointsMemUsage(globalEndpoints)
		// Notify all other MinIO peers to report mem usage numbers
		mems := globalNotificationSys.MemUsageInfo()
		mems = append(mems, m)

		// Marshal API response
		jsonBytes, err := json.Marshal(mems)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		// Reply with mem usage information (across nodes in a
		// distributed setup) as json.
		writeSuccessResponseJSON(w, jsonBytes)
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
	}
}

func newLockEntry(l lockRequesterInfo, resource, server string) *madmin.LockEntry {
	entry := &madmin.LockEntry{Timestamp: l.Timestamp, Resource: resource, ServerList: []string{server}, Owner: l.Node, Source: l.Source, ID: l.UID}
	if l.Writer {
		entry.Type = "Write"
	} else {
		entry.Type = "Read"
	}
	return entry
}

func topLockEntries(peerLocks []*PeerLocks) madmin.LockEntries {
	const listCount int = 10
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
					entryMap[lockReqInfo.UID] = newLockEntry(lockReqInfo, k, peerLock.Addr)
				}
			}
		}
	}
	var lockEntries = make(madmin.LockEntries, 0)
	for _, v := range entryMap {
		lockEntries = append(lockEntries, *v)
	}
	sort.Sort(lockEntries)
	if len(lockEntries) > listCount {
		lockEntries = lockEntries[:listCount]
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

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Method only allowed in Distributed XL mode.
	if !globalIsDistXL {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	peerLocks := globalNotificationSys.GetLocks(ctx)
	// Once we have received all the locks currently used from peers
	// add the local peer locks list as well.
	localLocks := globalLockServer.ll.DupLockMap()
	peerLocks = append(peerLocks, &PeerLocks{
		Addr:  thisAddr.String(),
		Locks: localLocks,
	})

	topLocks := topLockEntries(peerLocks)

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

// StartProfilingHandler - POST /minio/admin/v1/profiling/start?profilerType={profilerType}
// ----------
// Enable server profiling
func (a adminAPIHandlers) StartProfilingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StartProfiling")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	profiler := vars["profilerType"]

	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Start profiling on remote servers.
	hostErrs := globalNotificationSys.StartProfiling(profiler)

	// Start profiling locally as well.
	{
		if globalProfiler != nil {
			globalProfiler.Stop()
		}
		prof, err := startProfiler(profiler, "")
		if err != nil {
			hostErrs = append(hostErrs, NotificationPeerErr{
				Host: *thisAddr,
				Err:  err,
			})
		} else {
			globalProfiler = prof
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

	writeSuccessResponseJSON(w, []byte(startProfilingResultInBytes))
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

// DownloadProfilingHandler - POST /minio/admin/v1/profiling/download
// ----------
// Download profiling information of all nodes in a zip format
func (a adminAPIHandlers) DownloadProfilingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DownloadProfiling")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	if !globalNotificationSys.DownloadProfilingData(ctx, w) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminProfilerNotEnabled), r.URL)
		return
	}
}

// extractHealInitParams - Validates params for heal init API.
func extractHealInitParams(r *http.Request) (bucket, objPrefix string,
	hs madmin.HealOpts, clientToken string, forceStart bool, forceStop bool,
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
	} else if isReservedOrInvalidBucket(bucket, false) {
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
	if _, ok := qParms[string(mgmtForceStop)]; ok {
		forceStop = true
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
	ctx := newContext(r, w, "Heal")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Check if this setup has an erasure coded backend.
	if !globalIsXL {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrHealNotImplemented), r.URL)
		return
	}

	bucket, objPrefix, hs, clientToken, forceStart, forceStop, errCode := extractHealInitParams(r)
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
							r.URL.Path, w.Header().Get(responseRequestIDKey),
							w.Header().Get(responseDeploymentIDKey)))
					} else {
						errorRespJSON = encodeResponseJSON(APIErrorResponse{
							Code:      hr.apiErr.Code,
							Message:   hr.errBody,
							Resource:  r.URL.Path,
							RequestID: w.Header().Get(responseRequestIDKey),
							HostID:    w.Header().Get(responseDeploymentIDKey),
						})
					}
					if !started {
						setCommonHeaders(w)
						w.Header().Set("Content-Type", string(mimeJSON))
						w.WriteHeader(hr.apiErr.HTTPStatusCode)
					}
					w.Write(errorRespJSON)
					w.(http.Flusher).Flush()
				}
				break forLoop
			}
		}
	}

	// find number of disks in the setup
	info := objectAPI.StorageInfo(ctx)
	numDisks := info.Backend.OfflineDisks + info.Backend.OnlineDisks

	healPath := pathJoin(bucket, objPrefix)
	if clientToken == "" && !forceStart && !forceStop {
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

	if clientToken != "" && !forceStart && !forceStop {
		// Since clientToken is given, fetch heal status from running
		// heal sequence.
		respBytes, errCode := globalAllHealState.PopHealStatusJSON(
			healPath, clientToken)
		if errCode != ErrNone {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		} else {
			writeSuccessResponseJSON(w, respBytes)
		}
		return
	}

	respCh := make(chan healResp)
	switch {
	case forceStop:
		go func() {
			respBytes, apiErr := globalAllHealState.stopHealSequence(healPath)
			hr := healResp{respBytes: respBytes, apiErr: apiErr}
			respCh <- hr
		}()
	case clientToken == "":
		nh := newHealSequence(bucket, objPrefix, handlers.GetSourceIP(r), numDisks, hs, forceStart)
		go func() {
			respBytes, apiErr, errMsg := globalAllHealState.LaunchNewHealSequence(nh)
			hr := healResp{respBytes, apiErr, errMsg}
			respCh <- hr
		}()
	}

	// Due to the force-starting functionality, the Launch
	// call above can take a long time - to keep the
	// connection alive, we start sending whitespace
	keepConnLive(w, respCh)
}

// GetConfigHandler - GET /minio/admin/v1/config
// Get config.json of this minio setup.
func (a adminAPIHandlers) GetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfigHandler")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	config, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configData, err := json.MarshalIndent(config, "", "\t")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	password := config.GetCredential().SecretKey
	econfigData, err := madmin.EncryptData(password, configData)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// Disable tidwall json array notation in JSON key path so
// users can set json with a key as a number.
// In tidwall json, notify.webhook.0 = val means { "notify" : { "webhook" : [val] }}
// In MinIO, notify.webhook.0 = val means { "notify" : { "webhook" : {"0" : val}}}
func normalizeJSONKey(input string) (key string) {
	subKeys := strings.Split(input, ".")
	for i, k := range subKeys {
		if i > 0 {
			key += "."
		}
		if _, err := strconv.Atoi(k); err == nil {
			key += ":" + k
		} else {
			key += k
		}
	}
	return
}

func validateAdminReq(ctx context.Context, w http.ResponseWriter, r *http.Request) ObjectLayer {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil || globalIAMSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil
	}

	// Validate request signature.
	adminAPIErr := checkAdminRequestAuthType(ctx, r, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return nil
	}

	return objectAPI
}

// GetConfigHandler - GET /minio/admin/v1/config-keys
// Get some keys in config.json of this minio setup.
func (a adminAPIHandlers) GetConfigKeysHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetConfigKeysHandler")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	var keys []string
	queries := r.URL.Query()

	for k := range queries {
		keys = append(keys, k)
	}

	config, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configData, err := json.Marshal(config)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configStr := string(configData)
	newConfigStr := `{}`

	for _, key := range keys {
		// sjson.Set does not return an error if key is empty
		// we should check by ourselves here
		if key == "" {
			continue
		}
		val := gjson.Get(configStr, key)
		if j, ierr := sjson.Set(newConfigStr, normalizeJSONKey(key), val.Value()); ierr == nil {
			newConfigStr = j
		}
	}

	password := config.GetCredential().SecretKey
	econfigData, err := madmin.EncryptData(password, []byte(newConfigStr))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, []byte(econfigData))
}

// toAdminAPIErrCode - converts errXLWriteQuorum error to admin API
// specific error.
func toAdminAPIErrCode(ctx context.Context, err error) APIErrorCode {
	switch err {
	case errXLWriteQuorum:
		return ErrAdminConfigNoQuorum
	default:
		return toAPIErrorCode(ctx, err)
	}
}

func toAdminAPIErr(ctx context.Context, err error) APIError {
	return errorCodes.ToAPIErr(toAdminAPIErrCode(ctx, err))
}

// RemoveUser - DELETE /minio/admin/v1/remove-user?accessKey=<access_key>
func (a adminAPIHandlers) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveUser")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	if err := globalIAMSys.DeleteUser(accessKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
	}
}

// ListUsers - GET /minio/admin/v1/list-users
func (a adminAPIHandlers) ListUsers(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListUsers")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	allCredentials, err := globalIAMSys.ListUsers()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(allCredentials)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	password := globalServerConfig.GetCredential().SecretKey
	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// SetUserStatus - PUT /minio/admin/v1/set-user-status?accessKey=<access_key>&status=[enabled|disabled]
func (a adminAPIHandlers) SetUserStatus(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetUserStatus")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	status := vars["status"]

	// Custom IAM policies not allowed for admin user.
	if accessKey == globalServerConfig.GetCredential().AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := globalIAMSys.SetUserStatus(accessKey, madmin.AccountStatus(status)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// AddUser - PUT /minio/admin/v1/add-user?accessKey=<access_key>
func (a adminAPIHandlers) AddUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddUser")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	// Custom IAM policies not allowed for admin user.
	if accessKey == globalServerConfig.GetCredential().AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := globalServerConfig.GetCredential().SecretKey
	configBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var uinfo madmin.UserInfo
	if err = json.Unmarshal(configBytes, &uinfo); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	if err = globalIAMSys.SetUser(accessKey, uinfo); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// ListCannedPolicies - GET /minio/admin/v1/list-canned-policies
func (a adminAPIHandlers) ListCannedPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListCannedPolicies")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	policies, err := globalIAMSys.ListCannedPolicies()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = json.NewEncoder(w).Encode(policies); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

// RemoveCannedPolicy - DELETE /minio/admin/v1/remove-canned-policy?name=<policy_name>
func (a adminAPIHandlers) RemoveCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveCannedPolicy")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	if err := globalIAMSys.DeleteCannedPolicy(policyName); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// AddCannedPolicy - PUT /minio/admin/v1/add-canned-policy?name=<policy_name>
func (a adminAPIHandlers) AddCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddCannedPolicy")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	// Error out if Content-Length is missing.
	if r.ContentLength <= 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// Error out if Content-Length is beyond allowed size.
	if r.ContentLength > maxBucketPolicySize {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	iamPolicy, err := iampolicy.ParseConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPolicy), r.URL)
		return
	}

	// Version in policy must not be empty
	if iamPolicy.Version == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPolicy), r.URL)
		return
	}

	if err = globalIAMSys.SetCannedPolicy(policyName, *iamPolicy); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// SetUserPolicy - PUT /minio/admin/v1/set-user-policy?accessKey=<access_key>&name=<policy_name>
func (a adminAPIHandlers) SetUserPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetUserPolicy")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	policyName := vars["name"]

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	// Custom IAM policies not allowed for admin user.
	if accessKey == globalServerConfig.GetCredential().AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := globalIAMSys.SetUserPolicy(accessKey, policyName); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
	}

	// Notify all other MinIO peers to reload users
	for _, nerr := range globalNotificationSys.LoadUsers() {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// SetConfigHandler - PUT /minio/admin/v1/config
func (a adminAPIHandlers) SetConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfigHandler")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := globalServerConfig.GetCredential().SecretKey
	configBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	// Validate JSON provided in the request body: check the
	// client has not sent JSON objects with duplicate keys.
	if err = quick.CheckDuplicateKeys(string(configBytes)); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var config serverConfig
	if err = json.Unmarshal(configBytes, &config); err != nil {
		logger.LogIf(ctx, err)
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	// If credentials for the server are provided via environment,
	// then credentials in the provided configuration must match.
	if globalIsEnvCreds {
		if !globalServerConfig.GetCredential().Equal(config.Credential) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminCredentialsMismatch), r.URL)
			return
		}
	}

	if err = config.Validate(); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = config.TestNotificationTargets(); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = saveServerConfig(ctx, objectAPI, &config); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply to the client before restarting minio server.
	writeSuccessResponseHeadersOnly(w)
}

func convertValueType(elem []byte, jsonType gjson.Type) (interface{}, error) {
	str := string(elem)
	switch jsonType {
	case gjson.False, gjson.True:
		return strconv.ParseBool(str)
	case gjson.JSON:
		return gjson.Parse(str).Value(), nil
	case gjson.String:
		return str, nil
	case gjson.Number:
		return strconv.ParseFloat(str, 64)
	default:
		return nil, nil
	}
}

// SetConfigKeysHandler - PUT /minio/admin/v1/config-keys
func (a adminAPIHandlers) SetConfigKeysHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetConfigKeysHandler")

	objectAPI := validateAdminReq(ctx, w, r)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	// Load config
	configStruct, err := readServerConfig(ctx, objectAPI)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Convert config to json bytes
	configBytes, err := json.Marshal(configStruct)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configStr := string(configBytes)

	queries := r.URL.Query()
	password := globalServerConfig.GetCredential().SecretKey

	// Set key values in the JSON config
	for k := range queries {
		// Decode encrypted data associated to the current key
		encryptedElem, dErr := base64.StdEncoding.DecodeString(queries.Get(k))
		if dErr != nil {
			reqInfo := (&logger.ReqInfo{}).AppendTags("key", k)
			ctx = logger.SetReqInfo(ctx, reqInfo)
			logger.LogIf(ctx, dErr)
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), dErr.Error(), r.URL)
			return
		}

		elem, dErr := madmin.DecryptData(password, bytes.NewBuffer([]byte(encryptedElem)))
		if dErr != nil {
			logger.LogIf(ctx, dErr)
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), dErr.Error(), r.URL)
			return
		}

		// Calculate the type of the current key from the
		// original config json
		jsonFieldType := gjson.Get(configStr, k).Type
		// Convert passed value to json filed type
		val, cErr := convertValueType(elem, jsonFieldType)
		if cErr != nil {
			writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), cErr.Error(), r.URL)
			return
		}
		// Set the key/value in the new json document
		if s, sErr := sjson.Set(configStr, normalizeJSONKey(k), val); sErr == nil {
			configStr = s
		}
	}

	configBytes = []byte(configStr)

	// Validate config
	var config serverConfig
	if err = json.Unmarshal(configBytes, &config); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = config.Validate(); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	if err = config.TestNotificationTargets(); err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), err.Error(), r.URL)
		return
	}

	// If credentials for the server are provided via environment,
	// then credentials in the provided configuration must match.
	if globalIsEnvCreds {
		if !globalServerConfig.GetCredential().Equal(config.Credential) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminCredentialsMismatch), r.URL)
			return
		}
	}

	if err = saveServerConfig(ctx, objectAPI, &config); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Send success response
	writeSuccessResponseHeadersOnly(w)
}
