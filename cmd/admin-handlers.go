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
	"crypto/rsa"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/madmin-go/v3/estream"
	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/minio/pkg/v3/policy"
	"github.com/secure-io/sio-go"
	"github.com/zeebo/xxh3"
)

const (
	maxEConfigJSONSize        = 262272
	kubernetesVersionEndpoint = "https://kubernetes.default.svc/version"
	anonymizeParam            = "anonymize"
	anonymizeStrict           = "strict"
)

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket      = "bucket"
	mgmtPrefix      = "prefix"
	mgmtClientToken = "clientToken"
	mgmtForceStart  = "forceStart"
	mgmtForceStop   = "forceStop"
)

// ServerUpdateV2Handler - POST /minio/admin/v3/update?updateURL={updateURL}&type=2
// ----------
// updates all minio servers and restarts them gracefully.
func (a adminAPIHandlers) ServerUpdateV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ServerUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if globalInplaceUpdateDisabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	if currentReleaseTime.IsZero() || currentReleaseTime.Equal(timeSentinel) {
		apiErr := errorCodes.ToAPIErr(ErrMethodNotAllowed)
		apiErr.Description = fmt.Sprintf("unable to perform in-place update, release time is unrecognized: %s", currentReleaseTime)
		writeErrorResponseJSON(ctx, w, apiErr, r.URL)
		return
	}

	vars := mux.Vars(r)
	updateURL := vars["updateURL"]
	dryRun := r.Form.Get("dry-run") == "true"

	mode := getMinioMode()
	if updateURL == "" {
		updateURL = minioReleaseInfoURL
		if runtime.GOOS == globalWindowsOSName {
			updateURL = minioReleaseWindowsInfoURL
		}
	}

	local := globalLocalNodeName
	if local == "" {
		local = "127.0.0.1"
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

	updateStatus := madmin.ServerUpdateStatusV2{
		DryRun:  dryRun,
		Results: make([]madmin.ServerPeerUpdateStatus, 0, len(globalNotificationSys.allPeerClients)),
	}
	peerResults := make(map[string]madmin.ServerPeerUpdateStatus, len(globalNotificationSys.allPeerClients))
	failedClients := make(map[int]bool, len(globalNotificationSys.allPeerClients))

	if lrTime.Sub(currentReleaseTime) <= 0 {
		updateStatus.Results = append(updateStatus.Results, madmin.ServerPeerUpdateStatus{
			Host:           local,
			Err:            fmt.Sprintf("server is running the latest version: %s", Version),
			CurrentVersion: Version,
		})

		for _, client := range globalNotificationSys.peerClients {
			updateStatus.Results = append(updateStatus.Results, madmin.ServerPeerUpdateStatus{
				Host:           client.String(),
				Err:            fmt.Sprintf("server is running the latest version: %s", Version),
				CurrentVersion: Version,
			})
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

	u.Path = path.Dir(u.Path) + SlashSeparator + releaseInfo
	// Download Binary Once
	binC, bin, err := downloadBinary(u, mode)
	if err != nil {
		adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if globalIsDistErasure {
		// Push binary to other servers
		for idx, nerr := range globalNotificationSys.VerifyBinary(ctx, u, sha256Sum, releaseInfo, binC) {
			if nerr.Err != nil {
				peerResults[nerr.Host.String()] = madmin.ServerPeerUpdateStatus{
					Host:           nerr.Host.String(),
					Err:            nerr.Err.Error(),
					CurrentVersion: Version,
				}
				failedClients[idx] = true
			} else {
				peerResults[nerr.Host.String()] = madmin.ServerPeerUpdateStatus{
					Host:           nerr.Host.String(),
					CurrentVersion: Version,
					UpdatedVersion: lrTime.Format(MinioReleaseTagTimeLayout),
				}
			}
		}
	}

	if err = verifyBinary(u, sha256Sum, releaseInfo, mode, bytes.NewReader(bin)); err != nil {
		peerResults[local] = madmin.ServerPeerUpdateStatus{
			Host:           local,
			Err:            err.Error(),
			CurrentVersion: Version,
		}
	} else {
		peerResults[local] = madmin.ServerPeerUpdateStatus{
			Host:           local,
			CurrentVersion: Version,
			UpdatedVersion: lrTime.Format(MinioReleaseTagTimeLayout),
		}
	}

	if !dryRun {
		if globalIsDistErasure {
			ng := WithNPeers(len(globalNotificationSys.peerClients))
			for idx, client := range globalNotificationSys.peerClients {
				if failedClients[idx] {
					continue
				}
				client := client
				ng.Go(ctx, func() error {
					return client.CommitBinary(ctx)
				}, idx, *client.host)
			}

			for _, nerr := range ng.Wait() {
				if nerr.Err != nil {
					prs, ok := peerResults[nerr.Host.String()]
					if ok {
						prs.Err = nerr.Err.Error()
						peerResults[nerr.Host.String()] = prs
					} else {
						peerResults[nerr.Host.String()] = madmin.ServerPeerUpdateStatus{
							Host:           nerr.Host.String(),
							Err:            nerr.Err.Error(),
							CurrentVersion: Version,
							UpdatedVersion: lrTime.Format(MinioReleaseTagTimeLayout),
						}
					}
				}
			}
		}
		prs := peerResults[local]
		if prs.Err == "" {
			if err = commitBinary(); err != nil {
				prs.Err = err.Error()
			}
			peerResults[local] = prs
		}
	}

	prs, ok := peerResults[local]
	if ok {
		prs.WaitingDrives = waitingDrivesNode()
		peerResults[local] = prs
	}

	if globalIsDistErasure {
		// Notify all other MinIO peers signal service.
		startTime := time.Now().Add(restartUpdateDelay)
		ng := WithNPeers(len(globalNotificationSys.peerClients))
		for idx, client := range globalNotificationSys.peerClients {
			if failedClients[idx] {
				continue
			}
			client := client
			ng.Go(ctx, func() error {
				prs, ok := peerResults[client.String()]
				// We restart only on success, not for any failures.
				if ok && prs.Err == "" {
					return client.SignalService(serviceRestart, "", dryRun, &startTime)
				}
				return nil
			}, idx, *client.host)
		}

		for _, nerr := range ng.Wait() {
			if nerr.Err != nil {
				waitingDrives := map[string]madmin.DiskMetrics{}
				jerr := json.Unmarshal([]byte(nerr.Err.Error()), &waitingDrives)
				if jerr == nil {
					prs, ok := peerResults[nerr.Host.String()]
					if ok {
						prs.WaitingDrives = waitingDrives
						peerResults[nerr.Host.String()] = prs
					}
					continue
				}
			}
		}
	}

	for _, pr := range peerResults {
		updateStatus.Results = append(updateStatus.Results, pr)
	}

	// Marshal API response
	jsonBytes, err := json.Marshal(updateStatus)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, jsonBytes)

	if !dryRun {
		prs, ok := peerResults[local]
		// We restart only on success, not for any failures.
		if ok && prs.Err == "" {
			globalServiceSignalCh <- serviceRestart
		}
	}
}

// ServerUpdateHandler - POST /minio/admin/v3/update?updateURL={updateURL}
// ----------
// updates all minio servers and restarts them gracefully.
func (a adminAPIHandlers) ServerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ServerUpdateAdminAction)
	if objectAPI == nil {
		return
	}

	if globalInplaceUpdateDisabled || currentReleaseTime.IsZero() {
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

	if lrTime.Sub(currentReleaseTime) <= 0 {
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

	u.Path = path.Dir(u.Path) + SlashSeparator + releaseInfo

	// Download Binary Once
	binC, bin, err := downloadBinary(u, mode)
	if err != nil {
		adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Push binary to other servers
	for _, nerr := range globalNotificationSys.VerifyBinary(ctx, u, sha256Sum, releaseInfo, binC) {
		if nerr.Err != nil {
			err := AdminError{
				Code:       AdminUpdateApplyFailure,
				Message:    nerr.Err.Error(),
				StatusCode: http.StatusInternalServerError,
			}
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	err = verifyBinary(u, sha256Sum, releaseInfo, mode, bytes.NewReader(bin))
	if err != nil {
		adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
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
			adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	err = commitBinary()
	if err != nil {
		adminLogIf(ctx, fmt.Errorf("server update failed with %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	updateStatus := madmin.ServerUpdateStatus{
		CurrentVersion: Version,
		UpdatedVersion: lrTime.Format(MinioReleaseTagTimeLayout),
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
			adminLogIf(ctx, nerr.Err)
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
	ctx := r.Context()

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
		adminLogIf(ctx, fmt.Errorf("Unrecognized service action %s requested", action), logger.ErrorKind)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	var objectAPI ObjectLayer
	switch serviceSig {
	case serviceRestart:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceRestartAdminAction)
	case serviceStop:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceStopAdminAction)
	case serviceFreeze, serviceUnFreeze:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceFreezeAdminAction)
	}
	if objectAPI == nil {
		return
	}

	// Notify all other MinIO peers signal service.
	for _, nerr := range globalNotificationSys.SignalService(serviceSig) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			adminLogIf(ctx, nerr.Err)
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

type servicePeerResult struct {
	Host          string                        `json:"host"`
	Err           string                        `json:"err,omitempty"`
	WaitingDrives map[string]madmin.DiskMetrics `json:"waitingDrives,omitempty"`
}

type serviceResult struct {
	Action  madmin.ServiceAction `json:"action"`
	DryRun  bool                 `json:"dryRun"`
	Results []servicePeerResult  `json:"results,omitempty"`
}

// ServiceV2Handler - POST /minio/admin/v3/service?action={action}&type=2
// ----------
// Supports following actions:
// - restart (restarts all the MinIO instances in a setup)
// - stop (stops all the MinIO instances in a setup)
// - freeze (freezes all incoming S3 API calls)
// - unfreeze (unfreezes previously frozen S3 API calls)
//
// This newer API now returns back status per remote peer and local regarding
// if a "restart/stop" was successful or not. Service signal now supports
// a dry-run that helps skip the nodes that may have hung drives. By default
// restart/stop will ignore the servers that are hung on drives. You can use
// 'force' param to force restart even with hung drives if needed.
func (a adminAPIHandlers) ServiceV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	action := vars["action"]
	dryRun := r.Form.Get("dry-run") == "true"

	var serviceSig serviceSignal
	act := madmin.ServiceAction(action)
	switch act {
	case madmin.ServiceActionRestart:
		serviceSig = serviceRestart
	case madmin.ServiceActionStop:
		serviceSig = serviceStop
	case madmin.ServiceActionFreeze:
		serviceSig = serviceFreeze
	case madmin.ServiceActionUnfreeze:
		serviceSig = serviceUnFreeze
	default:
		adminLogIf(ctx, fmt.Errorf("Unrecognized service action %s requested", action), logger.ErrorKind)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	var objectAPI ObjectLayer
	var execAt *time.Time
	switch serviceSig {
	case serviceRestart:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceRestartAdminAction)
		t := time.Now().Add(restartUpdateDelay)
		execAt = &t
	case serviceStop:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceStopAdminAction)
	case serviceFreeze, serviceUnFreeze:
		objectAPI, _ = validateAdminReq(ctx, w, r, policy.ServiceFreezeAdminAction)
	}
	if objectAPI == nil {
		return
	}

	// Notify all other MinIO peers signal service.
	srvResult := serviceResult{Action: act, Results: []servicePeerResult{}}

	process := act == madmin.ServiceActionRestart || act == madmin.ServiceActionStop
	if process {
		localhost := globalLocalNodeName
		if globalLocalNodeName == "" {
			localhost = "127.0.0.1"
		}
		waitingDrives := waitingDrivesNode()
		srvResult.Results = append(srvResult.Results, servicePeerResult{
			Host:          localhost,
			WaitingDrives: waitingDrives,
		})
	}

	if globalIsDistErasure {
		for _, nerr := range globalNotificationSys.SignalServiceV2(serviceSig, dryRun, execAt) {
			if nerr.Err != nil && process {
				waitingDrives := map[string]madmin.DiskMetrics{}
				jerr := json.Unmarshal([]byte(nerr.Err.Error()), &waitingDrives)
				if jerr == nil {
					srvResult.Results = append(srvResult.Results, servicePeerResult{
						Host:          nerr.Host.String(),
						WaitingDrives: waitingDrives,
					})
					continue
				}
			}
			errStr := ""
			if nerr.Err != nil {
				errStr = nerr.Err.Error()
			}
			srvResult.Results = append(srvResult.Results, servicePeerResult{
				Host: nerr.Host.String(),
				Err:  errStr,
			})
		}
	}

	srvResult.DryRun = dryRun

	buf, err := json.Marshal(srvResult)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply to the client before restarting, stopping MinIO server.
	writeSuccessResponseJSON(w, buf)

	switch serviceSig {
	case serviceFreeze:
		freezeServices()
	case serviceUnFreeze:
		unfreezeServices()
	case serviceRestart, serviceStop:
		if !dryRun {
			globalServiceSignalCh <- serviceSig
		}
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

// serverConnStats holds transferred bytes from/to the server
type serverConnStats struct {
	internodeInputBytes  uint64
	internodeOutputBytes uint64
	s3InputBytes         uint64
	s3OutputBytes        uint64
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.StorageInfoAdminAction)
	if objectAPI == nil {
		return
	}

	storageInfo := objectAPI.StorageInfo(ctx, true)

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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ServerInfoAdminAction)
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

	disks := strings.Split(r.Form.Get("disks"), ",")
	byDisk := strings.EqualFold(r.Form.Get("by-disk"), "true")
	var diskMap map[string]struct{}
	if len(disks) > 0 && disks[0] != "" {
		diskMap = make(map[string]struct{}, len(disks))
		for _, k := range disks {
			if k != "" {
				diskMap[k] = struct{}{}
			}
		}
	}
	jobID := r.Form.Get("by-jobID")

	hosts := strings.Split(r.Form.Get("hosts"), ",")
	byHost := strings.EqualFold(r.Form.Get("by-host"), "true")
	var hostMap map[string]struct{}
	if len(hosts) > 0 && hosts[0] != "" {
		hostMap = make(map[string]struct{}, len(hosts))
		for _, k := range hosts {
			if k != "" {
				hostMap[k] = struct{}{}
			}
		}
	}
	dID := r.Form.Get("by-depID")
	done := ctx.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	w.Header().Set(xhttp.ContentType, string(mimeJSON))

	enc := json.NewEncoder(w)
	for n > 0 {
		var m madmin.RealtimeMetrics
		mLocal := collectLocalMetrics(types, collectMetricsOpts{
			hosts: hostMap,
			disks: diskMap,
			jobID: jobID,
			depID: dID,
		})
		m.Merge(&mLocal)
		// Allow half the interval for collecting remote...
		cctx, cancel := context.WithTimeout(ctx, interval/2)
		mRemote := collectRemoteMetrics(cctx, types, collectMetricsOpts{
			hosts: hostMap,
			disks: diskMap,
			jobID: jobID,
			depID: dID,
		})
		cancel()
		m.Merge(&mRemote)
		if !byHost {
			m.ByHost = nil
		}
		if !byDisk {
			m.ByDisk = nil
		}

		m.Final = n <= 1

		// Marshal API reesponse
		if err := enc.Encode(&m); err != nil {
			n = 0
		}

		n--
		if n <= 0 {
			break
		}

		// Flush before waiting for next...
		xhttp.Flush(w)

		select {
		case <-ticker.C:
		case <-done:
			return
		}
	}
}

// DataUsageInfoHandler - GET /minio/admin/v3/datausage?capacity={true}
// ----------
// Get server/cluster data usage info
func (a adminAPIHandlers) DataUsageInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.DataUsageInfoAdminAction)
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

	// Get capacity info when asked.
	if r.Form.Get("capacity") == "true" {
		sinfo := objectAPI.StorageInfo(ctx, false)
		dataUsageInfo.TotalCapacity = GetTotalUsableCapacity(sinfo.Disks, sinfo)
		dataUsageInfo.TotalFreeCapacity = GetTotalUsableCapacityFree(sinfo.Disks, sinfo)
		if dataUsageInfo.TotalCapacity > dataUsageInfo.TotalFreeCapacity {
			dataUsageInfo.TotalUsedCapacity = dataUsageInfo.TotalCapacity - dataUsageInfo.TotalFreeCapacity
		}
	}

	writeSuccessResponseJSON(w, dataUsageInfoJSON)
}

func lriToLockEntry(l lockRequesterInfo, now time.Time, resource, server string) *madmin.LockEntry {
	t := time.Unix(0, l.Timestamp)
	entry := &madmin.LockEntry{
		Timestamp:  t,
		Elapsed:    now.Sub(t),
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
	now := time.Now().UTC()
	entryMap := make(map[string]*madmin.LockEntry)
	toEntry := func(lri lockRequesterInfo) string {
		return fmt.Sprintf("%s/%s", lri.Name, lri.UID)
	}
	for _, peerLock := range peerLocks {
		if peerLock == nil {
			continue
		}
		for k, v := range peerLock.Locks {
			for _, lockReqInfo := range v {
				if val, ok := entryMap[toEntry(lockReqInfo)]; ok {
					val.ServerList = append(val.ServerList, peerLock.Addr)
				} else {
					entryMap[toEntry(lockReqInfo)] = lriToLockEntry(lockReqInfo, now, k, peerLock.Addr)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ForceUnlockAdminAction)
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
	for path := range strings.SplitSeq(vars["paths"], ",") {
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.TopLocksAdminAction)
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
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.ProfilingAdminAction, "")
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
		hostErrs = append(hostErrs, globalNotificationSys.StartProfiling(ctx, profiler)...)

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
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.ProfilingAdminAction, "")
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
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
	}

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
		// Limit start time to max 10s.
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		globalNotificationSys.StartProfiling(ctx, profiler)
		// StartProfiling blocks, so we can cancel now.
		cancel()

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
			// Stop remote profiles
			go globalNotificationSys.DownloadProfilingData(GlobalContext, io.Discard)

			// Stop local
			globalProfilerMu.Lock()
			defer globalProfilerMu.Unlock()
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
	sys     any
}

func (f dummyFileInfo) Name() string       { return f.name }
func (f dummyFileInfo) Size() int64        { return f.size }
func (f dummyFileInfo) Mode() os.FileMode  { return f.mode }
func (f dummyFileInfo) ModTime() time.Time { return f.modTime }
func (f dummyFileInfo) IsDir() bool        { return f.isDir }
func (f dummyFileInfo) Sys() any           { return f.sys }

// DownloadProfilingHandler - POST /minio/admin/v3/profiling/download
// ----------
// Download profiling information of all nodes in a zip format - deprecated API
func (a adminAPIHandlers) DownloadProfilingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.ProfilingAdminAction, "")
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
func extractHealInitParams(vars map[string]string, qParams url.Values, r io.Reader) (hip healInitParams, err APIErrorCode) {
	hip.bucket = vars[mgmtBucket]
	hip.objPrefix = vars[mgmtPrefix]

	if hip.bucket == "" {
		if hip.objPrefix != "" {
			// Bucket is required if object-prefix is given
			err = ErrHealMissingBucket
			return hip, err
		}
	} else if isReservedOrInvalidBucket(hip.bucket, false) {
		err = ErrInvalidBucketName
		return hip, err
	}

	// empty prefix is valid.
	if !IsValidObjectPrefix(hip.objPrefix) {
		err = ErrInvalidObjectName
		return hip, err
	}

	if len(qParams[mgmtClientToken]) > 0 {
		hip.clientToken = qParams[mgmtClientToken][0]
	}
	if _, ok := qParams[mgmtForceStart]; ok {
		hip.forceStart = true
	}
	if _, ok := qParams[mgmtForceStop]; ok {
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
		return hip, err
	}

	// ignore body if clientToken is provided
	if hip.clientToken == "" {
		jerr := json.NewDecoder(r).Decode(&hip.hs)
		if jerr != nil {
			adminLogIf(GlobalContext, jerr, logger.ErrorKind)
			err = ErrRequestBodyParse
			return hip, err
		}
	}

	err = ErrNone
	return hip, err
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
		return
	}

	hip, errCode := extractHealInitParams(mux.Vars(r), r.Form, r.Body)
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	// Analyze the heal token and route the request accordingly
	token, _, success := proxyRequestByToken(ctx, w, r, hip.clientToken, false)
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
				xhttp.Flush(w)
			case hr := <-respCh:
				switch hr.apiErr {
				case noError:
					if started {
						if _, err := w.Write(hr.respBytes); err != nil {
							return
						}
						xhttp.Flush(w)
					} else {
						writeSuccessResponseJSON(w, hr.respBytes)
					}
				default:
					var errorRespJSON []byte
					if hr.errBody == "" {
						errorRespJSON = encodeResponseJSON(getAPIErrorResponse(ctx, hr.apiErr,
							r.URL.Path, w.Header().Get(xhttp.AmzRequestID),
							w.Header().Get(xhttp.AmzRequestHostID)))
					} else {
						errorRespJSON = encodeResponseJSON(APIErrorResponse{
							Code:      hr.apiErr.Code,
							Message:   hr.errBody,
							Resource:  r.URL.Path,
							RequestID: w.Header().Get(xhttp.AmzRequestID),
							HostID:    globalDeploymentID(),
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
					xhttp.Flush(w)
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
				clientToken = fmt.Sprintf("%s%s%d", nh.clientToken, getKeySeparator(), GetProxyEndpointLocalIndex(globalProxyEndpoints))
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

	respCh := make(chan healResp, 1)
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
	bgHealStates, ok := getLocalBackgroundHealStatus(ctx, o)
	if !ok {
		return bgHealStates, errServerNotInitialized
	}

	if globalIsDistErasure {
		// Get heal status from other peers
		peersHealStates, nerrs := globalNotificationSys.BackgroundHealStatus(ctx)
		var errCount int
		for _, nerr := range nerrs {
			if nerr.Err != nil {
				adminLogIf(ctx, nerr.Err)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealAdminAction)
	if objectAPI == nil {
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

// SitePerfHandler -  measures network throughput between site replicated setups
func (a adminAPIHandlers) SitePerfHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	if !globalSiteReplicationSys.isEnabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	nsLock := objectAPI.NewNSLock(minioMetaBucket, "site-net-perf")
	lkctx, err := nsLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(toAPIErrorCode(ctx, err)), r.URL)
		return
	}
	ctx = lkctx.Context()
	defer nsLock.Unlock(lkctx)

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

	results, err := globalSiteReplicationSys.Netperf(ctx, duration)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(toAPIErrorCode(ctx, err)), r.URL)
		return
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(results); err != nil {
		return
	}
}

// ClientDevNullExtraTime - return extratime for last devnull
// [POST] /minio/admin/v3/speedtest/client/devnull/extratime
func (a adminAPIHandlers) ClientDevNullExtraTime(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.BandwidthMonitorAction)
	if objectAPI == nil {
		return
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(madmin.ClientPerfExtraTime{TimeSpent: atomic.LoadInt64(&globalLastClientPerfExtraTime)}); err != nil {
		return
	}
}

// ClientDevNull - everything goes to io.Discard
// [POST] /minio/admin/v3/speedtest/client/devnull
func (a adminAPIHandlers) ClientDevNull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	timeStart := time.Now()
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.BandwidthMonitorAction)
	if objectAPI == nil {
		return
	}

	nsLock := objectAPI.NewNSLock(minioMetaBucket, "client-perf")
	lkctx, err := nsLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(toAPIErrorCode(ctx, err)), r.URL)
		return
	}
	ctx = lkctx.Context()
	defer nsLock.Unlock(lkctx)
	timeEnd := time.Now()

	atomic.SwapInt64(&globalLastClientPerfExtraTime, timeEnd.Sub(timeStart).Nanoseconds())

	ctx, cancel := context.WithTimeout(ctx, madmin.MaxClientPerfTimeout)
	defer cancel()
	totalRx := int64(0)
	connectTime := time.Now()
	for {
		n, err := io.CopyN(xioutil.Discard, r.Body, 128*humanize.KiByte)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			// would mean the network is not stable. Logging here will help in debugging network issues.
			if time.Since(connectTime) < (globalNetPerfMinDuration - time.Second) {
				adminLogIf(ctx, err)
			}
		}
		totalRx += n
		if err != nil || ctx.Err() != nil || totalRx > 100*humanize.GiByte {
			break
		}
	}
	w.WriteHeader(http.StatusOK)
}

// NetperfHandler - perform mesh style network throughput test
func (a adminAPIHandlers) NetperfHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealthInfoAdminAction)
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
	ctx = lkctx.Context()
	defer nsLock.Unlock(lkctx)

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// Unfreeze as soon as request context is canceled or when the function returns.
	go func() {
		<-ctx.Done()
		globalNotificationSys.ServiceFreeze(ctx, false)
	}()

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

func isAllowedRWAccess(r *http.Request, cred auth.Credentials, bucketName string) (rd, wr bool) {
	owner := cred.AccessKey == globalActiveCred.AccessKey

	// Set prefix value for "s3:prefix" policy conditionals.
	r.Header.Set("prefix", "")

	// Set delimiter value for "s3:delimiter" policy conditionals.
	r.Header.Set("delimiter", SlashSeparator)

	isAllowedAccess := func(bucketName string) (rd, wr bool) {
		if globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.GetObjectAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          cred.Claims,
		}) {
			rd = true
		}

		if globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.PutObjectAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          cred.Claims,
		}) {
			wr = true
		}

		return rd, wr
	}
	return isAllowedAccess(bucketName)
}

// ObjectSpeedTestHandler - reports maximum speed of a cluster by performing PUT and
// GET operations on the server, supports auto tuning by default by automatically
// increasing concurrency and stopping when we have reached the limits on the
// system.
func (a adminAPIHandlers) ObjectSpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	objectAPI, creds := validateAdminReq(ctx, w, r, policy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	if !globalAPIConfig.permitRootAccess() {
		rd, wr := isAllowedRWAccess(r, creds, globalObjectPerfBucket)
		if !rd || !wr {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, AdminError{
				Code: "XMinioSpeedtestInsufficientPermissions",
				Message: fmt.Sprintf("%s does not have read and write access to '%s' bucket", creds.AccessKey,
					globalObjectPerfBucket),
				StatusCode: http.StatusForbidden,
			}), r.URL)
			return
		}
	}

	sizeStr := r.Form.Get(peerRESTSize)
	durationStr := r.Form.Get(peerRESTDuration)
	concurrentStr := r.Form.Get(peerRESTConcurrent)
	storageClass := strings.TrimSpace(r.Form.Get(peerRESTStorageClass))
	customBucket := strings.TrimSpace(r.Form.Get(peerRESTBucket))
	autotune := r.Form.Get("autotune") == "true"
	noClear := r.Form.Get("noclear") == "true"
	enableSha256 := r.Form.Get("enableSha256") == "true"
	enableMultipart := r.Form.Get("enableMultipart") == "true"

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		size = 64 * humanize.MiByte
	}

	concurrent, err := strconv.Atoi(concurrentStr)
	if err != nil {
		concurrent = 32
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = time.Second * 10
	}

	storageInfo := objectAPI.StorageInfo(ctx, false)

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

		if !noClear && !bucketExists {
			defer deleteObjectPerfBucket(objectAPI)
		}
	}

	if !noClear {
		defer objectAPI.DeleteObject(ctx, customBucket, speedTest+SlashSeparator, ObjectOptions{
			DeletePrefix: true,
		})
	}

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// Unfreeze as soon as request context is canceled or when the function returns.
	go func() {
		<-ctx.Done()
		globalNotificationSys.ServiceFreeze(ctx, false)
	}()

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
		enableSha256:     enableSha256,
		enableMultipart:  enableMultipart,
		creds:            creds,
	})
	var prevResult madmin.SpeedTestResult
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAliveTicker.C:
			// if previous result is set keep writing the
			// previous result back to the client
			if prevResult.Version != "" {
				if err := enc.Encode(prevResult); err != nil {
					return
				}
			} else {
				// first result is not yet obtained, keep writing
				// empty entry to prevent client from disconnecting.
				if err := enc.Encode(madmin.SpeedTestResult{}); err != nil {
					return
				}
			}
			xhttp.Flush(w)
		case result, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(result); err != nil {
				return
			}
			prevResult = result
			xhttp.Flush(w)
		}
	}
}

func makeObjectPerfBucket(ctx context.Context, objectAPI ObjectLayer, bucketName string) (bucketExists bool, err error) {
	if err = objectAPI.MakeBucket(ctx, bucketName, MakeBucketOptions{VersioningEnabled: globalSiteReplicationSys.isEnabled()}); err != nil {
		if _, ok := err.(BucketExists); !ok {
			// Only BucketExists error can be ignored.
			return false, err
		}
		bucketExists = true
	}

	if globalSiteReplicationSys.isEnabled() {
		configData := []byte(`<VersioningConfiguration><Status>Enabled</Status><ExcludedPrefixes><Prefix>speedtest/*</Prefix></ExcludedPrefixes></VersioningConfiguration>`)
		if _, err = globalBucketMetadataSys.Update(ctx, bucketName, bucketVersioningConfig, configData); err != nil {
			return false, err
		}
	}

	return bucketExists, nil
}

func deleteObjectPerfBucket(objectAPI ObjectLayer) {
	objectAPI.DeleteBucket(context.Background(), globalObjectPerfBucket, DeleteBucketOptions{
		Force:      true,
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

// DriveSpeedtestHandler - reports throughput of drives available in the cluster
func (a adminAPIHandlers) DriveSpeedtestHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	// Freeze all incoming S3 API calls before running speedtest.
	globalNotificationSys.ServiceFreeze(ctx, true)

	// Unfreeze as soon as request context is canceled or when the function returns.
	go func() {
		<-ctx.Done()
		globalNotificationSys.ServiceFreeze(ctx, false)
	}()

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
			xhttp.Flush(w)
		case result, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(result); err != nil {
				return
			}
			xhttp.Flush(w)
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
	return opts, err
}

// TraceHandler - POST /minio/admin/v3/trace
// ----------
// The handler sends http trace to the connected HTTP client.
func (a adminAPIHandlers) TraceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.TraceAdminAction, "")
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
	// Keep 100k buffered channel.
	// If receiver cannot keep up with that we drop events.
	traceCh := make(chan []byte, 100000)
	peers, _ := newPeerRestClients(globalEndpoints)
	err = globalTrace.SubscribeJSON(traceOpts.TraceTypes(), traceCh, ctx.Done(), func(entry madmin.TraceInfo) bool {
		return shouldTrace(entry, traceOpts)
	}, nil)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Publish bootstrap events that have already occurred before client could subscribe.
	if traceOpts.TraceTypes().Contains(madmin.TraceBootstrap) {
		go globalBootstrapTracer.Publish(ctx, globalTrace)
	}

	for _, peer := range peers {
		if peer == nil {
			continue
		}
		peer.Trace(ctx, traceCh, traceOpts)
	}

	keepAliveTicker := time.NewTicker(time.Second)
	defer keepAliveTicker.Stop()

	for {
		select {
		case entry := <-traceCh:
			if _, err := w.Write(entry); err != nil {
				return
			}
			grid.PutByteBuffer(entry)
			if len(traceCh) == 0 {
				// Flush if nothing is queued
				xhttp.Flush(w)
			}
		case <-keepAliveTicker.C:
			if len(traceCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-ctx.Done():
			return
		}
	}
}

// The ConsoleLogHandler handler sends console logs to the connected HTTP client.
func (a adminAPIHandlers) ConsoleLogHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ConsoleLogAdminAction)
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

	logCh := make(chan log.Info, 1000)
	peers, _ := newPeerRestClients(globalEndpoints)
	encodedCh := make(chan []byte, 1000+len(peers)*1000)
	err = globalConsoleSys.Subscribe(logCh, ctx.Done(), node, limitLines, logKind, nil)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Convert local entries to JSON
	go func() {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for {
			select {
			case <-ctx.Done():
				return
			case li := <-logCh:
				if !li.SendLog(node, logKind) {
					continue
				}
				buf.Reset()
				if err := enc.Encode(li); err != nil {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case encodedCh <- append(grid.GetByteBuffer()[:0], buf.Bytes()...):
				}
			}
		}
	}()

	// Collect from matching peers
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		if node == "" || strings.EqualFold(peer.host.Name, node) {
			peer.ConsoleLog(ctx, logKind, encodedCh)
		}
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()
	for {
		select {
		case log, ok := <-encodedCh:
			if !ok {
				return
			}
			_, err = w.Write(log)
			if err != nil {
				return
			}
			grid.PutByteBuffer(log)
			if len(logCh) == 0 {
				// Flush if nothing is queued
				xhttp.Flush(w)
			}
		case <-keepAliveTicker.C:
			if len(logCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-ctx.Done():
			return
		}
	}
}

// KMSCreateKeyHandler - POST /minio/admin/v3/kms/key/create?key-id=<master-key-id>
func (a adminAPIHandlers) KMSCreateKeyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSCreateKeyAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	if err := GlobalKMS.CreateKey(ctx, &kms.CreateKeyRequest{
		Name: r.Form.Get("key-id"),
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseHeadersOnly(w)
}

// KMSStatusHandler - GET /minio/admin/v3/kms/status
func (a adminAPIHandlers) KMSStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSKeyStatusAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	stat, err := GlobalKMS.Status(ctx)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	resp, err := json.Marshal(stat)
	if err != nil {
		writeCustomErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), err.Error(), r.URL)
		return
	}
	writeSuccessResponseJSON(w, resp)
}

// KMSKeyStatusHandler - GET /minio/admin/v3/kms/key/status?key-id=<master-key-id>
func (a adminAPIHandlers) KMSKeyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.KMSKeyStatusAdminAction)
	if objectAPI == nil {
		return
	}

	if GlobalKMS == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	keyID := r.Form.Get("key-id")
	if keyID == "" {
		keyID = GlobalKMS.DefaultKey
	}
	response := madmin.KMSKeyStatus{
		KeyID: keyID,
	}

	kmsContext := kms.Context{"MinIO admin API": "KMSKeyStatusHandler"} // Context for a test key operation
	// 1. Generate a new key using the KMS.
	key, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
		Name:           keyID,
		AssociatedData: kmsContext,
	})
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
	decryptedKey, err := GlobalKMS.Decrypt(ctx, &kms.DecryptRequest{
		Name:           key.KeyID,
		Ciphertext:     key.Ciphertext,
		AssociatedData: kmsContext,
	})
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

func getPoolsInfo(ctx context.Context, allDisks []madmin.Disk) (map[int]map[int]madmin.ErasureSetInfo, error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	z, ok := objectAPI.(*erasureServerPools)
	if !ok {
		return nil, errServerNotInitialized
	}

	poolsInfo := make(map[int]map[int]madmin.ErasureSetInfo)
	for _, d := range allDisks {
		poolInfo, ok := poolsInfo[d.PoolIndex]
		if !ok {
			poolInfo = make(map[int]madmin.ErasureSetInfo)
		}
		erasureSet, ok := poolInfo[d.SetIndex]
		if !ok {
			erasureSet.ID = d.SetIndex
			cache := dataUsageCache{}
			if err := cache.load(ctx, z.serverPools[d.PoolIndex].sets[d.SetIndex], dataUsageCacheName); err == nil {
				dataUsageInfo := cache.dui(dataUsageRoot, nil)
				erasureSet.ObjectsCount = dataUsageInfo.ObjectsTotalCount
				erasureSet.VersionsCount = dataUsageInfo.VersionsTotalCount
				erasureSet.DeleteMarkersCount = dataUsageInfo.DeleteMarkersTotalCount
				erasureSet.Usage = dataUsageInfo.ObjectsTotalSize
			}
		}
		erasureSet.RawCapacity += d.TotalSpace
		erasureSet.RawUsage += d.UsedSpace
		if d.Healing {
			erasureSet.HealDisks = 1
		}
		poolInfo[d.SetIndex] = erasureSet
		poolsInfo[d.PoolIndex] = poolInfo
	}
	return poolsInfo, nil
}

func getServerInfo(ctx context.Context, pools, metrics bool, r *http.Request) madmin.InfoMessage {
	const operationTimeout = 10 * time.Second
	ldap := madmin.LDAP{}
	if globalIAMSys.LDAPConfig.Enabled() {
		ldapConn, err := globalIAMSys.LDAPConfig.LDAP.Connect()
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

	log, audit := fetchLoggerInfo(ctx)

	// Get the notification target info
	notifyTarget := fetchLambdaInfo()

	local := getLocalServerProperty(globalEndpoints, r, metrics)
	servers := globalNotificationSys.ServerInfo(ctx, metrics)
	servers = append(servers, local)

	var poolsInfo map[int]map[int]madmin.ErasureSetInfo
	var backend madmin.ErasureBackend

	mode := madmin.ItemInitializing

	buckets := madmin.Buckets{}
	objects := madmin.Objects{}
	versions := madmin.Versions{}
	deleteMarkers := madmin.DeleteMarkers{}
	usage := madmin.Usage{}

	objectAPI := newObjectLayerFn()
	if objectAPI != nil {
		mode = madmin.ItemOnline

		// Load data usage
		ctx2, cancel := context.WithTimeout(ctx, operationTimeout)
		dataUsageInfo, err := loadDataUsageFromBackend(ctx2, objectAPI)
		cancel()
		if err == nil {
			buckets = madmin.Buckets{Count: dataUsageInfo.BucketsCount}
			objects = madmin.Objects{Count: dataUsageInfo.ObjectsTotalCount}
			versions = madmin.Versions{Count: dataUsageInfo.VersionsTotalCount}
			deleteMarkers = madmin.DeleteMarkers{Count: dataUsageInfo.DeleteMarkersTotalCount}
			usage = madmin.Usage{Size: dataUsageInfo.ObjectsTotalSize}
		} else {
			buckets = madmin.Buckets{Error: err.Error()}
			objects = madmin.Objects{Error: err.Error()}
			deleteMarkers = madmin.DeleteMarkers{Error: err.Error()}
			usage = madmin.Usage{Error: err.Error()}
		}

		// Fetching the backend information
		backendInfo := objectAPI.BackendInfo()
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
			TotalSets:        backendInfo.TotalSets,
			DrivesPerSet:     backendInfo.DrivesPerSet,
		}

		if pools {
			ctx2, cancel := context.WithTimeout(ctx, operationTimeout)
			poolsInfo, _ = getPoolsInfo(ctx2, allDisks)
			cancel()
		}
	}

	domain := globalDomainNames
	services := madmin.Services{
		LDAP:          ldap,
		Logger:        log,
		Audit:         audit,
		Notifications: notifyTarget,
	}
	{
		ctx2, cancel := context.WithTimeout(ctx, operationTimeout)
		services.KMSStatus = fetchKMSStatus(ctx2)
		cancel()
	}

	return madmin.InfoMessage{
		Mode:          string(mode),
		Domain:        domain,
		Region:        globalSite.Region(),
		SQSARN:        globalEventNotifier.GetARNList(),
		DeploymentID:  globalDeploymentID(),
		Buckets:       buckets,
		Objects:       objects,
		Versions:      versions,
		DeleteMarkers: deleteMarkers,
		Usage:         usage,
		Services:      services,
		Backend:       backend,
		Servers:       servers,
		Pools:         poolsInfo,
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
		Transport: globalRemoteTargetTransport,
		Timeout:   10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		ki.Error = err.Error()
		return ki
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&ki); err != nil {
		ki.Error = err.Error()
	}
	return ki
}

func fetchHealthInfo(healthCtx context.Context, objectAPI ObjectLayer, query *url.Values, healthInfoCh chan madmin.HealthInfo, healthInfo madmin.HealthInfo) {
	hostAnonymizer := createHostAnonymizer()

	anonParam := query.Get(anonymizeParam)
	// anonAddr - Anonymizes hosts in given input string
	// (only if the anonymize param is set to srict).
	anonAddr := func(addr string) string {
		if anonParam != anonymizeStrict {
			return addr
		}
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

	getAndWriteNetInfo := func() {
		if query.Get(string(madmin.HealthDataTypeSysNet)) == "true" {
			localNetInfo := madmin.GetNetInfo(globalLocalNodeName, globalInternodeInterface)
			healthInfo.Sys.NetInfo = append(healthInfo.Sys.NetInfo, localNetInfo)

			peerNetInfos := globalNotificationSys.GetNetInfo(healthCtx)
			for _, n := range peerNetInfos {
				anonymizeAddr(&n)
				healthInfo.Sys.NetInfo = append(healthInfo.Sys.NetInfo, n)
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

	// collect all realtime metrics except disk
	// disk metrics are already included under drive info of each server
	getRealtimeMetrics := func() *madmin.RealtimeMetrics {
		var m madmin.RealtimeMetrics
		types := madmin.MetricsAll &^ madmin.MetricsDisk
		mLocal := collectLocalMetrics(types, collectMetricsOpts{})
		m.Merge(&mLocal)
		cctx, cancel := context.WithTimeout(healthCtx, time.Second/2)
		mRemote := collectRemoteMetrics(cctx, types, collectMetricsOpts{})
		cancel()
		m.Merge(&mRemote)
		for idx, host := range m.Hosts {
			m.Hosts[idx] = anonAddr(host)
		}
		for host, metrics := range m.ByHost {
			m.ByHost[anonAddr(host)] = metrics
			delete(m.ByHost, host)
		}
		return &m
	}

	anonymizeCmdLine := func(cmdLine string) string {
		if anonParam != anonymizeStrict {
			return cmdLine
		}

		if !globalIsDistErasure {
			// FS mode - single server - hard code to `server1`
			anonCmdLine := strings.ReplaceAll(cmdLine, globalLocalNodeName, "server1")
			if len(globalMinioConsoleHost) > 0 {
				anonCmdLine = strings.ReplaceAll(anonCmdLine, globalMinioConsoleHost, "server1")
			}
			return anonCmdLine
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

		if !strings.Contains(poolsArgs, "{") || !strings.Contains(poolsArgs, "}") {
			// No ellipses pattern. Anonymize host name from every pool arg
			pools := strings.Fields(poolsArgs)
			anonPools = make([]string, len(pools))
			for index, arg := range pools {
				anonPools[index] = anonAddr(arg)
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
			config, err := readServerConfig(healthCtx, objectAPI, nil)
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
		defer xioutil.SafeClose(healthInfoCh)

		partialWrite(healthInfo) // Write first message with only version and deployment id populated
		getAndWritePlatformInfo()
		getAndWriteCPUs()
		getAndWritePartitions()
		getAndWriteNetInfo()
		getAndWriteOSInfo()
		getAndWriteMemInfo()
		getAndWriteProcInfo()
		getAndWriteMinioConfig()
		getAndWriteSysErrors()
		getAndWriteSysServices()
		getAndWriteSysConfig()

		if query.Get("minioinfo") == "true" {
			infoMessage := getServerInfo(healthCtx, false, true, nil)
			servers := make([]madmin.ServerInfo, 0, len(infoMessage.Servers))
			for _, server := range infoMessage.Servers {
				anonEndpoint := anonAddr(server.Endpoint)
				servers = append(servers, madmin.ServerInfo{
					State:    server.State,
					Endpoint: anonEndpoint,
					Uptime:   server.Uptime,
					Version:  server.Version,
					CommitID: server.CommitID,
					Network:  anonymizeNetwork(server.Network),
					Drives:   anonymizeDrives(server.Disks),
					PoolNumber: func() int {
						if len(server.PoolNumbers) == 1 {
							return server.PoolNumbers[0]
						}
						return math.MaxInt // this indicates that its unset.
					}(),
					PoolNumbers: server.PoolNumbers,
					MemStats: madmin.MemStats{
						Alloc:      server.MemStats.Alloc,
						TotalAlloc: server.MemStats.TotalAlloc,
						Mallocs:    server.MemStats.Mallocs,
						Frees:      server.MemStats.Frees,
						HeapAlloc:  server.MemStats.HeapAlloc,
					},
					GoMaxProcs:     server.GoMaxProcs,
					NumCPU:         server.NumCPU,
					RuntimeVersion: server.RuntimeVersion,
					GCStats:        server.GCStats,
					MinioEnvVars:   server.MinioEnvVars,
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
				Metrics:      getRealtimeMetrics(),
			}
			partialWrite(healthInfo)
		}
	}()
}

// HealthInfoHandler - GET /minio/admin/v3/healthinfo
// ----------
// Get server health info
func (a adminAPIHandlers) HealthInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.HealthInfoAdminAction)
	if objectAPI == nil {
		return
	}

	query := r.Form
	healthInfoCh := make(chan madmin.HealthInfo)
	enc := json.NewEncoder(w)

	healthInfo := madmin.HealthInfo{
		TimeStamp: time.Now().UTC(),
		Version:   madmin.HealthInfoVersion,
		Minio: madmin.MinioHealthInfo{
			Info: madmin.MinioInfo{
				DeploymentID: globalDeploymentID(),
			},
		},
	}

	errResp := func(err error) {
		errorResponse := getAPIErrorResponse(ctx, toAdminAPIErr(ctx, err), r.URL.String(),
			w.Header().Get(xhttp.AmzRequestID), w.Header().Get(xhttp.AmzRequestHostID))
		encodedErrorResponse := encodeResponse(errorResponse)
		healthInfo.Error = string(encodedErrorResponse)
		adminLogIf(ctx, enc.Encode(healthInfo))
	}

	deadline := 10 * time.Second // Default deadline is 10secs for health diagnostics.
	if dstr := query.Get("deadline"); dstr != "" {
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

	defer nsLock.Unlock(lkctx)
	healthCtx, healthCancel := context.WithTimeout(lkctx.Context(), deadline)
	defer healthCancel()

	go fetchHealthInfo(healthCtx, objectAPI, &query, healthInfoCh, healthInfo)

	setCommonHeaders(w)
	setEventStreamHeaders(w)
	w.WriteHeader(http.StatusOK)

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
				xhttp.Flush(w)
			}
		case <-ticker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
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
			check := xxh3.Hash(c.RawIssuer)
			check ^= xxh3.Hash(c.RawSubjectPublicKeyInfo)
			// We XOR, so order doesn't matter.
			for _, v := range c.DNSNames {
				check ^= xxh3.HashString(v)
			}
			for _, v := range c.EmailAddresses {
				check ^= xxh3.HashString(v)
			}
			for _, v := range c.IPAddresses {
				check ^= xxh3.HashString(v.String())
			}
			for _, v := range c.URIs {
				check ^= xxh3.HashString(v.String())
			}
			tlsInfo.Certs = append(tlsInfo.Certs, madmin.TLSCert{
				PubKeyAlgo:    c.PublicKeyAlgorithm.String(),
				SignatureAlgo: c.SignatureAlgorithm.String(),
				NotBefore:     c.NotBefore,
				NotAfter:      c.NotAfter,
				Checksum:      strconv.FormatUint(check, 16),
			})
		}
	}
	return tlsInfo
}

// ServerInfoHandler - GET /minio/admin/v3/info
// ----------
// Get server information
func (a adminAPIHandlers) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.ServerInfoAdminAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	metrics := r.Form.Get(peerRESTMetrics) == "true"

	// Marshal API response
	jsonBytes, err := json.Marshal(getServerInfo(ctx, true, metrics, r))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Reply with storage information (across nodes in a
	// distributed setup) as json.
	writeSuccessResponseJSON(w, jsonBytes)
}

func fetchLambdaInfo() []map[string][]madmin.TargetIDStatus {
	lambdaMap := make(map[string][]madmin.TargetIDStatus)

	for _, tgt := range globalEventNotifier.Targets() {
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

// fetchKMSStatus fetches KMS-related status information for all instances
func fetchKMSStatus(ctx context.Context) []madmin.KMS {
	if GlobalKMS == nil {
		return []madmin.KMS{}
	}

	stat, err := GlobalKMS.Status(ctx)
	if err != nil {
		kmsLogIf(ctx, err, "failed to fetch KMS status information")
		return []madmin.KMS{}
	}

	stats := make([]madmin.KMS, 0, len(stat.Endpoints))
	for endpoint, state := range stat.Endpoints {
		stats = append(stats, madmin.KMS{
			Status:   string(state),
			Endpoint: endpoint,
		})
	}
	return stats
}

func targetStatus(ctx context.Context, h logger.Target) madmin.Status {
	if h.IsOnline(ctx) {
		return madmin.Status{Status: string(madmin.ItemOnline)}
	}
	return madmin.Status{Status: string(madmin.ItemOffline)}
}

// fetchLoggerInfo return log info
func fetchLoggerInfo(ctx context.Context) ([]madmin.Logger, []madmin.Audit) {
	var loggerInfo []madmin.Logger
	var auditloggerInfo []madmin.Audit
	for _, tgt := range logger.SystemTargets() {
		if tgt.Endpoint() != "" {
			loggerInfo = append(loggerInfo, madmin.Logger{tgt.String(): targetStatus(ctx, tgt)})
		}
	}

	for _, tgt := range logger.AuditTargets() {
		if tgt.Endpoint() != "" {
			auditloggerInfo = append(auditloggerInfo, madmin.Audit{tgt.String(): targetStatus(ctx, tgt)})
		}
	}

	return loggerInfo, auditloggerInfo
}

func embedFileInZip(zipWriter *zip.Writer, name string, data []byte, fileMode os.FileMode) error {
	// Send profiling data to zip as file
	header, zerr := zip.FileInfoHeader(dummyFileInfo{
		name:    name,
		size:    int64(len(data)),
		mode:    fileMode,
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

// getClusterMetaInfo gets information of the current cluster and
// returns it.
// This is not a critical function, and it is allowed
// to fail with a ten seconds timeout, returning nil.
func getClusterMetaInfo(ctx context.Context) []byte {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil
	}

	// Add a ten seconds timeout because getting profiling data
	// is critical for debugging, in contrary to getting cluster info
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resultCh := make(chan madmin.ClusterRegistrationInfo)

	go func() {
		defer xioutil.SafeClose(resultCh)

		ci := madmin.ClusterRegistrationInfo{}
		ci.Info.NoOfServerPools = len(globalEndpoints)
		ci.Info.NoOfServers = totalNodeCount()
		ci.Info.MinioVersion = Version

		si := objectAPI.StorageInfo(ctx, false)

		ci.Info.NoOfDrives = len(si.Disks)
		for _, disk := range si.Disks {
			ci.Info.TotalDriveSpace += disk.TotalSpace
			ci.Info.UsedDriveSpace += disk.UsedSpace
		}

		dataUsageInfo, _ := loadDataUsageFromBackend(ctx, objectAPI)

		ci.UsedCapacity = dataUsageInfo.ObjectsTotalSize
		ci.Info.NoOfBuckets = dataUsageInfo.BucketsCount
		ci.Info.NoOfObjects = dataUsageInfo.ObjectsTotalCount

		ci.DeploymentID = globalDeploymentID()
		ci.ClusterName = fmt.Sprintf("%d-servers-%d-disks-%s", ci.Info.NoOfServers, ci.Info.NoOfDrives, ci.Info.MinioVersion)

		select {
		case resultCh <- ci:
		case <-ctx.Done():
			return
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case ci := <-resultCh:
		out, err := json.MarshalIndent(ci, "", "  ")
		if err != nil {
			bugLogIf(ctx, err)
			return nil
		}
		return out
	}
}

func bytesToPublicKey(pub []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pub)
	if block != nil {
		pub = block.Bytes
	}
	key, err := x509.ParsePKCS1PublicKey(pub)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// getRawDataer provides an interface for getting raw FS files.
type getRawDataer interface {
	GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, info StatInfo) error) error
}

// InspectDataHandler - GET /minio/admin/v3/inspect-data
// ----------
// Download file from all nodes in a zip format
func (a adminAPIHandlers) InspectDataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Validate request signature.
	_, adminAPIErr := checkAdminRequestAuth(ctx, r, policy.InspectDataAction, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return
	}

	objLayer := newObjectLayerFn()
	o, ok := objLayer.(getRawDataer)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if err := parseForm(r); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	volume := r.Form.Get("volume")
	if len(volume) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketName), r.URL)
		return
	}
	file := r.Form.Get("file")
	if len(file) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	file = filepath.ToSlash(file)
	// Reject attempts to traverse parent or absolute paths.
	if hasBadPathComponent(volume) || hasBadPathComponent(file) {
		writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInvalidResourceName), r.URL)
		return
	}

	var publicKey *rsa.PublicKey

	publicKeyB64 := r.Form.Get("public-key")
	if publicKeyB64 != "" {
		publicKeyBytes, err := base64.StdEncoding.DecodeString(publicKeyB64)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		publicKey, err = bytesToPublicKey(publicKeyBytes)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	addErr := func(msg string) {}

	// Write a version for making *incompatible* changes.
	// The AdminClient will reject any version it does not know.
	var inspectZipW *zip.Writer
	if publicKey != nil {
		w.WriteHeader(200)
		stream := estream.NewWriter(w)
		defer stream.Close()

		clusterKey, err := bytesToPublicKey(getSubnetAdminPublicKey())
		if err != nil {
			bugLogIf(ctx, stream.AddError(err.Error()))
			return
		}
		err = stream.AddKeyEncrypted(clusterKey)
		if err != nil {
			bugLogIf(ctx, stream.AddError(err.Error()))
			return
		}
		if b := getClusterMetaInfo(ctx); len(b) > 0 {
			w, err := stream.AddEncryptedStream("cluster.info", nil)
			if err != nil {
				bugLogIf(ctx, err)
				return
			}
			w.Write(b)
			w.Close()
		}

		// Add new key for inspect data.
		if err := stream.AddKeyEncrypted(publicKey); err != nil {
			bugLogIf(ctx, stream.AddError(err.Error()))
			return
		}
		encStream, err := stream.AddEncryptedStream("inspect.zip", nil)
		if err != nil {
			bugLogIf(ctx, stream.AddError(err.Error()))
			return
		}
		addErr = func(msg string) {
			inspectZipW.Close()
			encStream.Close()
			stream.AddError(msg)
		}
		defer encStream.Close()

		inspectZipW = zip.NewWriter(encStream)
		defer inspectZipW.Close()
	} else {
		// Legacy: Remove if we stop supporting inspection without public key.
		var key [32]byte
		// MUST use crypto/rand
		n, err := crand.Read(key[:])
		if err != nil || n != len(key) {
			bugLogIf(ctx, err)
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		// Write a version for making *incompatible* changes.
		// The AdminClient will reject any version it does not know.
		if publicKey == nil {
			w.Write([]byte{1})
			w.Write(key[:])
		}

		stream, err := sio.AES_256_GCM.Stream(key[:])
		if err != nil {
			bugLogIf(ctx, err)
			return
		}
		// Zero nonce, we only use each key once, and 32 bytes is plenty.
		nonce := make([]byte, stream.NonceSize())
		encw := stream.EncryptWriter(w, nonce, nil)
		defer encw.Close()

		// Initialize a zip writer which will provide a zipped content
		// of profiling data of all nodes
		inspectZipW = zip.NewWriter(encw)
		defer inspectZipW.Close()

		if b := getClusterMetaInfo(ctx); len(b) > 0 {
			adminLogIf(ctx, embedFileInZip(inspectZipW, "cluster.info", b, 0o600))
		}
	}

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
			bugLogIf(ctx, zerr)
			return nil
		}
		header.Method = zip.Deflate
		zwriter, zerr := inspectZipW.CreateHeader(header)
		if zerr != nil {
			bugLogIf(ctx, zerr)
			return nil
		}
		if _, err := io.Copy(zwriter, r); err != nil {
			adminLogIf(ctx, err)
		}
		return nil
	}

	// save args passed to inspect command
	var sb bytes.Buffer
	fmt.Fprintf(&sb, "Inspect path: %s%s%s\n", volume, slashSeparator, file)
	sb.WriteString("Server command line args:")
	for _, pool := range globalEndpoints {
		sb.WriteString(" ")
		sb.WriteString(pool.CmdLine)
	}
	sb.WriteString("\n")
	adminLogIf(ctx, embedFileInZip(inspectZipW, "inspect-input.txt", sb.Bytes(), 0o600))

	err := o.GetRawData(ctx, volume, file, rawDataFn)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			addErr("GetRawData: No files matched the given pattern")
			return
		}
		embedFileInZip(inspectZipW, "GetRawData-err.txt", []byte(err.Error()), 0o600)
		adminLogIf(ctx, err)
	}

	// save the format.json as part of inspect by default
	if volume != minioMetaBucket || file != formatConfigFile {
		err = o.GetRawData(ctx, minioMetaBucket, formatConfigFile, rawDataFn)
	}
	if !errors.Is(err, errFileNotFound) {
		adminLogIf(ctx, err)
	}

	scheme := "https"
	if !globalIsTLS {
		scheme = "http"
	}

	// save MinIO start script to inspect command
	var scrb bytes.Buffer
	fmt.Fprintf(&scrb, `#!/usr/bin/env bash

function main() {
	for file in $(ls -1); do
		dest_file=$(echo "$file" | cut -d ":" -f1)
		mv "$file" "$dest_file"
	done

	# Read content of inspect-input.txt
	MINIO_OPTS=$(grep "Server command line args" <./inspect-input.txt | sed "s/Server command line args: //g" | sed -r "s#%s:\/\/#\.\/#g")

	# Start MinIO instance using the options
	START_CMD="CI=on _MINIO_AUTO_DRIVE_HEALING=off minio server ${MINIO_OPTS} &"
	echo
	echo "Starting MinIO instance: ${START_CMD}"
	echo
	eval "$START_CMD"
	MINIO_SRVR_PID="$!"
	echo "MinIO Server PID: ${MINIO_SRVR_PID}"
	echo
	echo "Waiting for MinIO instance to get ready!"
	sleep 10
}

main "$@"`, scheme)
	adminLogIf(ctx, embedFileInZip(inspectZipW, "start-minio.sh", scrb.Bytes(), 0o755))
}

func getSubnetAdminPublicKey() []byte {
	if globalIsCICD {
		return subnetAdminPublicKeyDev
	}
	return subnetAdminPublicKey
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
// The anonymizer map will have mappings for all these variants for efficiently replacing
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

// createHostAnonymizer - Creates a map of various strings to corresponding anonymized names
func createHostAnonymizer() map[string]string {
	if !globalIsDistErasure {
		return createHostAnonymizerForFSMode()
	}

	hostAnonymizer := map[string]string{}
	hosts := set.NewStringSet()
	srvrIdx := 0

	for poolIdx, pool := range globalEndpoints {
		for _, endpoint := range pool.Endpoints {
			if !hosts.Contains(endpoint.Host) {
				hosts.Add(endpoint.Host)
				srvrIdx++
			}
			anonymizeHost(hostAnonymizer, endpoint, poolIdx+1, srvrIdx)
		}
	}
	return hostAnonymizer
}
