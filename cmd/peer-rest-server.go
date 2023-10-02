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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	b "github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/mux"
	"github.com/minio/pkg/v2/logger/message/log"
	"github.com/tinylib/msgp/msgp"
)

// To abstract a node over network.
type peerRESTServer struct{}

// GetLocksHandler - returns list of older lock from the server.
func (s *peerRESTServer) GetLocksHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "GetLocks")
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(globalLockServer.DupLockMap()))
}

// DeletePolicyHandler - deletes a policy on the server.
func (s *peerRESTServer) DeletePolicyHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	policyName := vars[peerRESTPolicy]
	if policyName == "" {
		s.writeErrorResponse(w, errors.New("policyName is missing"))
		return
	}

	if err := globalIAMSys.DeletePolicy(r.Context(), policyName, false); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// LoadPolicyHandler - reloads a policy on the server.
func (s *peerRESTServer) LoadPolicyHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	policyName := vars[peerRESTPolicy]
	if policyName == "" {
		s.writeErrorResponse(w, errors.New("policyName is missing"))
		return
	}

	if err := globalIAMSys.LoadPolicy(r.Context(), objAPI, policyName); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// LoadPolicyMappingHandler - reloads a policy mapping on the server.
func (s *peerRESTServer) LoadPolicyMappingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	userOrGroup := r.Form.Get(peerRESTUserOrGroup)
	if userOrGroup == "" {
		s.writeErrorResponse(w, errors.New("user-or-group is missing"))
		return
	}

	userType, err := strconv.Atoi(r.Form.Get(peerRESTUserType))
	if err != nil {
		s.writeErrorResponse(w, fmt.Errorf("user-type `%s` is invalid: %w", r.Form.Get(peerRESTUserType), err))
		return
	}

	_, isGroup := r.Form[peerRESTIsGroup]
	if err := globalIAMSys.LoadPolicyMapping(r.Context(), objAPI, userOrGroup, IAMUserType(userType), isGroup); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// DeleteServiceAccountHandler - deletes a service account on the server.
func (s *peerRESTServer) DeleteServiceAccountHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars[peerRESTUser]
	if accessKey == "" {
		s.writeErrorResponse(w, errors.New("service account name is missing"))
		return
	}

	if err := globalIAMSys.DeleteServiceAccount(r.Context(), accessKey, false); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// LoadServiceAccountHandler - reloads a service account on the server.
func (s *peerRESTServer) LoadServiceAccountHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars[peerRESTUser]
	if accessKey == "" {
		s.writeErrorResponse(w, errors.New("service account parameter is missing"))
		return
	}

	if err := globalIAMSys.LoadServiceAccount(r.Context(), accessKey); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// DeleteUserHandler - deletes a user on the server.
func (s *peerRESTServer) DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars[peerRESTUser]
	if accessKey == "" {
		s.writeErrorResponse(w, errors.New("username is missing"))
		return
	}

	if err := globalIAMSys.DeleteUser(r.Context(), accessKey, false); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// LoadUserHandler - reloads a user on the server.
func (s *peerRESTServer) LoadUserHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars[peerRESTUser]
	if accessKey == "" {
		s.writeErrorResponse(w, errors.New("username is missing"))
		return
	}

	temp, err := strconv.ParseBool(vars[peerRESTUserTemp])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	userType := regUser
	if temp {
		userType = stsUser
	}

	if err = globalIAMSys.LoadUser(r.Context(), objAPI, accessKey, userType); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// LoadGroupHandler - reloads group along with members list.
func (s *peerRESTServer) LoadGroupHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	group := vars[peerRESTGroup]
	err := globalIAMSys.LoadGroup(r.Context(), objAPI, group)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// StartProfilingHandler - Issues the start profiling command.
func (s *peerRESTServer) StartProfilingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	profiles := strings.Split(vars[peerRESTProfiler], ",")
	if len(profiles) == 0 {
		s.writeErrorResponse(w, errors.New("profiler name is missing"))
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

	for _, profiler := range profiles {
		prof, err := startProfiler(profiler)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
		globalProfiler[profiler] = prof
	}
}

// DownloadProfilingDataHandler - returns profiled data.
func (s *peerRESTServer) DownloadProfilingDataHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "DownloadProfiling")
	profileData, err := getProfileData()
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(profileData))
}

func (s *peerRESTServer) LocalStorageInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "LocalStorageInfo")

	objLayer := newObjectLayerFn()
	if objLayer == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(objLayer.LocalStorageInfo(r.Context())))
}

// ServerInfoHandler - returns Server Info
func (s *peerRESTServer) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "ServerInfo")
	info := getLocalServerProperty(globalEndpoints, r)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetCPUsHandler - returns CPU info.
func (s *peerRESTServer) GetCPUsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetCPUs(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetPartitionsHandler - returns disk partition information.
func (s *peerRESTServer) GetPartitionsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetPartitions(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetOSInfoHandler - returns operating system's information.
func (s *peerRESTServer) GetOSInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetOSInfo(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetProcInfoHandler - returns this MinIO process information.
func (s *peerRESTServer) GetProcInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetProcInfo(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetMemInfoHandler - returns memory information.
func (s *peerRESTServer) GetMemInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetMemInfo(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetMetricsHandler - returns server metrics.
func (s *peerRESTServer) GetMetricsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	var types madmin.MetricType
	if t, _ := strconv.ParseUint(r.Form.Get(peerRESTMetricsTypes), 10, 64); t != 0 {
		types = madmin.MetricType(t)
	} else {
		types = madmin.MetricsAll
	}

	diskMap := make(map[string]struct{})
	for _, disk := range r.Form[peerRESTDisk] {
		diskMap[disk] = struct{}{}
	}

	hostMap := make(map[string]struct{})
	for _, host := range r.Form[peerRESTHost] {
		hostMap[host] = struct{}{}
	}

	jobID := r.Form.Get(peerRESTJobID)
	depID := r.Form.Get(peerRESTDepID)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := collectLocalMetrics(types, collectMetricsOpts{
		disks: diskMap,
		hosts: hostMap,
		jobID: jobID,
		depID: depID,
	})
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

func (s *peerRESTServer) GetResourceMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	enc := gob.NewEncoder(w)

	for m := range ReportMetrics(r.Context(), resourceMetricsGroups) {
		if err := enc.Encode(m); err != nil {
			s.writeErrorResponse(w, errors.New("Encoding metric failed: "+err.Error()))
			return
		}
	}
}

// GetSysConfigHandler - returns system config information.
// (only the config that are of concern to minio)
func (s *peerRESTServer) GetSysConfigHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetSysConfig(ctx, r.Host)

	logger.LogOnceIf(ctx, gob.NewEncoder(w).Encode(info), "get-sys-config")
}

// GetSysServicesHandler - returns system services information.
// (only the services that are of concern to minio)
func (s *peerRESTServer) GetSysServicesHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetSysServices(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetSysErrorsHandler - returns system level errors
func (s *peerRESTServer) GetSysErrorsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	info := madmin.GetSysErrors(ctx, r.Host)

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// DeleteBucketMetadataHandler - Delete in memory bucket metadata
func (s *peerRESTServer) DeleteBucketMetadataHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}

	globalReplicationStats.Delete(bucketName)
	globalBucketMetadataSys.Remove(bucketName)
	globalBucketTargetSys.Delete(bucketName)
	globalEventNotifier.RemoveNotification(bucketName)
	globalBucketConnStats.delete(bucketName)
	globalBucketHTTPStats.delete(bucketName)
	if localMetacacheMgr != nil {
		localMetacacheMgr.deleteBucketCache(bucketName)
	}
}

// ReloadSiteReplicationConfigHandler - reloads site replication configuration from the disks
func (s *peerRESTServer) ReloadSiteReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "LoadSiteReplication")

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	logger.LogIf(r.Context(), globalSiteReplicationSys.Init(ctx, objAPI))
}

// GetAllBucketStatsHandler - fetches bucket replication stats for all buckets from this peer.
func (s *peerRESTServer) GetAllBucketStatsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	replicationStats := globalReplicationStats.GetAll()
	bucketStatsMap := make(map[string]BucketStats, len(replicationStats))
	for k, v := range replicationStats {
		bucketStatsMap[k] = BucketStats{
			ReplicationStats: v,
		}
	}
	logger.LogIf(r.Context(), msgp.Encode(w, &BucketStatsMap{Stats: bucketStatsMap, Timestamp: UTCNow()}))
}

// GetBucketStatsHandler - fetches current in-memory bucket stats, currently only
// returns BucketStats, that currently includes ReplicationStats.
func (s *peerRESTServer) GetBucketStatsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}

	bs := BucketStats{
		ReplicationStats: globalReplicationStats.Get(bucketName),
		QueueStats:       ReplicationQueueStats{Nodes: []ReplQNodeStats{globalReplicationStats.getNodeQueueStats(bucketName)}},
	}
	logger.LogIf(r.Context(), msgp.Encode(w, &bs))
}

// GetSRMetricsHandler - fetches current in-memory replication stats at site level from this peer
func (s *peerRESTServer) GetSRMetricsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	sm := globalReplicationStats.getSRMetricsForNode()
	logger.LogIf(r.Context(), msgp.Encode(w, &sm))
}

// LoadBucketMetadataHandler - reloads in memory bucket metadata
func (s *peerRESTServer) LoadBucketMetadataHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	meta, err := loadBucketMetadata(r.Context(), objAPI, bucketName)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	globalBucketMetadataSys.Set(bucketName, meta)

	if meta.notificationConfig != nil {
		globalEventNotifier.AddRulesMap(bucketName, meta.notificationConfig.ToRulesMap())
	}

	if meta.bucketTargetConfig != nil {
		globalBucketTargetSys.UpdateAllTargets(bucketName, meta.bucketTargetConfig)
	}
}

func (s *peerRESTServer) GetMetacacheListingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}
	ctx := newContext(r, w, "GetMetacacheListing")

	var opts listPathOptions
	err := gob.NewDecoder(r.Body).Decode(&opts)
	if err != nil && err != io.EOF {
		s.writeErrorResponse(w, err)
		return
	}
	resp := localMetacacheMgr.getBucket(ctx, opts.Bucket).findCache(opts)
	logger.LogIf(ctx, msgp.Encode(w, &resp))
}

func (s *peerRESTServer) UpdateMetacacheListingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}
	ctx := newContext(r, w, "UpdateMetacacheListing")

	var req metacache
	err := msgp.Decode(r.Body, &req)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	cache, err := localMetacacheMgr.updateCacheEntry(req)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	// Return updated metadata.
	logger.LogIf(ctx, msgp.Encode(w, &cache))
}

// PutBucketNotificationHandler - Set bucket policy.
func (s *peerRESTServer) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}

	var rulesMap event.RulesMap
	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&rulesMap)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	globalEventNotifier.AddRulesMap(bucketName, rulesMap)
}

// Return disk IDs of all the local disks.
func getLocalDiskIDs(z *erasureServerPools) []string {
	var ids []string

	for poolIdx := range z.serverPools {
		for _, set := range z.serverPools[poolIdx].sets {
			disks := set.getDisks()
			for _, disk := range disks {
				if disk == nil {
					continue
				}
				if disk.IsLocal() {
					id, err := disk.GetDiskID()
					if err != nil {
						continue
					}
					if id == "" {
						continue
					}
					ids = append(ids, id)
				}
			}
		}
	}

	return ids
}

// HealthHandler - returns true of health
func (s *peerRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	s.IsValid(w, r)
}

// GetLocalDiskIDs - Return disk IDs of all the local disks.
func (s *peerRESTServer) GetLocalDiskIDs(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "GetLocalDiskIDs")

	objLayer := newObjectLayerFn()

	// Service not initialized yet
	if objLayer == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	z, ok := objLayer.(*erasureServerPools)
	if !ok {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	ids := getLocalDiskIDs(z)
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(ids))
}

// DownloadBinary - updates the current server.
func (s *peerRESTServer) DownloadBinaryHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	var info binaryInfo
	err := gob.NewDecoder(r.Body).Decode(&info)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	if err = verifyBinary(info.URL, info.Sha256Sum, info.ReleaseInfo, getMinioMode(), info.BinaryFile); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// CommitBinary - overwrites the current binary with the new one.
func (s *peerRESTServer) CommitBinaryHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	if err := commitBinary(); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

var errUnsupportedSignal = fmt.Errorf("unsupported signal")

// SignalServiceHandler - signal service handler.
func (s *peerRESTServer) SignalServiceHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	signalString := vars[peerRESTSignal]
	if signalString == "" {
		s.writeErrorResponse(w, errors.New("signal name is missing"))
		return
	}
	si, err := strconv.Atoi(signalString)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	signal := serviceSignal(si)
	switch signal {
	case serviceRestart:
		globalServiceSignalCh <- signal
	case serviceStop:
		globalServiceSignalCh <- signal
	case serviceFreeze:
		freezeServices()
	case serviceUnFreeze:
		unfreezeServices()
	case serviceReloadDynamic:
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			s.writeErrorResponse(w, errServerNotInitialized)
			return
		}
		srvCfg, err := getValidConfig(objAPI)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
		subSys := r.Form.Get(peerRESTSubSys)
		// Apply dynamic values.
		if subSys == "" {
			err = applyDynamicConfig(r.Context(), objAPI, srvCfg)
		} else {
			err = applyDynamicConfigForSubSys(r.Context(), objAPI, srvCfg, subSys)
		}
		if err != nil {
			s.writeErrorResponse(w, err)
		}
		return
	default:
		s.writeErrorResponse(w, errUnsupportedSignal)
		return
	}
}

// ListenHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) ListenHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	values := r.Form

	var prefix string
	if len(values[peerRESTListenPrefix]) > 1 {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	if len(values[peerRESTListenPrefix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenPrefix][0]); err != nil {
			s.writeErrorResponse(w, err)
			return
		}

		prefix = values[peerRESTListenPrefix][0]
	}

	var suffix string
	if len(values[peerRESTListenSuffix]) > 1 {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	if len(values[peerRESTListenSuffix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenSuffix][0]); err != nil {
			s.writeErrorResponse(w, err)
			return
		}

		suffix = values[peerRESTListenSuffix][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	var eventNames []event.Name
	var mask pubsub.Mask
	for _, ev := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(ev)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
		mask.MergeMaskable(eventName)
		eventNames = append(eventNames, eventName)
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	doneCh := r.Context().Done()

	// Listen Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan event.Event, 2000)

	err := globalHTTPListen.Subscribe(mask, ch, doneCh, func(ev event.Event) bool {
		if ev.S3.Bucket.Name != "" && values.Get(peerRESTListenBucket) != "" {
			if ev.S3.Bucket.Name != values.Get(peerRESTListenBucket) {
				return false
			}
		}
		return rulesMap.MatchSimple(ev.EventName, ev.S3.Object.Key)
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case ev := <-ch:
			if err := enc.Encode(ev); err != nil {
				return
			}
			if len(ch) == 0 {
				// Flush if nothing is queued
				w.(http.Flusher).Flush()
			}
		case <-r.Context().Done():
			return
		case <-keepAliveTicker.C:
			if err := enc.Encode(&event.Event{}); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

// TraceHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) TraceHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	var traceOpts madmin.ServiceTraceOpts
	err := traceOpts.ParseParams(r)
	if err != nil {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	// Trace Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan madmin.TraceInfo, 2000)
	err = globalTrace.Subscribe(traceOpts.TraceTypes(), ch, r.Context().Done(), func(entry madmin.TraceInfo) bool {
		return shouldTrace(entry, traceOpts)
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	// Publish bootstrap events that have already occurred before client could subscribe.
	if traceOpts.TraceTypes().Contains(madmin.TraceBootstrap) {
		go globalBootstrapTracer.Publish(r.Context(), globalTrace)
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case entry := <-ch:
			if err := enc.Encode(entry); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		case <-keepAliveTicker.C:
			if err := enc.Encode(&madmin.TraceInfo{}); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

func (s *peerRESTServer) BackgroundHealStatusHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}
	ctx := newContext(r, w, "BackgroundHealStatus")

	state, ok := getLocalBackgroundHealStatus(ctx, newObjectLayerFn())
	if !ok {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(state))
}

func (s *peerRESTServer) ReloadPoolMetaHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		return
	}
	if err := pools.ReloadPoolMeta(r.Context()); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

func (s *peerRESTServer) StopRebalanceHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}
	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		s.writeErrorResponse(w, errors.New("not a multiple pools setup"))
		return
	}

	pools.StopRebalance()
}

func (s *peerRESTServer) LoadRebalanceMetaHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		s.writeErrorResponse(w, errors.New("not a multiple pools setup"))
		return
	}

	startRebalanceStr := r.Form.Get(peerRESTStartRebalance)
	startRebalance, err := strconv.ParseBool(startRebalanceStr)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	if err := pools.loadRebalanceMeta(r.Context()); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	if startRebalance {
		go pools.StartRebalance()
	}
}

func (s *peerRESTServer) LoadTransitionTierConfigHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}
	go func() {
		err := globalTierConfigMgr.Reload(context.Background(), newObjectLayerFn())
		if err != nil {
			logger.LogIf(context.Background(), fmt.Errorf("Failed to reload remote tier config %s", err))
		}
	}()
}

// ConsoleLogHandler sends console logs of this node back to peer rest client
func (s *peerRESTServer) ConsoleLogHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	ch := make(chan log.Info, 2000)
	err := globalConsoleSys.Subscribe(ch, doneCh, "", 0, madmin.LogMaskAll, nil)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(entry); err != nil {
				return
			}
			if len(ch) == 0 {
				w.(http.Flusher).Flush()
			}
		case <-keepAliveTicker.C:
			if len(ch) == 0 {
				if err := enc.Encode(&madmin.LogInfo{}); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (s *peerRESTServer) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
}

// IsValid - To authenticate and verify the time difference.
func (s *peerRESTServer) IsValid(w http.ResponseWriter, r *http.Request) bool {
	if err := storageServerRequestValidate(r); err != nil {
		s.writeErrorResponse(w, err)
		return false
	}
	return true
}

// GetBandwidth gets the bandwidth for the buckets requested.
func (s *peerRESTServer) GetBandwidth(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	bucketsString := r.Form.Get("buckets")

	doneCh := make(chan struct{})
	defer close(doneCh)

	selectBuckets := b.SelectBuckets(strings.Split(bucketsString, ",")...)
	report := globalBucketMonitor.GetReport(selectBuckets)

	enc := gob.NewEncoder(w)
	if err := enc.Encode(report); err != nil {
		s.writeErrorResponse(w, errors.New("Encoding report failed: "+err.Error()))
		return
	}
}

// GetPeerMetrics gets the metrics to be federated across peers.
func (s *peerRESTServer) GetPeerMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	enc := gob.NewEncoder(w)

	for m := range ReportMetrics(r.Context(), peerMetricsGroups) {
		if err := enc.Encode(m); err != nil {
			s.writeErrorResponse(w, errors.New("Encoding metric failed: "+err.Error()))
			return
		}
	}
}

// GetPeerBucketMetrics gets the metrics to be federated across peers.
func (s *peerRESTServer) GetPeerBucketMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	enc := gob.NewEncoder(w)

	for m := range ReportMetrics(r.Context(), bucketPeerMetricsGroups) {
		if err := enc.Encode(m); err != nil {
			s.writeErrorResponse(w, errors.New("Encoding metric failed: "+err.Error()))
			return
		}
	}
}

func (s *peerRESTServer) SpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	sizeStr := r.Form.Get(peerRESTSize)
	durationStr := r.Form.Get(peerRESTDuration)
	concurrentStr := r.Form.Get(peerRESTConcurrent)
	storageClass := r.Form.Get(peerRESTStorageClass)
	bucketName := r.Form.Get(peerRESTBucket)

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

	done := keepHTTPResponseAlive(w)

	result, err := selfSpeedTest(r.Context(), speedTestOpts{
		objectSize:   size,
		concurrency:  concurrent,
		duration:     duration,
		storageClass: storageClass,
		bucketName:   bucketName,
	})
	if err != nil {
		result.Error = err.Error()
	}

	done(nil)
	logger.LogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}

// GetLastDayTierStatsHandler - returns per-tier stats in the last 24hrs for this server
func (s *peerRESTServer) GetLastDayTierStatsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	ctx := newContext(r, w, "GetLastDayTierStats")
	if objAPI := newObjectLayerFn(); objAPI == nil || globalTransitionState == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	result := globalTransitionState.getDailyAllTierStats()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(result))
}

func (s *peerRESTServer) DriveSpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

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

	done := keepHTTPResponseAlive(w)
	result := driveSpeedTest(r.Context(), opts)
	done(nil)

	logger.LogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}

// GetReplicationMRFHandler - returns replication MRF for bucket
func (s *peerRESTServer) GetReplicationMRFHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	ctx := newContext(r, w, "GetReplicationMRF")
	re, err := globalReplicationPool.getMRF(ctx, bucketName)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	enc := gob.NewEncoder(w)

	for m := range re {
		if err := enc.Encode(m); err != nil {
			s.writeErrorResponse(w, errors.New("Encoding mrf failed: "+err.Error()))
			return
		}
	}
}

// DevNull - everything goes to io.Discard
func (s *peerRESTServer) DevNull(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	globalNetPerfRX.Connect()
	defer globalNetPerfRX.Disconnect()

	connectTime := time.Now()
	ctx := newContext(r, w, "DevNull")
	for {
		n, err := io.CopyN(io.Discard, r.Body, 128*humanize.KiByte)
		atomic.AddUint64(&globalNetPerfRX.RX, uint64(n))
		if err != nil && err != io.EOF {
			// If there is a disconnection before globalNetPerfMinDuration (we give a margin of error of 1 sec)
			// would mean the network is not stable. Logging here will help in debugging network issues.
			if time.Since(connectTime) < (globalNetPerfMinDuration - time.Second) {
				logger.LogIf(ctx, err)
			}
		}
		if err != nil {
			break
		}
	}
}

// NetSpeedTestHandlers - perform network speedtest
func (s *peerRESTServer) NetSpeedTestHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	durationStr := r.Form.Get(peerRESTDuration)
	duration, err := time.ParseDuration(durationStr)
	if err != nil || duration.Seconds() == 0 {
		duration = time.Second * 10
	}
	result := netperf(r.Context(), duration.Round(time.Second))
	logger.LogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}

// registerPeerRESTHandlers - register peer rest router.
func registerPeerRESTHandlers(router *mux.Router) {
	h := func(f http.HandlerFunc) http.HandlerFunc {
		return collectInternodeStats(httpTraceHdrs(f))
	}

	server := &peerRESTServer{}
	subrouter := router.PathPrefix(peerRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodHealth).HandlerFunc(h(server.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocks).HandlerFunc(h(server.GetLocksHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerInfo).HandlerFunc(h(server.ServerInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLocalStorageInfo).HandlerFunc(h(server.LocalStorageInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodProcInfo).HandlerFunc(h(server.GetProcInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodMemInfo).HandlerFunc(h(server.GetMemInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodMetrics).HandlerFunc(h(server.GetMetricsHandler)).Queries(restQueries(peerRESTMetricsTypes)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodResourceMetrics).HandlerFunc(h(server.GetResourceMetrics))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysErrors).HandlerFunc(h(server.GetSysErrorsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysServices).HandlerFunc(h(server.GetSysServicesHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysConfig).HandlerFunc(h(server.GetSysConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodOsInfo).HandlerFunc(h(server.GetOSInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDiskHwInfo).HandlerFunc(h(server.GetPartitionsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCPUInfo).HandlerFunc(h(server.GetCPUsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetAllBucketStats).HandlerFunc(h(server.GetAllBucketStatsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteBucketMetadata).HandlerFunc(h(server.DeleteBucketMetadataHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadBucketMetadata).HandlerFunc(h(server.LoadBucketMetadataHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetBucketStats).HandlerFunc(h(server.GetBucketStatsHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSignalService).HandlerFunc(h(server.SignalServiceHandler)).Queries(restQueries(peerRESTSignal)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadBinary).HandlerFunc(h(server.DownloadBinaryHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCommitBinary).HandlerFunc(h(server.CommitBinaryHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeletePolicy).HandlerFunc(h(server.DeletePolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicy).HandlerFunc(h(server.LoadPolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicyMapping).HandlerFunc(h(server.LoadPolicyMappingHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteUser).HandlerFunc(h(server.DeleteUserHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteServiceAccount).HandlerFunc(h(server.DeleteServiceAccountHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadUser).HandlerFunc(h(server.LoadUserHandler)).Queries(restQueries(peerRESTUser, peerRESTUserTemp)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadServiceAccount).HandlerFunc(h(server.LoadServiceAccountHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadGroup).HandlerFunc(h(server.LoadGroupHandler)).Queries(restQueries(peerRESTGroup)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetReplicationMRF).HandlerFunc(httpTraceHdrs(server.GetReplicationMRFHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetSRMetrics).HandlerFunc(h(server.GetSRMetricsHandler))

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStartProfiling).HandlerFunc(h(server.StartProfilingHandler)).Queries(restQueries(peerRESTProfiler)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadProfilingData).HandlerFunc(h(server.DownloadProfilingDataHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTrace).HandlerFunc(server.TraceHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodListen).HandlerFunc(h(server.ListenHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBackgroundHealStatus).HandlerFunc(server.BackgroundHealStatusHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLog).HandlerFunc(server.ConsoleLogHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocalDiskIDs).HandlerFunc(h(server.GetLocalDiskIDs))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetBandwidth).HandlerFunc(h(server.GetBandwidth))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetMetacacheListing).HandlerFunc(h(server.GetMetacacheListingHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodUpdateMetacacheListing).HandlerFunc(h(server.UpdateMetacacheListingHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetPeerMetrics).HandlerFunc(h(server.GetPeerMetrics))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetPeerBucketMetrics).HandlerFunc(h(server.GetPeerBucketMetrics))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadTransitionTierConfig).HandlerFunc(h(server.LoadTransitionTierConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSpeedTest).HandlerFunc(h(server.SpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDriveSpeedTest).HandlerFunc(h(server.DriveSpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetperf).HandlerFunc(h(server.NetSpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDevNull).HandlerFunc(h(server.DevNull))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadSiteReplicationConfig).HandlerFunc(h(server.ReloadSiteReplicationConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadPoolMeta).HandlerFunc(h(server.ReloadPoolMetaHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadRebalanceMeta).HandlerFunc(h(server.LoadRebalanceMetaHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStopRebalance).HandlerFunc(h(server.StopRebalanceHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLastDayTierStats).HandlerFunc(h(server.GetLastDayTierStatsHandler))
}
