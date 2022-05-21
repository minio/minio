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
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	b "github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
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

	vars := mux.Vars(r)
	userOrGroup := vars[peerRESTUserOrGroup]
	if userOrGroup == "" {
		s.writeErrorResponse(w, errors.New("user-or-group is missing"))
		return
	}

	_, isGroup := r.Form[peerRESTIsGroup]
	if err := globalIAMSys.LoadPolicyMapping(r.Context(), objAPI, userOrGroup, isGroup); err != nil {
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

func (s *peerRESTServer) NetInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NetInfo")
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	// Use this trailer to send additional headers after sending body
	w.Header().Set("Trailer", "FinalStatus")

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	n, err := io.Copy(ioutil.Discard, r.Body)
	if err == io.ErrUnexpectedEOF {
		w.Header().Set("FinalStatus", err.Error())
		return
	}
	if err != nil && err != io.EOF {
		logger.LogIf(ctx, err)
		w.Header().Set("FinalStatus", err.Error())
		return
	}
	if n != r.ContentLength {
		err := fmt.Errorf("Subnet health: short read: expected %d found %d", r.ContentLength, n)
		logger.LogIf(ctx, err)
		w.Header().Set("FinalStatus", err.Error())
		return
	}
	w.Header().Set("FinalStatus", "Success")
}

func (s *peerRESTServer) DispatchNetInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	done := keepHTTPResponseAlive(w)

	ctx := newContext(r, w, "DispatchNetInfo")
	info := globalNotificationSys.GetNetPerfInfo(ctx)

	done(nil)
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// GetDrivePerfInfosHandler - returns all disk's serial/parallal performance information.
func (s *peerRESTServer) GetDrivePerfInfosHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "DriveInfo"))
	defer cancel()

	info := getDrivePerfInfos(ctx, r.Host)

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

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
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

	logger.LogIf(r.Context(), msgp.Encode(w, BucketStatsMap(bucketStatsMap)))
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
	}
	logger.LogIf(r.Context(), msgp.Encode(w, &bs))
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
		globalNotificationSys.AddRulesMap(bucketName, meta.notificationConfig.ToRulesMap())
	}

	if meta.bucketTargetConfig != nil {
		globalBucketTargetSys.UpdateAllTargets(bucketName, meta.bucketTargetConfig)
	}
}

// CycleServerBloomFilterHandler cycles bloom filter on server.
func (s *peerRESTServer) CycleServerBloomFilterHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "CycleServerBloomFilter")

	var req bloomFilterRequest
	err := gob.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	bf, err := intDataUpdateTracker.cycleFilter(ctx, req)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	logger.LogIf(ctx, gob.NewEncoder(w).Encode(bf))
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

	globalNotificationSys.AddRulesMap(bucketName, rulesMap)
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

// ServerUpdateHandler - updates the current server.
func (s *peerRESTServer) ServerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	var info serverUpdateInfo
	err := gob.NewDecoder(r.Body).Decode(&info)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	if _, err = updateServer(info.URL, info.Sha256Sum, info.Time, info.ReleaseInfo, getMinioMode()); err != nil {
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
	for _, ev := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(ev)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}

		eventNames = append(eventNames, eventName)
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Listen Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan interface{}, 2000)

	globalHTTPListen.Subscribe(ch, doneCh, func(evI interface{}) bool {
		ev, ok := evI.(event.Event)
		if !ok {
			return false
		}
		if ev.S3.Bucket.Name != "" && values.Get(peerRESTListenBucket) != "" {
			if ev.S3.Bucket.Name != values.Get(peerRESTListenBucket) {
				return false
			}
		}
		return rulesMap.MatchSimple(ev.EventName, ev.S3.Object.Key)
	})

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case ev := <-ch:
			if err := enc.Encode(ev); err != nil {
				return
			}
			w.(http.Flusher).Flush()
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

func extractTraceOptsFromPeerRequest(r *http.Request) (opts madmin.ServiceTraceOpts, err error) {
	opts.S3 = r.Form.Get(peerRESTTraceS3) == "true"
	opts.OS = r.Form.Get(peerRESTTraceOS) == "true"
	opts.Storage = r.Form.Get(peerRESTTraceStorage) == "true"
	opts.Internal = r.Form.Get(peerRESTTraceInternal) == "true"
	opts.OnlyErrors = r.Form.Get(peerRESTTraceErr) == "true"

	if t := r.Form.Get(peerRESTTraceThreshold); t != "" {
		d, err := time.ParseDuration(t)
		if err != nil {
			return opts, err
		}
		opts.Threshold = d
	}
	return
}

// TraceHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) TraceHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	traceOpts, err := extractTraceOptsFromPeerRequest(r)
	if err != nil {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Trace Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan interface{}, 2000)

	globalTrace.Subscribe(ch, doneCh, func(entry interface{}) bool {
		return mustTrace(entry, traceOpts)
	})

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case entry := <-ch:
			if err := enc.Encode(entry); err != nil {
				return
			}
			w.(http.Flusher).Flush()
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

	state, ok := getBackgroundHealStatus(ctx, newObjectLayerFn())
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

	ch := make(chan interface{}, 2000)
	globalConsoleSys.Subscribe(ch, doneCh, "", 0, string(logger.All), nil)

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := gob.NewEncoder(w)
	for {
		select {
		case entry := <-ch:
			if err := enc.Encode(entry); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-keepAliveTicker.C:
			if err := enc.Encode(&madmin.LogInfo{}); err != nil {
				return
			}
			w.(http.Flusher).Flush()
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
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	enc := gob.NewEncoder(w)

	ch := ReportMetrics(r.Context(), peerMetricsGroups)
	for m := range ch {
		if err := enc.Encode(m); err != nil {
			s.writeErrorResponse(w, errors.New("Encoding metric failed: "+err.Error()))
			return
		}
	}
}

func (s *peerRESTServer) SpeedtestHandler(w http.ResponseWriter, r *http.Request) {
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

	result, err := selfSpeedtest(r.Context(), size, concurrent, duration, storageClass)
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

// Netperf - perform netperf
func (s *peerRESTServer) Netperf(w http.ResponseWriter, r *http.Request) {
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
	server := &peerRESTServer{}
	subrouter := router.PathPrefix(peerRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodHealth).HandlerFunc(httpTraceHdrs(server.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocks).HandlerFunc(httpTraceHdrs(server.GetLocksHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerInfo).HandlerFunc(httpTraceHdrs(server.ServerInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodProcInfo).HandlerFunc(httpTraceHdrs(server.GetProcInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodMemInfo).HandlerFunc(httpTraceHdrs(server.GetMemInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysErrors).HandlerFunc(httpTraceHdrs(server.GetSysErrorsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysServices).HandlerFunc(httpTraceHdrs(server.GetSysServicesHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSysConfig).HandlerFunc(httpTraceHdrs(server.GetSysConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodOsInfo).HandlerFunc(httpTraceHdrs(server.GetOSInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDiskHwInfo).HandlerFunc(httpTraceHdrs(server.GetPartitionsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCPUInfo).HandlerFunc(httpTraceHdrs(server.GetCPUsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDriveInfo).HandlerFunc(httpTraceHdrs(server.GetDrivePerfInfosHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetInfo).HandlerFunc(httpTraceHdrs(server.NetInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDispatchNetInfo).HandlerFunc(httpTraceHdrs(server.DispatchNetInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCycleBloom).HandlerFunc(httpTraceHdrs(server.CycleServerBloomFilterHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetAllBucketStats).HandlerFunc(httpTraceHdrs(server.GetAllBucketStatsHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteBucketMetadata).HandlerFunc(httpTraceHdrs(server.DeleteBucketMetadataHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadBucketMetadata).HandlerFunc(httpTraceHdrs(server.LoadBucketMetadataHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetBucketStats).HandlerFunc(httpTraceHdrs(server.GetBucketStatsHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSignalService).HandlerFunc(httpTraceHdrs(server.SignalServiceHandler)).Queries(restQueries(peerRESTSignal)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerUpdate).HandlerFunc(httpTraceHdrs(server.ServerUpdateHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeletePolicy).HandlerFunc(httpTraceAll(server.DeletePolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicy).HandlerFunc(httpTraceAll(server.LoadPolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicyMapping).HandlerFunc(httpTraceAll(server.LoadPolicyMappingHandler)).Queries(restQueries(peerRESTUserOrGroup)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteUser).HandlerFunc(httpTraceAll(server.DeleteUserHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteServiceAccount).HandlerFunc(httpTraceAll(server.DeleteServiceAccountHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadUser).HandlerFunc(httpTraceAll(server.LoadUserHandler)).Queries(restQueries(peerRESTUser, peerRESTUserTemp)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadServiceAccount).HandlerFunc(httpTraceAll(server.LoadServiceAccountHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadGroup).HandlerFunc(httpTraceAll(server.LoadGroupHandler)).Queries(restQueries(peerRESTGroup)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStartProfiling).HandlerFunc(httpTraceAll(server.StartProfilingHandler)).Queries(restQueries(peerRESTProfiler)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadProfilingData).HandlerFunc(httpTraceHdrs(server.DownloadProfilingDataHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTrace).HandlerFunc(server.TraceHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodListen).HandlerFunc(httpTraceHdrs(server.ListenHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBackgroundHealStatus).HandlerFunc(server.BackgroundHealStatusHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLog).HandlerFunc(server.ConsoleLogHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocalDiskIDs).HandlerFunc(httpTraceHdrs(server.GetLocalDiskIDs))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetBandwidth).HandlerFunc(httpTraceHdrs(server.GetBandwidth))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetMetacacheListing).HandlerFunc(httpTraceHdrs(server.GetMetacacheListingHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodUpdateMetacacheListing).HandlerFunc(httpTraceHdrs(server.UpdateMetacacheListingHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetPeerMetrics).HandlerFunc(httpTraceHdrs(server.GetPeerMetrics))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadTransitionTierConfig).HandlerFunc(httpTraceHdrs(server.LoadTransitionTierConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSpeedtest).HandlerFunc(httpTraceHdrs(server.SpeedtestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDriveSpeedTest).HandlerFunc(httpTraceHdrs(server.DriveSpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetperf).HandlerFunc(httpTraceHdrs(server.Netperf))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDevNull).HandlerFunc(httpTraceHdrs(server.DevNull))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadSiteReplicationConfig).HandlerFunc(httpTraceHdrs(server.ReloadSiteReplicationConfigHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadPoolMeta).HandlerFunc(httpTraceHdrs(server.ReloadPoolMetaHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLastDayTierStats).HandlerFunc(httpTraceHdrs(server.GetLastDayTierStatsHandler))
}
