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
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/grid"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/mux"
)

// To abstract a node over network.
type peerRESTServer struct{}

var (
	// Types & Wrappers
	aoBucketInfo           = grid.NewArrayOf[*BucketInfo](func() *BucketInfo { return &BucketInfo{} })
	aoMetricsGroup         = grid.NewArrayOf[*MetricV2](func() *MetricV2 { return &MetricV2{} })
	madminBgHealState      = grid.NewJSONPool[madmin.BgHealState]()
	madminHealResultItem   = grid.NewJSONPool[madmin.HealResultItem]()
	madminCPUs             = grid.NewJSONPool[madmin.CPUs]()
	madminMemInfo          = grid.NewJSONPool[madmin.MemInfo]()
	madminNetInfo          = grid.NewJSONPool[madmin.NetInfo]()
	madminOSInfo           = grid.NewJSONPool[madmin.OSInfo]()
	madminPartitions       = grid.NewJSONPool[madmin.Partitions]()
	madminProcInfo         = grid.NewJSONPool[madmin.ProcInfo]()
	madminRealtimeMetrics  = grid.NewJSONPool[madmin.RealtimeMetrics]()
	madminServerProperties = grid.NewJSONPool[madmin.ServerProperties]()
	madminStorageInfo      = grid.NewJSONPool[madmin.StorageInfo]()
	madminSysConfig        = grid.NewJSONPool[madmin.SysConfig]()
	madminSysErrors        = grid.NewJSONPool[madmin.SysErrors]()
	madminSysServices      = grid.NewJSONPool[madmin.SysServices]()

	// Request -> Response RPC calls
	deleteBucketMetadataRPC        = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerDeleteBucketMetadata, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	deleteBucketRPC                = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerDeleteBucket, grid.NewMSS, grid.NewNoPayload)
	deletePolicyRPC                = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerDeletePolicy, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	deleteSvcActRPC                = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerDeleteServiceAccount, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	deleteUserRPC                  = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerDeleteUser, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	getAllBucketStatsRPC           = grid.NewSingleHandler[*grid.MSS, *BucketStatsMap](grid.HandlerGetAllBucketStats, grid.NewMSS, func() *BucketStatsMap { return &BucketStatsMap{} })
	getBackgroundHealStatusRPC     = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.BgHealState]](grid.HandlerBackgroundHealStatus, grid.NewMSS, madminBgHealState.NewJSON)
	getBandwidthRPC                = grid.NewSingleHandler[*grid.URLValues, *bandwidth.BucketBandwidthReport](grid.HandlerGetBandwidth, grid.NewURLValues, func() *bandwidth.BucketBandwidthReport { return &bandwidth.BucketBandwidthReport{} })
	getBucketStatsRPC              = grid.NewSingleHandler[*grid.MSS, *BucketStats](grid.HandlerGetBucketStats, grid.NewMSS, func() *BucketStats { return &BucketStats{} })
	getCPUsHandler                 = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.CPUs]](grid.HandlerGetCPUs, grid.NewMSS, madminCPUs.NewJSON)
	getLastDayTierStatsRPC         = grid.NewSingleHandler[*grid.MSS, *DailyAllTierStats](grid.HandlerGetLastDayTierStats, grid.NewMSS, func() *DailyAllTierStats { return &DailyAllTierStats{} })
	getLocksRPC                    = grid.NewSingleHandler[*grid.MSS, *localLockMap](grid.HandlerGetLocks, grid.NewMSS, func() *localLockMap { return &localLockMap{} })
	getMemInfoRPC                  = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.MemInfo]](grid.HandlerGetMemInfo, grid.NewMSS, madminMemInfo.NewJSON)
	getMetacacheListingRPC         = grid.NewSingleHandler[*listPathOptions, *metacache](grid.HandlerGetMetacacheListing, func() *listPathOptions { return &listPathOptions{} }, func() *metacache { return &metacache{} })
	getMetricsRPC                  = grid.NewSingleHandler[*grid.URLValues, *grid.JSON[madmin.RealtimeMetrics]](grid.HandlerGetMetrics, grid.NewURLValues, madminRealtimeMetrics.NewJSON)
	getNetInfoRPC                  = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.NetInfo]](grid.HandlerGetNetInfo, grid.NewMSS, madminNetInfo.NewJSON)
	getOSInfoRPC                   = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.OSInfo]](grid.HandlerGetOSInfo, grid.NewMSS, madminOSInfo.NewJSON)
	getPartitionsRPC               = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.Partitions]](grid.HandlerGetPartitions, grid.NewMSS, madminPartitions.NewJSON)
	getPeerBucketMetricsRPC        = grid.NewSingleHandler[*grid.MSS, *grid.Array[*MetricV2]](grid.HandlerGetPeerBucketMetrics, grid.NewMSS, aoMetricsGroup.New)
	getPeerMetricsRPC              = grid.NewSingleHandler[*grid.MSS, *grid.Array[*MetricV2]](grid.HandlerGetPeerMetrics, grid.NewMSS, aoMetricsGroup.New)
	getResourceMetricsRPC          = grid.NewSingleHandler[*grid.MSS, *grid.Array[*MetricV2]](grid.HandlerGetResourceMetrics, grid.NewMSS, aoMetricsGroup.New)
	getProcInfoRPC                 = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.ProcInfo]](grid.HandlerGetProcInfo, grid.NewMSS, madminProcInfo.NewJSON)
	getSRMetricsRPC                = grid.NewSingleHandler[*grid.MSS, *SRMetricsSummary](grid.HandlerGetSRMetrics, grid.NewMSS, func() *SRMetricsSummary { return &SRMetricsSummary{} })
	getSysConfigRPC                = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.SysConfig]](grid.HandlerGetSysConfig, grid.NewMSS, madminSysConfig.NewJSON)
	getSysErrorsRPC                = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.SysErrors]](grid.HandlerGetSysErrors, grid.NewMSS, madminSysErrors.NewJSON)
	getSysServicesRPC              = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.SysServices]](grid.HandlerGetSysServices, grid.NewMSS, madminSysServices.NewJSON)
	headBucketRPC                  = grid.NewSingleHandler[*grid.MSS, *VolInfo](grid.HandlerHeadBucket, grid.NewMSS, func() *VolInfo { return &VolInfo{} })
	healBucketRPC                  = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.HealResultItem]](grid.HandlerHealBucket, grid.NewMSS, madminHealResultItem.NewJSON)
	listBucketsRPC                 = grid.NewSingleHandler[*BucketOptions, *grid.Array[*BucketInfo]](grid.HandlerListBuckets, func() *BucketOptions { return &BucketOptions{} }, aoBucketInfo.New)
	loadBucketMetadataRPC          = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadBucketMetadata, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	loadGroupRPC                   = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadGroup, grid.NewMSS, grid.NewNoPayload)
	loadPolicyMappingRPC           = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadPolicyMapping, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	loadPolicyRPC                  = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadPolicy, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	loadRebalanceMetaRPC           = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadRebalanceMeta, grid.NewMSS, grid.NewNoPayload)
	loadSvcActRPC                  = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadServiceAccount, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	loadTransitionTierConfigRPC    = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadTransitionTierConfig, grid.NewMSS, grid.NewNoPayload)
	loadUserRPC                    = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerLoadUser, grid.NewMSS, grid.NewNoPayload).IgnoreNilConn()
	localStorageInfoRPC            = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.StorageInfo]](grid.HandlerStorageInfo, grid.NewMSS, madminStorageInfo.NewJSON)
	makeBucketRPC                  = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerMakeBucket, grid.NewMSS, grid.NewNoPayload)
	reloadPoolMetaRPC              = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerReloadPoolMeta, grid.NewMSS, grid.NewNoPayload)
	reloadSiteReplicationConfigRPC = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerReloadSiteReplicationConfig, grid.NewMSS, grid.NewNoPayload)
	serverInfoRPC                  = grid.NewSingleHandler[*grid.MSS, *grid.JSON[madmin.ServerProperties]](grid.HandlerServerInfo, grid.NewMSS, madminServerProperties.NewJSON)
	signalServiceRPC               = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerSignalService, grid.NewMSS, grid.NewNoPayload)
	stopRebalanceRPC               = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerStopRebalance, grid.NewMSS, grid.NewNoPayload)
	updateMetacacheListingRPC      = grid.NewSingleHandler[*metacache, *metacache](grid.HandlerUpdateMetacacheListing, func() *metacache { return &metacache{} }, func() *metacache { return &metacache{} })
	cleanupUploadIDCacheMetaRPC    = grid.NewSingleHandler[*grid.MSS, grid.NoPayload](grid.HandlerClearUploadID, grid.NewMSS, grid.NewNoPayload)

	// STREAMS
	// Set an output capacity of 100 for consoleLog and listenRPC
	// There is another buffer that will buffer events.
	consoleLogRPC = grid.NewStream[*grid.MSS, grid.NoPayload, *grid.Bytes](grid.HandlerConsoleLog, grid.NewMSS, nil, grid.NewBytes).WithOutCapacity(100)
	listenRPC     = grid.NewStream[*grid.URLValues, grid.NoPayload, *grid.Bytes](grid.HandlerListen, grid.NewURLValues, nil, grid.NewBytes).WithOutCapacity(100)
)

// GetLocksHandler - returns list of lock from the server.
func (s *peerRESTServer) GetLocksHandler(_ *grid.MSS) (*localLockMap, *grid.RemoteErr) {
	res := globalLockServer.DupLockMap()
	return &res, nil
}

// DeletePolicyHandler - deletes a policy on the server.
func (s *peerRESTServer) DeletePolicyHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	policyName := mss.Get(peerRESTPolicy)
	if policyName == "" {
		return np, grid.NewRemoteErr(errors.New("policyName is missing"))
	}

	if err := globalIAMSys.DeletePolicy(context.Background(), policyName, false); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// LoadPolicyHandler - reloads a policy on the server.
func (s *peerRESTServer) LoadPolicyHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	policyName := mss.Get(peerRESTPolicy)
	if policyName == "" {
		return np, grid.NewRemoteErr(errors.New("policyName is missing"))
	}

	if err := globalIAMSys.LoadPolicy(context.Background(), objAPI, policyName); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// LoadPolicyMappingHandler - reloads a policy mapping on the server.
func (s *peerRESTServer) LoadPolicyMappingHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}
	userOrGroup := mss.Get(peerRESTUserOrGroup)
	if userOrGroup == "" {
		return np, grid.NewRemoteErr(errors.New("user-or-group is missing"))
	}

	userType, err := strconv.Atoi(mss.Get(peerRESTUserType))
	if err != nil {
		return np, grid.NewRemoteErr(fmt.Errorf("user-type `%s` is invalid: %w", mss.Get(peerRESTUserType), err))
	}

	isGroup := mss.Get(peerRESTIsGroup) == "true"
	if err := globalIAMSys.LoadPolicyMapping(context.Background(), objAPI, userOrGroup, IAMUserType(userType), isGroup); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// DeleteServiceAccountHandler - deletes a service account on the server.
func (s *peerRESTServer) DeleteServiceAccountHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	accessKey := mss.Get(peerRESTUser)
	if accessKey == "" {
		return np, grid.NewRemoteErr(errors.New("service account name is missing"))
	}

	if err := globalIAMSys.DeleteServiceAccount(context.Background(), accessKey, false); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// LoadServiceAccountHandler - reloads a service account on the server.
func (s *peerRESTServer) LoadServiceAccountHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	accessKey := mss.Get(peerRESTUser)
	if accessKey == "" {
		return np, grid.NewRemoteErr(errors.New("service account name is missing"))
	}

	if err := globalIAMSys.LoadServiceAccount(context.Background(), accessKey); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// DeleteUserHandler - deletes a user on the server.
func (s *peerRESTServer) DeleteUserHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	accessKey := mss.Get(peerRESTUser)
	if accessKey == "" {
		return np, grid.NewRemoteErr(errors.New("username is missing"))
	}

	if err := globalIAMSys.DeleteUser(context.Background(), accessKey, false); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// LoadUserHandler - reloads a user on the server.
func (s *peerRESTServer) LoadUserHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	accessKey := mss.Get(peerRESTUser)
	if accessKey == "" {
		return np, grid.NewRemoteErr(errors.New("username is missing"))
	}

	temp, err := strconv.ParseBool(mss.Get(peerRESTUserTemp))
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	userType := regUser
	if temp {
		userType = stsUser
	}

	if err = globalIAMSys.LoadUser(context.Background(), objAPI, accessKey, userType); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

// LoadGroupHandler - reloads group along with members list.
func (s *peerRESTServer) LoadGroupHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	group := mss.Get(peerRESTGroup)
	if group == "" {
		return np, grid.NewRemoteErr(errors.New("group is missing"))
	}

	err := globalIAMSys.LoadGroup(context.Background(), objAPI, group)
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
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
	peersLogIf(ctx, gob.NewEncoder(w).Encode(profileData))
}

func (s *peerRESTServer) LocalStorageInfoHandler(mss *grid.MSS) (*grid.JSON[madmin.StorageInfo], *grid.RemoteErr) {
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		return nil, grid.NewRemoteErr(errServerNotInitialized)
	}

	metrics, err := strconv.ParseBool(mss.Get(peerRESTMetrics))
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	info := objLayer.LocalStorageInfo(context.Background(), metrics)
	return madminStorageInfo.NewJSONWith(&info), nil
}

// ServerInfoHandler - returns Server Info
func (s *peerRESTServer) ServerInfoHandler(params *grid.MSS) (*grid.JSON[madmin.ServerProperties], *grid.RemoteErr) {
	r := http.Request{Host: globalLocalNodeName}
	metrics, err := strconv.ParseBool(params.Get(peerRESTMetrics))
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	info := getLocalServerProperty(globalEndpoints, &r, metrics)
	return madminServerProperties.NewJSONWith(&info), nil
}

// GetCPUsHandler - returns CPU info.
func (s *peerRESTServer) GetCPUsHandler(_ *grid.MSS) (*grid.JSON[madmin.CPUs], *grid.RemoteErr) {
	info := madmin.GetCPUs(context.Background(), globalLocalNodeName)
	return madminCPUs.NewJSONWith(&info), nil
}

// GetNetInfoHandler - returns network information.
func (s *peerRESTServer) GetNetInfoHandler(_ *grid.MSS) (*grid.JSON[madmin.NetInfo], *grid.RemoteErr) {
	info := madmin.GetNetInfo(globalLocalNodeName, globalInternodeInterface)
	return madminNetInfo.NewJSONWith(&info), nil
}

// GetPartitionsHandler - returns disk partition information.
func (s *peerRESTServer) GetPartitionsHandler(_ *grid.MSS) (*grid.JSON[madmin.Partitions], *grid.RemoteErr) {
	info := madmin.GetPartitions(context.Background(), globalLocalNodeName)
	return madminPartitions.NewJSONWith(&info), nil
}

// GetOSInfoHandler - returns operating system's information.
func (s *peerRESTServer) GetOSInfoHandler(_ *grid.MSS) (*grid.JSON[madmin.OSInfo], *grid.RemoteErr) {
	info := madmin.GetOSInfo(context.Background(), globalLocalNodeName)
	return madminOSInfo.NewJSONWith(&info), nil
}

// GetProcInfoHandler - returns this MinIO process information.
func (s *peerRESTServer) GetProcInfoHandler(_ *grid.MSS) (*grid.JSON[madmin.ProcInfo], *grid.RemoteErr) {
	info := madmin.GetProcInfo(context.Background(), globalLocalNodeName)
	return madminProcInfo.NewJSONWith(&info), nil
}

// GetMemInfoHandler - returns memory information.
func (s *peerRESTServer) GetMemInfoHandler(_ *grid.MSS) (*grid.JSON[madmin.MemInfo], *grid.RemoteErr) {
	info := madmin.GetMemInfo(context.Background(), globalLocalNodeName)
	return madminMemInfo.NewJSONWith(&info), nil
}

// GetMetricsHandler - returns server metrics.
func (s *peerRESTServer) GetMetricsHandler(v *grid.URLValues) (*grid.JSON[madmin.RealtimeMetrics], *grid.RemoteErr) {
	values := v.Values()
	var types madmin.MetricType
	if t, _ := strconv.ParseUint(values.Get(peerRESTMetricsTypes), 10, 64); t != 0 {
		types = madmin.MetricType(t)
	} else {
		types = madmin.MetricsAll
	}

	diskMap := make(map[string]struct{})
	for _, disk := range values[peerRESTDisk] {
		diskMap[disk] = struct{}{}
	}

	hostMap := make(map[string]struct{})
	for _, host := range values[peerRESTHost] {
		hostMap[host] = struct{}{}
	}

	info := collectLocalMetrics(types, collectMetricsOpts{
		disks: diskMap,
		hosts: hostMap,
		jobID: values.Get(peerRESTJobID),
		depID: values.Get(peerRESTDepID),
	})
	return madminRealtimeMetrics.NewJSONWith(&info), nil
}

// GetSysConfigHandler - returns system config information.
// (only the config that are of concern to minio)
func (s *peerRESTServer) GetSysConfigHandler(_ *grid.MSS) (*grid.JSON[madmin.SysConfig], *grid.RemoteErr) {
	info := madmin.GetSysConfig(context.Background(), globalLocalNodeName)
	return madminSysConfig.NewJSONWith(&info), nil
}

// GetSysServicesHandler - returns system services information.
// (only the services that are of concern to minio)
func (s *peerRESTServer) GetSysServicesHandler(_ *grid.MSS) (*grid.JSON[madmin.SysServices], *grid.RemoteErr) {
	info := madmin.GetSysServices(context.Background(), globalLocalNodeName)
	return madminSysServices.NewJSONWith(&info), nil
}

// GetSysErrorsHandler - returns system level errors
func (s *peerRESTServer) GetSysErrorsHandler(_ *grid.MSS) (*grid.JSON[madmin.SysErrors], *grid.RemoteErr) {
	info := madmin.GetSysErrors(context.Background(), globalLocalNodeName)
	return madminSysErrors.NewJSONWith(&info), nil
}

// DeleteBucketMetadataHandler - Delete in memory bucket metadata
func (s *peerRESTServer) DeleteBucketMetadataHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	bucketName := mss.Get(peerRESTBucket)
	if bucketName == "" {
		return np, grid.NewRemoteErr(errors.New("Bucket name is missing"))
	}

	globalReplicationStats.Load().Delete(bucketName)
	globalBucketMetadataSys.Remove(bucketName)
	globalBucketTargetSys.Delete(bucketName)
	globalEventNotifier.RemoveNotification(bucketName)
	globalBucketConnStats.delete(bucketName)
	globalBucketHTTPStats.delete(bucketName)
	if localMetacacheMgr != nil {
		localMetacacheMgr.deleteBucketCache(bucketName)
	}
	return np, nerr
}

// GetAllBucketStatsHandler - fetches bucket replication stats for all buckets from this peer.
func (s *peerRESTServer) GetAllBucketStatsHandler(mss *grid.MSS) (*BucketStatsMap, *grid.RemoteErr) {
	replicationStats := globalReplicationStats.Load().GetAll()
	bucketStatsMap := make(map[string]BucketStats, len(replicationStats))
	for k, v := range replicationStats {
		bucketStatsMap[k] = BucketStats{
			ReplicationStats: v,
			ProxyStats:       globalReplicationStats.Load().getProxyStats(k),
		}
	}
	return &BucketStatsMap{Stats: bucketStatsMap, Timestamp: time.Now()}, nil
}

// GetBucketStatsHandler - fetches current in-memory bucket stats, currently only
// returns BucketStats, that currently includes ReplicationStats.
func (s *peerRESTServer) GetBucketStatsHandler(vars *grid.MSS) (*BucketStats, *grid.RemoteErr) {
	bucketName := vars.Get(peerRESTBucket)
	if bucketName == "" {
		return nil, grid.NewRemoteErrString("Bucket name is missing")
	}
	st := globalReplicationStats.Load()
	if st == nil {
		return &BucketStats{}, nil
	}
	bs := BucketStats{
		ReplicationStats: st.Get(bucketName),
		QueueStats:       ReplicationQueueStats{Nodes: []ReplQNodeStats{st.getNodeQueueStats(bucketName)}},
		ProxyStats:       st.getProxyStats(bucketName),
	}
	return &bs, nil
}

// GetSRMetricsHandler - fetches current in-memory replication stats at site level from this peer
func (s *peerRESTServer) GetSRMetricsHandler(mss *grid.MSS) (*SRMetricsSummary, *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return nil, grid.NewRemoteErr(errServerNotInitialized)
	}
	if st := globalReplicationStats.Load(); st != nil {
		sm := st.getSRMetricsForNode()
		return &sm, nil
	}
	return &SRMetricsSummary{}, nil
}

// LoadBucketMetadataHandler - reloads in memory bucket metadata
func (s *peerRESTServer) LoadBucketMetadataHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	bucketName := mss.Get(peerRESTBucket)
	if bucketName == "" {
		return np, grid.NewRemoteErr(errors.New("Bucket name is missing"))
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	meta, err := loadBucketMetadata(context.Background(), objAPI, bucketName)
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	globalBucketMetadataSys.Set(bucketName, meta)

	if meta.notificationConfig != nil {
		globalEventNotifier.AddRulesMap(bucketName, meta.notificationConfig.ToRulesMap())
	}

	if meta.bucketTargetConfig != nil {
		globalBucketTargetSys.UpdateAllTargets(bucketName, meta.bucketTargetConfig)
	}

	return np, nerr
}

func (s *peerRESTServer) GetMetacacheListingHandler(opts *listPathOptions) (*metacache, *grid.RemoteErr) {
	resp := localMetacacheMgr.getBucket(context.Background(), opts.Bucket).findCache(*opts)
	return &resp, nil
}

func (s *peerRESTServer) UpdateMetacacheListingHandler(req *metacache) (*metacache, *grid.RemoteErr) {
	cache, err := localMetacacheMgr.updateCacheEntry(*req)
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	return &cache, nil
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

// HealthHandler - returns true of health
func (s *peerRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	s.IsValid(w, r)
}

// VerifyBinary - verifies the downloaded binary is in-tact
func (s *peerRESTServer) VerifyBinaryHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	u, err := url.Parse(r.Form.Get(peerRESTURL))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	sha256Sum, err := hex.DecodeString(r.Form.Get(peerRESTSha256Sum))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	releaseInfo := r.Form.Get(peerRESTReleaseInfo)

	lrTime, err := releaseInfoToReleaseTime(releaseInfo)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	if lrTime.Sub(currentReleaseTime) <= 0 {
		s.writeErrorResponse(w, fmt.Errorf("server is running the latest version: %s", Version))
		return
	}

	zr, err := zstd.NewReader(r.Body)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer zr.Close()

	if err = verifyBinary(u, sha256Sum, releaseInfo, getMinioMode(), zr); err != nil {
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

func waitingDrivesNode() map[string]madmin.DiskMetrics {
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	errs := make([]error, len(localDrives))
	infos := make([]DiskInfo, len(localDrives))
	for i, drive := range localDrives {
		infos[i], errs[i] = drive.DiskInfo(GlobalContext, DiskInfoOptions{})
	}
	infoMaps := make(map[string]madmin.DiskMetrics)
	for i := range infos {
		if infos[i].Metrics.TotalWaiting >= 1 && errors.Is(errs[i], errFaultyDisk) {
			infoMaps[infos[i].Endpoint] = madmin.DiskMetrics{
				TotalWaiting: infos[i].Metrics.TotalWaiting,
			}
		}
	}
	return infoMaps
}

// SignalServiceHandler - signal service handler.
func (s *peerRESTServer) SignalServiceHandler(vars *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	signalString := vars.Get(peerRESTSignal)
	if signalString == "" {
		return np, grid.NewRemoteErrString("signal name is missing")
	}
	si, err := strconv.Atoi(signalString)
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	// Wait until the specified time before executing the signal.
	if t := vars.Get(peerRESTExecAt); t != "" {
		execAt, err := time.Parse(time.RFC3339Nano, vars.Get(peerRESTExecAt))
		if err != nil {
			logger.LogIf(GlobalContext, "signalservice", err)
			execAt = time.Now().Add(restartUpdateDelay)
		}
		if d := time.Until(execAt); d > 0 {
			time.Sleep(d)
		}
	}
	signal := serviceSignal(si)
	switch signal {
	case serviceRestart, serviceStop:
		dryRun := vars.Get("dry-run") == "true" // This is only supported for `restart/stop`

		waitingDisks := waitingDrivesNode()
		if len(waitingDisks) > 0 {
			buf, err := json.Marshal(waitingDisks)
			if err != nil {
				return np, grid.NewRemoteErr(err)
			}
			return np, grid.NewRemoteErrString(string(buf))
		}
		if !dryRun {
			globalServiceSignalCh <- signal
		}
	case serviceFreeze:
		freezeServices()
	case serviceUnFreeze:
		unfreezeServices()
	case serviceReloadDynamic:
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return np, grid.NewRemoteErr(errServerNotInitialized)
		}
		srvCfg, err := getValidConfig(objAPI)
		if err != nil {
			return np, grid.NewRemoteErr(err)
		}
		subSys := vars.Get(peerRESTSubSys)
		// Apply dynamic values.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if subSys == "" {
			err = applyDynamicConfig(ctx, objAPI, srvCfg)
		} else {
			err = applyDynamicConfigForSubSys(ctx, objAPI, srvCfg, subSys)
		}
		if err != nil {
			return np, grid.NewRemoteErr(err)
		}
	default:
		return np, grid.NewRemoteErr(errUnsupportedSignal)
	}
	return np, nil
}

// ListenHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) ListenHandler(ctx context.Context, v *grid.URLValues, out chan<- *grid.Bytes) *grid.RemoteErr {
	values := v.Values()
	defer v.Recycle()
	var prefix string
	if len(values[peerRESTListenPrefix]) > 1 {
		return grid.NewRemoteErrString("invalid request (peerRESTListenPrefix)")
	}
	globalAPIConfig.getRequestsPoolCapacity()
	if len(values[peerRESTListenPrefix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenPrefix][0]); err != nil {
			return grid.NewRemoteErr(err)
		}

		prefix = values[peerRESTListenPrefix][0]
	}

	var suffix string
	if len(values[peerRESTListenSuffix]) > 1 {
		return grid.NewRemoteErrString("invalid request (peerRESTListenSuffix)")
	}

	if len(values[peerRESTListenSuffix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenSuffix][0]); err != nil {
			return grid.NewRemoteErr(err)
		}

		suffix = values[peerRESTListenSuffix][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	var eventNames []event.Name
	var mask pubsub.Mask
	for _, ev := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(ev)
		if err != nil {
			return grid.NewRemoteErr(err)
		}
		mask.MergeMaskable(eventName)
		eventNames = append(eventNames, eventName)
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	// Listen Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan event.Event, globalAPIConfig.getRequestsPoolCapacity())
	err := globalHTTPListen.Subscribe(mask, ch, ctx.Done(), func(ev event.Event) bool {
		if ev.S3.Bucket.Name != "" && values.Get(peerRESTListenBucket) != "" {
			if ev.S3.Bucket.Name != values.Get(peerRESTListenBucket) {
				return false
			}
		}
		return rulesMap.MatchSimple(ev.EventName, ev.S3.Object.Key)
	})
	if err != nil {
		return grid.NewRemoteErr(err)
	}

	// Process until remote disconnects.
	// Blocks on upstream (out) congestion.
	// We have however a dynamic downstream buffer (ch).
	buf := bytes.NewBuffer(grid.GetByteBuffer())
	enc := json.NewEncoder(buf)
	tmpEvt := struct{ Records []event.Event }{[]event.Event{{}}}
	for {
		select {
		case <-ctx.Done():
			grid.PutByteBuffer(buf.Bytes())
			return nil
		case ev := <-ch:
			buf.Reset()
			tmpEvt.Records[0] = ev
			if err := enc.Encode(tmpEvt); err != nil {
				peersLogOnceIf(ctx, err, "event: Encode failed")
				continue
			}
			out <- grid.NewBytesWithCopyOf(buf.Bytes())
		}
	}
}

// TraceHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) TraceHandler(ctx context.Context, payload []byte, _ <-chan []byte, out chan<- []byte) *grid.RemoteErr {
	var traceOpts madmin.ServiceTraceOpts
	err := json.Unmarshal(payload, &traceOpts)
	if err != nil {
		return grid.NewRemoteErr(err)
	}
	var wg sync.WaitGroup

	// Trace Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	err = globalTrace.SubscribeJSON(traceOpts.TraceTypes(), out, ctx.Done(), func(entry madmin.TraceInfo) bool {
		return shouldTrace(entry, traceOpts)
	}, &wg)
	if err != nil {
		return grid.NewRemoteErr(err)
	}

	// Publish bootstrap events that have already occurred before client could subscribe.
	if traceOpts.TraceTypes().Contains(madmin.TraceBootstrap) {
		go globalBootstrapTracer.Publish(ctx, globalTrace)
	}

	// Wait for remote to cancel and SubscribeJSON to exit.
	wg.Wait()
	return nil
}

func (s *peerRESTServer) BackgroundHealStatusHandler(_ *grid.MSS) (*grid.JSON[madmin.BgHealState], *grid.RemoteErr) {
	state, ok := getLocalBackgroundHealStatus(context.Background(), newObjectLayerFn())
	if !ok {
		return nil, grid.NewRemoteErr(errServerNotInitialized)
	}
	return madminBgHealState.NewJSONWith(&state), nil
}

// ReloadSiteReplicationConfigHandler - reloads site replication configuration from the disks
func (s *peerRESTServer) ReloadSiteReplicationConfigHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	peersLogIf(context.Background(), globalSiteReplicationSys.Init(context.Background(), objAPI))
	return np, nerr
}

func (s *peerRESTServer) ReloadPoolMetaHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		return np, nerr
	}

	if err := pools.ReloadPoolMeta(context.Background()); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nerr
}

func (s *peerRESTServer) HandlerClearUploadID(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		return np, nerr
	}

	// No need to return errors, this is not a highly strict operation.
	uploadID := mss.Get(peerRESTUploadID)
	if uploadID != "" {
		pools.ClearUploadID(uploadID)
	}

	return np, nerr
}

func (s *peerRESTServer) StopRebalanceHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		return np, grid.NewRemoteErr(errors.New("not a pooled setup"))
	}

	pools.StopRebalance()
	return np, nerr
}

func (s *peerRESTServer) LoadRebalanceMetaHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		return np, grid.NewRemoteErr(errors.New("not a pooled setup"))
	}

	startRebalance, err := strconv.ParseBool(mss.Get(peerRESTStartRebalance))
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	if err := pools.loadRebalanceMeta(context.Background()); err != nil {
		return np, grid.NewRemoteErr(err)
	}

	if startRebalance {
		go pools.StartRebalance()
	}

	return np, nerr
}

func (s *peerRESTServer) LoadTransitionTierConfigHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return np, grid.NewRemoteErr(errServerNotInitialized)
	}

	go func() {
		err := globalTierConfigMgr.Reload(context.Background(), newObjectLayerFn())
		if err != nil {
			peersLogIf(context.Background(), fmt.Errorf("Failed to reload remote tier config %s", err))
		}
	}()

	return np, nerr
}

// ConsoleLogHandler sends console logs of this node back to peer rest client
func (s *peerRESTServer) ConsoleLogHandler(ctx context.Context, params *grid.MSS, out chan<- *grid.Bytes) *grid.RemoteErr {
	mask, err := strconv.Atoi(params.Get(peerRESTLogMask))
	if err != nil {
		mask = int(madmin.LogMaskAll)
	}
	ch := make(chan log.Info, 1000)
	err = globalConsoleSys.Subscribe(ch, ctx.Done(), "", 0, madmin.LogMask(mask), nil)
	if err != nil {
		return grid.NewRemoteErr(err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return grid.NewRemoteErrString("console log channel closed")
			}
			if !entry.SendLog("", madmin.LogMask(mask)) {
				continue
			}
			buf.Reset()
			if err := enc.Encode(entry); err != nil {
				return grid.NewRemoteErr(err)
			}
			out <- grid.NewBytesWithCopyOf(buf.Bytes())
		case <-ctx.Done():
			return grid.NewRemoteErr(ctx.Err())
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
func (s *peerRESTServer) GetBandwidth(params *grid.URLValues) (*bandwidth.BucketBandwidthReport, *grid.RemoteErr) {
	buckets := params.Values().Get("buckets")
	selectBuckets := bandwidth.SelectBuckets(buckets)
	return globalBucketMonitor.GetReport(selectBuckets), nil
}

func (s *peerRESTServer) GetResourceMetrics(_ *grid.MSS) (*grid.Array[*MetricV2], *grid.RemoteErr) {
	res := make([]*MetricV2, 0, len(resourceMetricsGroups))
	populateAndPublish(resourceMetricsGroups, func(m MetricV2) bool {
		if m.VariableLabels == nil {
			m.VariableLabels = make(map[string]string, 1)
		}
		m.VariableLabels[serverName] = globalLocalNodeName
		res = append(res, &m)
		return true
	})
	return aoMetricsGroup.NewWith(res), nil
}

// GetPeerMetrics gets the metrics to be federated across peers.
func (s *peerRESTServer) GetPeerMetrics(_ *grid.MSS) (*grid.Array[*MetricV2], *grid.RemoteErr) {
	res := make([]*MetricV2, 0, len(peerMetricsGroups))
	populateAndPublish(peerMetricsGroups, func(m MetricV2) bool {
		if m.VariableLabels == nil {
			m.VariableLabels = make(map[string]string, 1)
		}
		m.VariableLabels[serverName] = globalLocalNodeName
		res = append(res, &m)
		return true
	})
	return aoMetricsGroup.NewWith(res), nil
}

// GetPeerBucketMetrics gets the metrics to be federated across peers.
func (s *peerRESTServer) GetPeerBucketMetrics(_ *grid.MSS) (*grid.Array[*MetricV2], *grid.RemoteErr) {
	res := make([]*MetricV2, 0, len(bucketPeerMetricsGroups))
	populateAndPublish(bucketPeerMetricsGroups, func(m MetricV2) bool {
		if m.VariableLabels == nil {
			m.VariableLabels = make(map[string]string, 1)
		}
		m.VariableLabels[serverName] = globalLocalNodeName
		res = append(res, &m)
		return true
	})
	return aoMetricsGroup.NewWith(res), nil
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
	enableSha256 := r.Form.Get(peerRESTEnableSha256) == "true"
	enableMultipart := r.Form.Get(peerRESTEnableMultipart) == "true"

	u, ok := globalIAMSys.GetUser(r.Context(), r.Form.Get(peerRESTAccessKey))
	if !ok {
		s.writeErrorResponse(w, errAuthentication)
		return
	}

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
		objectSize:      size,
		concurrency:     concurrent,
		duration:        duration,
		storageClass:    storageClass,
		bucketName:      bucketName,
		enableSha256:    enableSha256,
		enableMultipart: enableMultipart,
		creds:           u.Credentials,
	})
	if err != nil {
		result.Error = err.Error()
	}

	done(nil)
	peersLogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}

// GetLastDayTierStatsHandler - returns per-tier stats in the last 24hrs for this server
func (s *peerRESTServer) GetLastDayTierStatsHandler(_ *grid.MSS) (*DailyAllTierStats, *grid.RemoteErr) {
	if objAPI := newObjectLayerFn(); objAPI == nil || globalTransitionState == nil {
		return nil, grid.NewRemoteErr(errServerNotInitialized)
	}

	result := globalTransitionState.getDailyAllTierStats()
	return &result, nil
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

	peersLogIf(r.Context(), gob.NewEncoder(w).Encode(result))
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
	re, err := globalReplicationPool.Get().getMRF(ctx, bucketName)
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
		n, err := io.CopyN(xioutil.Discard, r.Body, 128*humanize.KiByte)
		atomic.AddUint64(&globalNetPerfRX.RX, uint64(n))
		if err != nil && err != io.EOF {
			// If there is a disconnection before globalNetPerfMinDuration (we give a margin of error of 1 sec)
			// would mean the network is not stable. Logging here will help in debugging network issues.
			if time.Since(connectTime) < (globalNetPerfMinDuration - time.Second) {
				peersLogIf(ctx, err)
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
	peersLogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}

func (s *peerRESTServer) HealBucketHandler(mss *grid.MSS) (np *grid.JSON[madmin.HealResultItem], nerr *grid.RemoteErr) {
	bucket := mss.Get(peerS3Bucket)
	if isMinioMetaBucket(bucket) {
		return np, grid.NewRemoteErr(errInvalidArgument)
	}

	bucketDeleted := mss.Get(peerS3BucketDeleted) == "true"
	res, err := healBucketLocal(context.Background(), bucket, madmin.HealOpts{
		Remove: bucketDeleted,
	})
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return madminHealResultItem.NewJSONWith(&res), nil
}

func (s *peerRESTServer) ListBucketsHandler(opts *BucketOptions) (*grid.Array[*BucketInfo], *grid.RemoteErr) {
	buckets, err := listBucketsLocal(context.Background(), *opts)
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	res := aoBucketInfo.New()
	for i := range buckets {
		bucket := buckets[i]
		res.Append(&bucket)
	}
	return res, nil
}

// HeadBucketHandler implements peer BucketInfo call, returns bucket create date.
func (s *peerRESTServer) HeadBucketHandler(mss *grid.MSS) (info *VolInfo, nerr *grid.RemoteErr) {
	bucket := mss.Get(peerS3Bucket)
	if isMinioMetaBucket(bucket) {
		return info, grid.NewRemoteErr(errInvalidArgument)
	}

	bucketDeleted := mss.Get(peerS3BucketDeleted) == "true"

	bucketInfo, err := getBucketInfoLocal(context.Background(), bucket, BucketOptions{
		Deleted: bucketDeleted,
	})
	if err != nil {
		return info, grid.NewRemoteErr(err)
	}

	return &VolInfo{
		Name:    bucketInfo.Name,
		Created: bucketInfo.Created,
		Deleted: bucketInfo.Deleted, // needed for site replication
	}, nil
}

// DeleteBucketHandler implements peer delete bucket call.
func (s *peerRESTServer) DeleteBucketHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	bucket := mss.Get(peerS3Bucket)
	if isMinioMetaBucket(bucket) {
		return np, grid.NewRemoteErr(errInvalidArgument)
	}

	forceDelete := mss.Get(peerS3BucketForceDelete) == "true"
	err := deleteBucketLocal(context.Background(), bucket, DeleteBucketOptions{
		Force: forceDelete,
	})
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nil
}

// MakeBucketHandler implements peer create bucket call.
func (s *peerRESTServer) MakeBucketHandler(mss *grid.MSS) (np grid.NoPayload, nerr *grid.RemoteErr) {
	bucket := mss.Get(peerS3Bucket)
	if isMinioMetaBucket(bucket) {
		return np, grid.NewRemoteErr(errInvalidArgument)
	}

	forceCreate := mss.Get(peerS3BucketForceCreate) == "true"
	err := makeBucketLocal(context.Background(), bucket, MakeBucketOptions{
		ForceCreate: forceCreate,
	})
	if err != nil {
		return np, grid.NewRemoteErr(err)
	}

	return np, nil
}

// registerPeerRESTHandlers - register peer rest router.
func registerPeerRESTHandlers(router *mux.Router, gm *grid.Manager) {
	h := func(f http.HandlerFunc) http.HandlerFunc {
		return collectInternodeStats(httpTraceHdrs(f))
	}

	server := &peerRESTServer{}
	subrouter := router.PathPrefix(peerRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodHealth).HandlerFunc(h(server.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodVerifyBinary).HandlerFunc(h(server.VerifyBinaryHandler)).Queries(restQueries(peerRESTURL, peerRESTSha256Sum, peerRESTReleaseInfo)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCommitBinary).HandlerFunc(h(server.CommitBinaryHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetReplicationMRF).HandlerFunc(httpTraceHdrs(server.GetReplicationMRFHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStartProfiling).HandlerFunc(h(server.StartProfilingHandler)).Queries(restQueries(peerRESTProfiler)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadProfilingData).HandlerFunc(h(server.DownloadProfilingDataHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSpeedTest).HandlerFunc(h(server.SpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDriveSpeedTest).HandlerFunc(h(server.DriveSpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetperf).HandlerFunc(h(server.NetSpeedTestHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDevNull).HandlerFunc(h(server.DevNull))

	logger.FatalIf(consoleLogRPC.RegisterNoInput(gm, server.ConsoleLogHandler), "unable to register handler")
	logger.FatalIf(deleteBucketMetadataRPC.Register(gm, server.DeleteBucketMetadataHandler), "unable to register handler")
	logger.FatalIf(deleteBucketRPC.Register(gm, server.DeleteBucketHandler), "unable to register handler")
	logger.FatalIf(deletePolicyRPC.Register(gm, server.DeletePolicyHandler), "unable to register handler")
	logger.FatalIf(deleteSvcActRPC.Register(gm, server.DeleteServiceAccountHandler), "unable to register handler")
	logger.FatalIf(deleteUserRPC.Register(gm, server.DeleteUserHandler), "unable to register handler")
	logger.FatalIf(getAllBucketStatsRPC.Register(gm, server.GetAllBucketStatsHandler), "unable to register handler")
	logger.FatalIf(getBackgroundHealStatusRPC.Register(gm, server.BackgroundHealStatusHandler), "unable to register handler")
	logger.FatalIf(getBandwidthRPC.Register(gm, server.GetBandwidth), "unable to register handler")
	logger.FatalIf(getBucketStatsRPC.Register(gm, server.GetBucketStatsHandler), "unable to register handler")
	logger.FatalIf(getCPUsHandler.Register(gm, server.GetCPUsHandler), "unable to register handler")
	logger.FatalIf(getLastDayTierStatsRPC.Register(gm, server.GetLastDayTierStatsHandler), "unable to register handler")
	logger.FatalIf(getLocksRPC.Register(gm, server.GetLocksHandler), "unable to register handler")
	logger.FatalIf(getMemInfoRPC.Register(gm, server.GetMemInfoHandler), "unable to register handler")
	logger.FatalIf(getMetacacheListingRPC.Register(gm, server.GetMetacacheListingHandler), "unable to register handler")
	logger.FatalIf(getMetricsRPC.Register(gm, server.GetMetricsHandler), "unable to register handler")
	logger.FatalIf(getNetInfoRPC.Register(gm, server.GetNetInfoHandler), "unable to register handler")
	logger.FatalIf(getOSInfoRPC.Register(gm, server.GetOSInfoHandler), "unable to register handler")
	logger.FatalIf(getPartitionsRPC.Register(gm, server.GetPartitionsHandler), "unable to register handler")
	logger.FatalIf(getPeerBucketMetricsRPC.Register(gm, server.GetPeerBucketMetrics), "unable to register handler")
	logger.FatalIf(getPeerMetricsRPC.Register(gm, server.GetPeerMetrics), "unable to register handler")
	logger.FatalIf(getProcInfoRPC.Register(gm, server.GetProcInfoHandler), "unable to register handler")
	logger.FatalIf(getResourceMetricsRPC.Register(gm, server.GetResourceMetrics), "unable to register handler")
	logger.FatalIf(getSRMetricsRPC.Register(gm, server.GetSRMetricsHandler), "unable to register handler")
	logger.FatalIf(getSysConfigRPC.Register(gm, server.GetSysConfigHandler), "unable to register handler")
	logger.FatalIf(getSysErrorsRPC.Register(gm, server.GetSysErrorsHandler), "unable to register handler")
	logger.FatalIf(getSysServicesRPC.Register(gm, server.GetSysServicesHandler), "unable to register handler")
	logger.FatalIf(headBucketRPC.Register(gm, server.HeadBucketHandler), "unable to register handler")
	logger.FatalIf(healBucketRPC.Register(gm, server.HealBucketHandler), "unable to register handler")
	logger.FatalIf(listBucketsRPC.Register(gm, server.ListBucketsHandler), "unable to register handler")
	logger.FatalIf(listenRPC.RegisterNoInput(gm, server.ListenHandler), "unable to register handler")
	logger.FatalIf(loadBucketMetadataRPC.Register(gm, server.LoadBucketMetadataHandler), "unable to register handler")
	logger.FatalIf(loadGroupRPC.Register(gm, server.LoadGroupHandler), "unable to register handler")
	logger.FatalIf(loadPolicyMappingRPC.Register(gm, server.LoadPolicyMappingHandler), "unable to register handler")
	logger.FatalIf(loadPolicyRPC.Register(gm, server.LoadPolicyHandler), "unable to register handler")
	logger.FatalIf(loadRebalanceMetaRPC.Register(gm, server.LoadRebalanceMetaHandler), "unable to register handler")
	logger.FatalIf(loadSvcActRPC.Register(gm, server.LoadServiceAccountHandler), "unable to register handler")
	logger.FatalIf(loadTransitionTierConfigRPC.Register(gm, server.LoadTransitionTierConfigHandler), "unable to register handler")
	logger.FatalIf(loadUserRPC.Register(gm, server.LoadUserHandler), "unable to register handler")
	logger.FatalIf(localStorageInfoRPC.Register(gm, server.LocalStorageInfoHandler), "unable to register handler")
	logger.FatalIf(makeBucketRPC.Register(gm, server.MakeBucketHandler), "unable to register handler")
	logger.FatalIf(reloadPoolMetaRPC.Register(gm, server.ReloadPoolMetaHandler), "unable to register handler")
	logger.FatalIf(reloadSiteReplicationConfigRPC.Register(gm, server.ReloadSiteReplicationConfigHandler), "unable to register handler")
	logger.FatalIf(serverInfoRPC.Register(gm, server.ServerInfoHandler), "unable to register handler")
	logger.FatalIf(signalServiceRPC.Register(gm, server.SignalServiceHandler), "unable to register handler")
	logger.FatalIf(stopRebalanceRPC.Register(gm, server.StopRebalanceHandler), "unable to register handler")
	logger.FatalIf(updateMetacacheListingRPC.Register(gm, server.UpdateMetacacheListingHandler), "unable to register handler")
	logger.FatalIf(cleanupUploadIDCacheMetaRPC.Register(gm, server.HandlerClearUploadID), "unable to register handler")

	logger.FatalIf(gm.RegisterStreamingHandler(grid.HandlerTrace, grid.StreamHandler{
		Handle:      server.TraceHandler,
		Subroute:    "",
		OutCapacity: 100000,
		InCapacity:  0,
	}), "unable to register handler")
}
