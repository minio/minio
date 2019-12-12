/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/lifecycle"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
	trace "github.com/minio/minio/pkg/trace"
)

// To abstract a node over network.
type peerRESTServer struct {
}

func getServerInfo() (*ServerInfoData, error) {
	objLayer := newObjectLayerWithoutSafeModeFn()
	if objLayer == nil {
		return nil, errServerNotInitialized
	}

	// Server info data.
	return &ServerInfoData{
		ConnStats: globalConnStats.toServerConnStats(),
		HTTPStats: globalHTTPStats.toServerHTTPStats(),
		Properties: ServerProperties{
			Uptime:       UTCNow().Unix() - globalBootTime.Unix(),
			Version:      Version,
			CommitID:     CommitID,
			DeploymentID: globalDeploymentID,
			SQSARN:       globalNotificationSys.GetARNList(),
			Region:       globalServerRegion,
		},
	}, nil
}

// NetReadPerfInfoHandler - returns network read performance information.
func (s *peerRESTServer) NetReadPerfInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	params := mux.Vars(r)

	sizeStr, found := params[peerRESTNetPerfSize]
	if !found {
		s.writeErrorResponse(w, errors.New("size is missing"))
		return
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil || size < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	start := time.Now()
	n, err := io.CopyN(ioutil.Discard, r.Body, size)
	end := time.Now()

	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	if n != size {
		s.writeErrorResponse(w, fmt.Errorf("short read; expected: %v, got: %v", size, n))
		return
	}

	addr := r.Host
	if globalIsDistXL {
		addr = GetLocalPeer(globalEndpoints)
	}

	d := end.Sub(start)
	info := ServerNetReadPerfInfo{
		Addr:           addr,
		ReadThroughput: uint64(int64(time.Second) * size / int64(d)),
	}

	ctx := newContext(r, w, "NetReadPerfInfo")
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
	w.(http.Flusher).Flush()
}

// CollectNetPerfInfoHandler - returns network performance information collected from other peers.
func (s *peerRESTServer) CollectNetPerfInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	params := mux.Vars(r)
	sizeStr, found := params[peerRESTNetPerfSize]
	if !found {
		s.writeErrorResponse(w, errors.New("size is missing"))
		return
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil || size < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	info := globalNotificationSys.NetReadPerfInfo(size)

	ctx := newContext(r, w, "CollectNetPerfInfo")
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
	w.(http.Flusher).Flush()
}

// GetLocksHandler - returns list of older lock from the server.
func (s *peerRESTServer) GetLocksHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "GetLocks")

	var llockers []map[string][]lockRequesterInfo
	for _, llocker := range globalLockServers {
		llockers = append(llockers, llocker.DupLockMap())
	}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(llockers))

	w.(http.Flusher).Flush()

}

// DeletePolicyHandler - deletes a policy on the server.
func (s *peerRESTServer) DeletePolicyHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	policyName := vars[peerRESTPolicy]
	if policyName == "" {
		s.writeErrorResponse(w, errors.New("policyName is missing"))
		return
	}

	if err := globalIAMSys.DeletePolicy(policyName); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// LoadPolicyHandler - reloads a policy on the server.
func (s *peerRESTServer) LoadPolicyHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	policyName := vars[peerRESTPolicy]
	if policyName == "" {
		s.writeErrorResponse(w, errors.New("policyName is missing"))
		return
	}

	if err := globalIAMSys.LoadPolicy(objAPI, policyName); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// LoadPolicyMappingHandler - reloads a policy mapping on the server.
func (s *peerRESTServer) LoadPolicyMappingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	userOrGroup := vars[peerRESTUserOrGroup]
	if userOrGroup == "" {
		s.writeErrorResponse(w, errors.New("user-or-group is missing"))
		return
	}
	_, isGroup := vars[peerRESTIsGroup]

	if err := globalIAMSys.LoadPolicyMapping(objAPI, userOrGroup, isGroup); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// DeleteUserHandler - deletes a user on the server.
func (s *peerRESTServer) DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars[peerRESTUser]
	if accessKey == "" {
		s.writeErrorResponse(w, errors.New("username is missing"))
		return
	}

	if err := globalIAMSys.DeleteUser(accessKey); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// LoadUserHandler - reloads a user on the server.
func (s *peerRESTServer) LoadUserHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
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

	if err = globalIAMSys.LoadUser(objAPI, accessKey, temp); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// LoadUsersHandler - reloads all users and canned policies.
func (s *peerRESTServer) LoadUsersHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	err := globalIAMSys.Load()
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// LoadGroupHandler - reloads group along with members list.
func (s *peerRESTServer) LoadGroupHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	if globalIAMSys == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}

	vars := mux.Vars(r)
	group := vars[peerRESTGroup]
	err := globalIAMSys.LoadGroup(objAPI, group)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// StartProfilingHandler - Issues the start profiling command.
func (s *peerRESTServer) StartProfilingHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	profiler := vars[peerRESTProfiler]
	if profiler == "" {
		s.writeErrorResponse(w, errors.New("profiler name is missing"))
		return
	}

	if globalProfiler != nil {
		globalProfiler.Stop()
	}

	var err error
	globalProfiler, err = startProfiler(profiler, "")
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	w.(http.Flusher).Flush()
}

// DownloadProflingDataHandler - returns proflied data.
func (s *peerRESTServer) DownloadProflingDataHandler(w http.ResponseWriter, r *http.Request) {
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

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(profileData))
}

// CPULoadInfoHandler - returns CPU Load info.
func (s *peerRESTServer) CPULoadInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "CPULoadInfo")
	info := getLocalCPULoad(globalEndpoints, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// CPUInfoHandler - returns CPU Hardware info.
func (s *peerRESTServer) CPUInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "CPUInfo")
	info := getLocalCPUInfo(globalEndpoints, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// NetworkInfoHandler - returns Network Hardware info.
func (s *peerRESTServer) NetworkInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "NetworkInfo")
	info := getLocalNetworkInfo(globalEndpoints, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// ServerInfoHandler - returns Server Info
func (s *peerRESTServer) ServerInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "ServerInfo")
	info := getLocalServerProperty(globalEndpoints, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// DrivePerfInfoHandler - returns Drive Performance info.
func (s *peerRESTServer) DrivePerfInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	params := mux.Vars(r)

	sizeStr, found := params[peerRESTDrivePerfSize]
	if !found {
		s.writeErrorResponse(w, errors.New("size is missing"))
		return
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil || size < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	ctx := newContext(r, w, "DrivePerfInfo")

	info := getLocalDrivesPerf(globalEndpoints, size, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// MemUsageInfoHandler - returns Memory Usage info.
func (s *peerRESTServer) MemUsageInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}
	ctx := newContext(r, w, "MemUsageInfo")
	info := getLocalMemUsage(globalEndpoints, r)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// DeleteBucketHandler - Delete notification and policies related to the bucket.
func (s *peerRESTServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
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

	globalNotificationSys.RemoveNotification(bucketName)
	globalPolicySys.Remove(bucketName)
	globalBucketObjectLockConfig.Remove(bucketName)
	globalLifecycleSys.Remove(bucketName)

	w.(http.Flusher).Flush()
}

// ReloadFormatHandler - Reload Format.
func (s *peerRESTServer) ReloadFormatHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	dryRunString := vars[peerRESTDryRun]
	if dryRunString == "" {
		s.writeErrorResponse(w, errors.New("dry run parameter is missing"))
		return
	}

	var dryRun bool
	switch strings.ToLower(dryRunString) {
	case "true":
		dryRun = true
	case "false":
		dryRun = false
	default:
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return
	}
	err := objAPI.ReloadFormat(context.Background(), dryRun)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	w.(http.Flusher).Flush()
}

// RemoveBucketPolicyHandler - Remove bucket policy.
func (s *peerRESTServer) RemoveBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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

	globalPolicySys.Remove(bucketName)
	w.(http.Flusher).Flush()
}

// SetBucketPolicyHandler - Set bucket policy.
func (s *peerRESTServer) SetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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
	var policyData policy.Policy
	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&policyData)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	globalPolicySys.Set(bucketName, policyData)
	w.(http.Flusher).Flush()
}

// RemoveBucketLifecycleHandler - Remove bucket lifecycle.
func (s *peerRESTServer) RemoveBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
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

	globalLifecycleSys.Remove(bucketName)
	w.(http.Flusher).Flush()
}

// SetBucketLifecycleHandler - Set bucket lifecycle.
func (s *peerRESTServer) SetBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}
	var lifecycleData lifecycle.Lifecycle
	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lifecycleData)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	globalLifecycleSys.Set(bucketName, lifecycleData)
	w.(http.Flusher).Flush()
}

type remoteTargetExistsResp struct {
	Exists bool
}

// TargetExistsHandler - Check if Target exists.
func (s *peerRESTServer) TargetExistsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "TargetExists")
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
	var targetID event.TargetID
	if r.ContentLength <= 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&targetID)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	var targetExists remoteTargetExistsResp
	targetExists.Exists = globalNotificationSys.RemoteTargetExist(bucketName, targetID)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(&targetExists))
}

type sendEventRequest struct {
	Event    event.Event
	TargetID event.TargetID
}

type sendEventResp struct {
	Success bool
}

// SendEventHandler - Send Event.
func (s *peerRESTServer) SendEventHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "SendEvent")

	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}
	var eventReq sendEventRequest
	if r.ContentLength <= 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&eventReq)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	var eventResp sendEventResp
	eventResp.Success = true
	errs := globalNotificationSys.send(bucketName, eventReq.Event, eventReq.TargetID)

	for i := range errs {
		reqInfo := (&logger.ReqInfo{}).AppendTags("Event", eventReq.Event.EventName.String())
		reqInfo.AppendTags("targetName", eventReq.TargetID.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, errs[i].Err)

		eventResp.Success = false
		s.writeErrorResponse(w, errs[i].Err)
		return
	}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(&eventResp))
	w.(http.Flusher).Flush()
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
	w.(http.Flusher).Flush()
}

// RemoveBucketObjectLockConfigHandler - handles DELETE bucket object lock configuration.
func (s *peerRESTServer) RemoveBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
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

	globalBucketObjectLockConfig.Remove(bucketName)
	w.(http.Flusher).Flush()
}

// PutBucketObjectLockConfigHandler - handles PUT bucket object lock configuration.
func (s *peerRESTServer) PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
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

	var retention Retention
	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&retention)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	globalBucketObjectLockConfig.Set(bucketName, retention)
	w.(http.Flusher).Flush()
}

type listenBucketNotificationReq struct {
	EventNames []event.Name   `json:"eventNames"`
	Pattern    string         `json:"pattern"`
	TargetID   event.TargetID `json:"targetId"`
	Addr       xnet.Host      `json:"addr"`
}

// ListenBucketNotificationHandler - Listen bucket notification handler.
func (s *peerRESTServer) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
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

	var args listenBucketNotificationReq
	if r.ContentLength <= 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&args)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	restClient, err := newPeerRESTClient(&args.Addr)
	if err != nil {
		s.writeErrorResponse(w, fmt.Errorf("unable to find PeerRESTClient for provided address %v. This happens only if remote and this minio run with different set of endpoints", args.Addr))
		return
	}

	target := NewPeerRESTClientTarget(bucketName, args.TargetID, restClient)
	rulesMap := event.NewRulesMap(args.EventNames, args.Pattern, target.ID())
	if err := globalNotificationSys.AddRemoteTarget(bucketName, target, rulesMap); err != nil {
		reqInfo := &logger.ReqInfo{BucketName: target.bucketName}
		reqInfo.AppendTags("target", target.id.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		s.writeErrorResponse(w, err)
		return
	}
	w.(http.Flusher).Flush()
}

// ServerUpdateHandler - updates the current server.
func (s *peerRESTServer) ServerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	vars := mux.Vars(r)
	updateURL := vars[peerRESTUpdateURL]
	sha256Hex := vars[peerRESTSha256Hex]
	var latestReleaseTime time.Time
	var err error
	if latestRelease := vars[peerRESTLatestRelease]; latestRelease != "" {
		latestReleaseTime, err = time.Parse(latestRelease, time.RFC3339)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
	}
	us, err := updateServer(updateURL, sha256Hex, latestReleaseTime)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	if us.CurrentVersion != us.UpdatedVersion {
		globalServiceSignalCh <- serviceRestart
	}
}

var errUnsupportedSignal = fmt.Errorf("unsupported signal: only restart and stop signals are supported")

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
	defer w.(http.Flusher).Flush()
	switch signal {
	case serviceRestart:
		globalServiceSignalCh <- signal
	case serviceStop:
		globalServiceSignalCh <- signal
	default:
		s.writeErrorResponse(w, errUnsupportedSignal)
		return
	}
}

// ListenHandler sends http trace messages back to peer rest client
func (s *peerRESTServer) ListenHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Listen Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan interface{}, 2000)

	globalHTTPListen.Subscribe(ch, doneCh, func(entry interface{}) bool {
		return true
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
	trcAll := r.URL.Query().Get(peerRESTTraceAll) == "true"
	trcErr := r.URL.Query().Get(peerRESTTraceErr) == "true"

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Trace Publisher uses nonblocking publish and hence does not wait for slow subscribers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	ch := make(chan interface{}, 2000)

	globalHTTPTrace.Subscribe(ch, doneCh, func(entry interface{}) bool {
		return mustTrace(entry, trcAll, trcErr)
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
		case <-keepAliveTicker.C:
			if err := enc.Encode(&trace.Info{}); err != nil {
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

	state := getLocalBackgroundHealStatus()

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(state))
}

func (s *peerRESTServer) BackgroundOpsStatusHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	ctx := newContext(r, w, "BackgroundOpsStatus")

	state := BgOpsStatus{
		LifecycleOps: getLocalBgLifecycleOpsStatus(),
	}

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(state))
}

// ConsoleLogHandler sends console logs of this node back to peer rest client
func (s *peerRESTServer) ConsoleLogHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	doneCh := make(chan struct{})
	defer close(doneCh)

	ch := make(chan interface{}, 2000)
	globalConsoleSys.Subscribe(ch, doneCh, "", 0, string(logger.All), nil)

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

// registerPeerRESTHandlers - register peer rest router.
func registerPeerRESTHandlers(router *mux.Router) {
	server := &peerRESTServer{}
	subrouter := router.PathPrefix(peerRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetReadPerfInfo).HandlerFunc(httpTraceHdrs(server.NetReadPerfInfoHandler)).Queries(restQueries(peerRESTNetPerfSize)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCollectNetPerfInfo).HandlerFunc(httpTraceHdrs(server.CollectNetPerfInfoHandler)).Queries(restQueries(peerRESTNetPerfSize)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocks).HandlerFunc(httpTraceHdrs(server.GetLocksHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerInfo).HandlerFunc(httpTraceHdrs(server.ServerInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCPULoadInfo).HandlerFunc(httpTraceHdrs(server.CPULoadInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodMemUsageInfo).HandlerFunc(httpTraceHdrs(server.MemUsageInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDrivePerfInfo).HandlerFunc(httpTraceHdrs(server.DrivePerfInfoHandler)).Queries(restQueries(peerRESTDrivePerfSize)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodHardwareCPUInfo).HandlerFunc(httpTraceHdrs(server.CPUInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodHardwareNetworkInfo).HandlerFunc(httpTraceHdrs(server.NetworkInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteBucket).HandlerFunc(httpTraceHdrs(server.DeleteBucketHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSignalService).HandlerFunc(httpTraceHdrs(server.SignalServiceHandler)).Queries(restQueries(peerRESTSignal)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerUpdate).HandlerFunc(httpTraceHdrs(server.ServerUpdateHandler)).Queries(restQueries(peerRESTUpdateURL, peerRESTSha256Hex, peerRESTLatestRelease)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketPolicyRemove).HandlerFunc(httpTraceAll(server.RemoveBucketPolicyHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketPolicySet).HandlerFunc(httpTraceHdrs(server.SetBucketPolicyHandler)).Queries(restQueries(peerRESTBucket)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeletePolicy).HandlerFunc(httpTraceAll(server.DeletePolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicy).HandlerFunc(httpTraceAll(server.LoadPolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicyMapping).HandlerFunc(httpTraceAll(server.LoadPolicyMappingHandler)).Queries(restQueries(peerRESTUserOrGroup)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteUser).HandlerFunc(httpTraceAll(server.LoadUserHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadUser).HandlerFunc(httpTraceAll(server.LoadUserHandler)).Queries(restQueries(peerRESTUser, peerRESTUserTemp)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadUsers).HandlerFunc(httpTraceAll(server.LoadUsersHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadGroup).HandlerFunc(httpTraceAll(server.LoadGroupHandler)).Queries(restQueries(peerRESTGroup)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStartProfiling).HandlerFunc(httpTraceAll(server.StartProfilingHandler)).Queries(restQueries(peerRESTProfiler)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadProfilingData).HandlerFunc(httpTraceHdrs(server.DownloadProflingDataHandler))

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTargetExists).HandlerFunc(httpTraceHdrs(server.TargetExistsHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSendEvent).HandlerFunc(httpTraceHdrs(server.SendEventHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketNotificationPut).HandlerFunc(httpTraceHdrs(server.PutBucketNotificationHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketNotificationListen).HandlerFunc(httpTraceHdrs(server.ListenBucketNotificationHandler)).Queries(restQueries(peerRESTBucket)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadFormat).HandlerFunc(httpTraceHdrs(server.ReloadFormatHandler)).Queries(restQueries(peerRESTDryRun)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketLifecycleSet).HandlerFunc(httpTraceHdrs(server.SetBucketLifecycleHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketLifecycleRemove).HandlerFunc(httpTraceHdrs(server.RemoveBucketLifecycleHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBackgroundOpsStatus).HandlerFunc(server.BackgroundOpsStatusHandler)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTrace).HandlerFunc(server.TraceHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodListen).HandlerFunc(server.ListenHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBackgroundHealStatus).HandlerFunc(server.BackgroundHealStatusHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLog).HandlerFunc(server.ConsoleLogHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodPutBucketObjectLockConfig).HandlerFunc(httpTraceHdrs(server.PutBucketObjectLockConfigHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketObjectLockConfigRemove).HandlerFunc(httpTraceHdrs(server.RemoveBucketObjectLockConfigHandler)).Queries(restQueries(peerRESTBucket)...)
}
