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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
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
			SQSARN:       globalNotificationSys.GetARNList(false),
			Region:       globalServerRegion,
		},
	}, nil
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

	var userType = regularUser
	if temp {
		userType = stsUser
	}

	if err = globalIAMSys.LoadUser(objAPI, accessKey, userType); err != nil {
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

	w.(http.Flusher).Flush()
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

	defer w.(http.Flusher).Flush()
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

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

func (s *peerRESTServer) NetOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NetOBDInfo")
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
		err := fmt.Errorf("OBD: short read: expected %d found %d", r.ContentLength, n)

		logger.LogIf(ctx, err)
		w.Header().Set("FinalStatus", err.Error())
		return
	}
	w.Header().Set("FinalStatus", "Success")
	w.(http.Flusher).Flush()
}

func (s *peerRESTServer) DispatchNetOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	done := keepHTTPResponseAlive(w)

	ctx := newContext(r, w, "DispatchNetOBDInfo")
	info := globalNotificationSys.NetOBDInfo(ctx)

	done()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
	w.(http.Flusher).Flush()
}

// DriveOBDInfoHandler - returns Drive OBD info.
func (s *peerRESTServer) DriveOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "DriveOBDInfo"))
	defer cancel()
	infoSerial := getLocalDrivesOBD(ctx, false, globalEndpoints, r)
	infoParallel := getLocalDrivesOBD(ctx, true, globalEndpoints, r)

	errStr := ""
	if infoSerial.Error != "" {
		errStr = "serial: " + infoSerial.Error
	}
	if infoParallel.Error != "" {
		errStr = errStr + " parallel: " + infoParallel.Error
	}
	info := madmin.ServerDrivesOBDInfo{
		Addr:     infoSerial.Addr,
		Serial:   infoSerial.Serial,
		Parallel: infoParallel.Parallel,
		Error:    errStr,
	}
	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// CPUOBDInfoHandler - returns CPU OBD info.
func (s *peerRESTServer) CPUOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "CpuOBDInfo"))
	defer cancel()
	info := getLocalCPUOBDInfo(ctx)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// DiskHwOBDInfoHandler - returns Disk HW OBD info.
func (s *peerRESTServer) DiskHwOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "DiskHwOBDInfo"))
	defer cancel()
	info := getLocalDiskHwOBD(ctx)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// OsOBDInfoHandler - returns Os OBD info.
func (s *peerRESTServer) OsOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "OsOBDInfo"))
	defer cancel()
	info := getLocalOsInfoOBD(ctx)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// ProcOBDInfoHandler - returns Proc OBD info.
func (s *peerRESTServer) ProcOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "ProcOBDInfo"))
	defer cancel()
	info := getLocalProcOBD(ctx)

	defer w.(http.Flusher).Flush()
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(info))
}

// MemOBDInfoHandler - returns Mem OBD info.
func (s *peerRESTServer) MemOBDInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		s.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx, cancel := context.WithCancel(newContext(r, w, "MemOBDInfo"))
	defer cancel()
	info := getLocalMemOBD(ctx)

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
		s.writeErrorResponse(w, errors.New("dry-run parameter is missing"))
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

	err := objAPI.ReloadFormat(GlobalContext, dryRun)
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

// RemoveBucketSSEConfigHandler - Remove bucket encryption.
func (s *peerRESTServer) RemoveBucketSSEConfigHandler(w http.ResponseWriter, r *http.Request) {
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

	globalBucketSSEConfigSys.Remove(bucketName)
	w.(http.Flusher).Flush()
}

// SetBucketSSEConfigHandler - Set bucket encryption.
func (s *peerRESTServer) SetBucketSSEConfigHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars[peerRESTBucket]
	if bucketName == "" {
		s.writeErrorResponse(w, errors.New("Bucket name is missing"))
		return
	}

	var encConfig bucketsse.BucketSSEConfig
	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&encConfig)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	globalBucketSSEConfigSys.Set(bucketName, encConfig)
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
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
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

	var retention objectlock.Retention
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

	values := r.URL.Query()

	var prefix string
	if len(values[peerRESTListenPrefix]) > 1 {
		s.writeErrorResponse(w, errors.New("Invalid request"))
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
		s.writeErrorResponse(w, errors.New("Invalid request"))
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

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

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
		if ev.S3.Bucket.Name != values.Get(peerRESTListenBucket) {
			return false
		}
		objectName, uerr := url.QueryUnescape(ev.S3.Object.Key)
		if uerr != nil {
			objectName = ev.S3.Object.Key
		}
		return len(rulesMap.Match(ev.EventName, objectName).ToSlice()) != 0
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
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodGetLocks).HandlerFunc(httpTraceHdrs(server.GetLocksHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerInfo).HandlerFunc(httpTraceHdrs(server.ServerInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodProcOBDInfo).HandlerFunc(httpTraceHdrs(server.ProcOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodMemOBDInfo).HandlerFunc(httpTraceHdrs(server.MemOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodOsInfoOBDInfo).HandlerFunc(httpTraceHdrs(server.OsOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDiskHwOBDInfo).HandlerFunc(httpTraceHdrs(server.DiskHwOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodCPUOBDInfo).HandlerFunc(httpTraceHdrs(server.CPUOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDriveOBDInfo).HandlerFunc(httpTraceHdrs(server.DriveOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodNetOBDInfo).HandlerFunc(httpTraceHdrs(server.NetOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDispatchNetOBDInfo).HandlerFunc(httpTraceHdrs(server.DispatchNetOBDInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteBucket).HandlerFunc(httpTraceHdrs(server.DeleteBucketHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSignalService).HandlerFunc(httpTraceHdrs(server.SignalServiceHandler)).Queries(restQueries(peerRESTSignal)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodServerUpdate).HandlerFunc(httpTraceHdrs(server.ServerUpdateHandler)).Queries(restQueries(peerRESTUpdateURL, peerRESTSha256Hex, peerRESTLatestRelease)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketPolicyRemove).HandlerFunc(httpTraceAll(server.RemoveBucketPolicyHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketPolicySet).HandlerFunc(httpTraceHdrs(server.SetBucketPolicyHandler)).Queries(restQueries(peerRESTBucket)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeletePolicy).HandlerFunc(httpTraceAll(server.DeletePolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicy).HandlerFunc(httpTraceAll(server.LoadPolicyHandler)).Queries(restQueries(peerRESTPolicy)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadPolicyMapping).HandlerFunc(httpTraceAll(server.LoadPolicyMappingHandler)).Queries(restQueries(peerRESTUserOrGroup)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDeleteUser).HandlerFunc(httpTraceAll(server.DeleteUserHandler)).Queries(restQueries(peerRESTUser)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadUser).HandlerFunc(httpTraceAll(server.LoadUserHandler)).Queries(restQueries(peerRESTUser, peerRESTUserTemp)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLoadGroup).HandlerFunc(httpTraceAll(server.LoadGroupHandler)).Queries(restQueries(peerRESTGroup)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodStartProfiling).HandlerFunc(httpTraceAll(server.StartProfilingHandler)).Queries(restQueries(peerRESTProfiler)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodDownloadProfilingData).HandlerFunc(httpTraceHdrs(server.DownloadProfilingDataHandler))

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTargetExists).HandlerFunc(httpTraceHdrs(server.TargetExistsHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodSendEvent).HandlerFunc(httpTraceHdrs(server.SendEventHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketNotificationPut).HandlerFunc(httpTraceHdrs(server.PutBucketNotificationHandler)).Queries(restQueries(peerRESTBucket)...)

	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodReloadFormat).HandlerFunc(httpTraceHdrs(server.ReloadFormatHandler)).Queries(restQueries(peerRESTDryRun)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketLifecycleSet).HandlerFunc(httpTraceHdrs(server.SetBucketLifecycleHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketLifecycleRemove).HandlerFunc(httpTraceHdrs(server.RemoveBucketLifecycleHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketEncryptionSet).HandlerFunc(httpTraceHdrs(server.SetBucketSSEConfigHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketEncryptionRemove).HandlerFunc(httpTraceHdrs(server.RemoveBucketSSEConfigHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodTrace).HandlerFunc(server.TraceHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodListen).HandlerFunc(httpTraceHdrs(server.ListenHandler))
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBackgroundHealStatus).HandlerFunc(server.BackgroundHealStatusHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodLog).HandlerFunc(server.ConsoleLogHandler)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodPutBucketObjectLockConfig).HandlerFunc(httpTraceHdrs(server.PutBucketObjectLockConfigHandler)).Queries(restQueries(peerRESTBucket)...)
	subrouter.Methods(http.MethodPost).Path(peerRESTVersionPrefix + peerRESTMethodBucketObjectLockConfigRemove).HandlerFunc(httpTraceHdrs(server.RemoveBucketObjectLockConfigHandler)).Queries(restQueries(peerRESTBucket)...)
}
