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
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

const (
	throttleDeadline = 1 * time.Hour
	// ReplicationReset has reset id and timestamp of last reset operation
	ReplicationReset = "replication-reset"
	// ReplicationStatus has internal replication status - stringified representation of target's replication status for all replication
	// activity initiated from this cluster
	ReplicationStatus = "replication-status"
	// ReplicationTimestamp - the last time replication was initiated on this cluster for this object version
	ReplicationTimestamp = "replication-timestamp"
	// ReplicaStatus - this header is present if a replica was received by this cluster for this object version
	ReplicaStatus = "replica-status"
	// ReplicaTimestamp - the last time a replica was received by this cluster for this object version
	ReplicaTimestamp = "replica-timestamp"
	// TaggingTimestamp - the last time a tag metadata modification happened on this cluster for this object version
	TaggingTimestamp = "tagging-timestamp"
	// ObjectLockRetentionTimestamp - the last time a object lock metadata modification happened on this cluster for this object version
	ObjectLockRetentionTimestamp = "objectlock-retention-timestamp"
	// ObjectLockLegalHoldTimestamp - the last time a legal hold metadata modification happened on this cluster for this object version
	ObjectLockLegalHoldTimestamp = "objectlock-legalhold-timestamp"
)

// gets replication config associated to a given bucket name.
func getReplicationConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return rc, errServerNotInitialized
		}

		return rc, BucketReplicationConfigNotFound{Bucket: bucketName}
	}

	return globalBucketMetadataSys.GetReplicationConfig(ctx, bucketName)
}

// validateReplicationDestination returns error if replication destination bucket missing or not configured
// It also returns true if replication destination is same as this server.
func validateReplicationDestination(ctx context.Context, bucket string, rCfg *replication.Config) (bool, APIError) {
	var arns []string
	if rCfg.RoleArn != "" {
		arns = append(arns, rCfg.RoleArn)
	} else {
		for _, rule := range rCfg.Rules {
			arns = append(arns, rule.Destination.String())
		}
	}
	for _, arnStr := range arns {
		arn, err := madmin.ParseARN(arnStr)
		if err != nil {
			return false, errorCodes.ToAPIErrWithErr(ErrBucketRemoteArnInvalid, err)
		}
		if arn.Type != madmin.ReplicationService {
			return false, toAPIError(ctx, BucketRemoteArnTypeInvalid{Bucket: bucket})
		}
		clnt := globalBucketTargetSys.GetRemoteTargetClient(ctx, arnStr)
		if clnt == nil {
			return false, toAPIError(ctx, BucketRemoteTargetNotFound{Bucket: bucket})
		}
		if found, err := clnt.BucketExists(ctx, arn.Bucket); !found {
			return false, errorCodes.ToAPIErrWithErr(ErrRemoteDestinationNotFoundError, err)
		}
		if ret, err := globalBucketObjectLockSys.Get(bucket); err == nil {
			if ret.LockEnabled {
				lock, _, _, _, err := clnt.GetObjectLockConfig(ctx, arn.Bucket)
				if err != nil || lock != "Enabled" {
					return false, errorCodes.ToAPIErrWithErr(ErrReplicationDestinationMissingLock, err)
				}
			}
		}
		// validate replication ARN against target endpoint
		c, ok := globalBucketTargetSys.arnRemotesMap[arnStr]
		if ok {
			if c.EndpointURL().String() == clnt.EndpointURL().String() {
				sameTarget, _ := isLocalHost(clnt.EndpointURL().Hostname(), clnt.EndpointURL().Port(), globalMinioPort)
				return sameTarget, toAPIError(ctx, nil)
			}
		}
	}
	return false, toAPIError(ctx, BucketRemoteTargetNotFound{Bucket: bucket})
}

type mustReplicateOptions struct {
	meta               map[string]string
	status             replication.StatusType
	opType             replication.Type
	replicationRequest bool // incoming request is a replication request
}

func (o mustReplicateOptions) ReplicationStatus() (s replication.StatusType) {
	if rs, ok := o.meta[xhttp.AmzBucketReplicationStatus]; ok {
		return replication.StatusType(rs)
	}
	return s
}
func (o mustReplicateOptions) isExistingObjectReplication() bool {
	return o.opType == replication.ExistingObjectReplicationType
}

func (o mustReplicateOptions) isMetadataReplication() bool {
	return o.opType == replication.MetadataReplicationType
}
func getMustReplicateOptions(o ObjectInfo, op replication.Type, opts ObjectOptions) mustReplicateOptions {
	if !op.Valid() {
		op = replication.ObjectReplicationType
		if o.metadataOnly {
			op = replication.MetadataReplicationType
		}
	}
	meta := cloneMSS(o.UserDefined)
	if o.UserTags != "" {
		meta[xhttp.AmzObjectTagging] = o.UserTags
	}

	return mustReplicateOptions{
		meta:               meta,
		status:             o.ReplicationStatus,
		opType:             op,
		replicationRequest: opts.ReplicationRequest,
	}
}

// mustReplicate returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicate(ctx context.Context, bucket, object string, mopts mustReplicateOptions) (dsc ReplicateDecision) {
	if globalIsGateway {
		return
	}

	replStatus := mopts.ReplicationStatus()
	if replStatus == replication.Replica && !mopts.isMetadataReplication() {
		return
	}

	if mopts.replicationRequest { // incoming replication request on target cluster
		return
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return
	}
	opts := replication.ObjectOpts{
		Name:           object,
		SSEC:           crypto.SSEC.IsEncrypted(mopts.meta),
		Replica:        replStatus == replication.Replica,
		ExistingObject: mopts.isExistingObjectReplication(),
	}
	tagStr, ok := mopts.meta[xhttp.AmzObjectTagging]
	if ok {
		opts.UserTags = tagStr
	}
	tgtArns := cfg.FilterTargetArns(opts)
	for _, tgtArn := range tgtArns {
		tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, tgtArn)
		// the target online status should not be used here while deciding
		// whether to replicate as the target could be temporarily down
		opts.TargetArn = tgtArn
		replicate := cfg.Replicate(opts)
		var synchronous bool
		if tgt != nil {
			synchronous = tgt.replicateSync
		}
		dsc.Set(newReplicateTargetDecision(tgtArn, replicate, synchronous))
	}
	return dsc
}

// Standard headers that needs to be extracted from User metadata.
var standardHeaders = []string{
	xhttp.ContentType,
	xhttp.CacheControl,
	xhttp.ContentEncoding,
	xhttp.ContentLanguage,
	xhttp.ContentDisposition,
	xhttp.AmzStorageClass,
	xhttp.AmzObjectTagging,
	xhttp.AmzBucketReplicationStatus,
	xhttp.AmzObjectLockMode,
	xhttp.AmzObjectLockRetainUntilDate,
	xhttp.AmzObjectLockLegalHold,
	xhttp.AmzTagCount,
	xhttp.AmzServerSideEncryption,
}

// returns true if any of the objects being deleted qualifies for replication.
func hasReplicationRules(ctx context.Context, bucket string, objects []ObjectToDelete) bool {
	c, err := getReplicationConfig(ctx, bucket)
	if err != nil || c == nil {
		return false
	}
	for _, obj := range objects {
		if c.HasActiveRules(obj.ObjectName, true) {
			return true
		}
	}
	return false
}

// isStandardHeader returns true if header is a supported header and not a custom header
func isStandardHeader(matchHeaderKey string) bool {
	return equals(matchHeaderKey, standardHeaders...)
}

// returns whether object version is a deletemarker and if object qualifies for replication
func checkReplicateDelete(ctx context.Context, bucket string, dobj ObjectToDelete, oi ObjectInfo, delOpts ObjectOptions, gerr error) (dsc ReplicateDecision) {
	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || rcfg == nil {
		return
	}
	// If incoming request is a replication request, it does not need to be re-replicated.
	if delOpts.ReplicationRequest {
		return
	}
	opts := replication.ObjectOpts{
		Name:         dobj.ObjectName,
		SSEC:         crypto.SSEC.IsEncrypted(oi.UserDefined),
		UserTags:     oi.UserTags,
		DeleteMarker: oi.DeleteMarker,
		VersionID:    dobj.VersionID,
		OpType:       replication.DeleteReplicationType,
	}
	tgtArns := rcfg.FilterTargetArns(opts)
	if len(tgtArns) > 0 {
		dsc.targetsMap = make(map[string]replicateTargetDecision, len(tgtArns))
		var sync, replicate bool
		for _, tgtArn := range tgtArns {
			opts.TargetArn = tgtArn
			replicate = rcfg.Replicate(opts)
			// when incoming delete is removal of a delete marker( a.k.a versioned delete),
			// GetObjectInfo returns extra information even though it returns errFileNotFound
			if gerr != nil {
				validReplStatus := false
				switch oi.TargetReplicationStatus(tgtArn) {
				case replication.Pending, replication.Completed, replication.Failed:
					validReplStatus = true
				}
				if oi.DeleteMarker && (validReplStatus || replicate) {
					dsc.Set(newReplicateTargetDecision(tgtArn, replicate, sync))
					continue
				} else {
					// can be the case that other cluster is down and duplicate `mc rm --vid`
					// is issued - this still needs to be replicated back to the other target
					replicate = oi.VersionPurgeStatus == Pending || oi.VersionPurgeStatus == Failed
					dsc.Set(newReplicateTargetDecision(tgtArn, replicate, sync))
					continue
				}
			}
			tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, tgtArn)
			// the target online status should not be used here while deciding
			// whether to replicate deletes as the target could be temporarily down
			tgtDsc := newReplicateTargetDecision(tgtArn, false, false)
			if tgt != nil {
				tgtDsc = newReplicateTargetDecision(tgtArn, replicate, tgt.replicateSync)
			}
			dsc.Set(tgtDsc)
		}
	}
	return dsc
}

// replicate deletes to the designated replication target if replication configuration
// has delete marker replication or delete replication (MinIO extension to allow deletes where version id
// is specified) enabled.
// Similar to bucket replication for PUT operation, soft delete (a.k.a setting delete marker) and
// permanent deletes (by specifying a version ID in the delete operation) have three states "Pending", "Complete"
// and "Failed" to mark the status of the replication of "DELETE" operation. All failed operations can
// then be retried by healing. In the case of permanent deletes, until the replication is completed on the
// target cluster, the object version is marked deleted on the source and hidden from listing. It is permanently
// deleted from the source when the VersionPurgeStatus changes to "Complete", i.e after replication succeeds
// on target.
func replicateDelete(ctx context.Context, dobj DeletedObjectReplicationInfo, objectAPI ObjectLayer, trigger string) {
	var replicationStatus replication.StatusType
	bucket := dobj.Bucket
	versionID := dobj.DeleteMarkerVersionID
	if versionID == "" {
		versionID = dobj.VersionID
	}

	defer func() {
		replStatus := string(replicationStatus)
		auditLogInternal(context.Background(), bucket, dobj.ObjectName, AuditLogOptions{
			Trigger:   trigger,
			APIName:   ReplicateDeleteAPI,
			VersionID: versionID,
			Status:    replStatus,
		})
	}()

	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || rcfg == nil {
		logger.LogIf(ctx, err)
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			Host:      "Internal: [Replication]",
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}
	dsc, err := parseReplicateDecision(dobj.ReplicationState.ReplicateDecisionStr)
	if err != nil {
		logger.LogIf(ctx, err)
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			Host:      "Internal: [Replication]",
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}

	// Lock the object name before starting replication operation.
	// Use separate lock that doesn't collide with regular objects.
	lk := objectAPI.NewNSLock(bucket, "/[replicate]/"+dobj.ObjectName)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("failed to get lock for object: %s bucket:%s arn:%s", dobj.ObjectName, bucket, rcfg.RoleArn))
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			Host:      "Internal: [Replication]",
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	var wg sync.WaitGroup
	var rinfos replicatedInfos
	rinfos.Targets = make([]replicatedTargetInfo, len(dsc.targetsMap))
	idx := -1
	for tgtArn := range dsc.targetsMap {
		idx++
		tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, tgtArn)
		if tgt == nil {
			logger.LogIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, tgtArn))
			sendEvent(eventArgs{
				BucketName: bucket,
				Object: ObjectInfo{
					Bucket:       bucket,
					Name:         dobj.ObjectName,
					VersionID:    versionID,
					DeleteMarker: dobj.DeleteMarker,
				},
				Host:      "Internal: [Replication]",
				EventName: event.ObjectReplicationNotTracked,
			})
			continue
		}
		if tgt := dsc.targetsMap[tgtArn]; !tgt.Replicate {
			continue
		}
		// if dobj.TargetArn is not empty string, this is a case of specific target being re-synced.
		if dobj.TargetArn != "" && dobj.TargetArn != tgt.ARN {
			continue
		}
		wg.Add(1)
		go func(index int, tgt *TargetClient) {
			defer wg.Done()
			rinfo := replicateDeleteToTarget(ctx, dobj, objectAPI, tgt)
			rinfos.Targets[index] = rinfo
		}(idx, tgt)
	}
	wg.Wait()

	replicationStatus = rinfos.ReplicationStatus()
	prevStatus := dobj.DeleteMarkerReplicationStatus()

	if dobj.VersionID != "" {
		prevStatus = replication.StatusType(dobj.VersionPurgeStatus())
		replicationStatus = replication.StatusType(rinfos.VersionPurgeStatus())
	}

	// to decrement pending count later.
	for _, rinfo := range rinfos.Targets {
		if rinfo.ReplicationStatus != rinfo.PrevReplicationStatus {
			globalReplicationStats.Update(dobj.Bucket, rinfo.Arn, 0, replicationStatus,
				prevStatus, replication.DeleteReplicationType)
		}
	}

	var eventName = event.ObjectReplicationComplete
	if replicationStatus == replication.Failed {
		eventName = event.ObjectReplicationFailed
	}
	drs := getReplicationState(rinfos, dobj.ReplicationState, dobj.VersionID)
	dobjInfo, err := objectAPI.DeleteObject(ctx, bucket, dobj.ObjectName, ObjectOptions{
		VersionID:         versionID,
		MTime:             dobj.DeleteMarkerMTime.Time,
		DeleteReplication: drs,
		Versioned:         globalBucketVersioningSys.Enabled(bucket),
		VersionSuspended:  globalBucketVersioningSys.Suspended(bucket),
	})
	if err != nil && !isErrVersionNotFound(err) { // VersionNotFound would be reported by pool that object version is missing on.
		logger.LogIf(ctx, fmt.Errorf("Unable to update replication metadata for %s/%s(%s): %s", bucket, dobj.ObjectName, versionID, err))
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			Host:      "Internal: [Replication]",
			EventName: eventName,
		})
	} else {
		sendEvent(eventArgs{
			BucketName: bucket,
			Object:     dobjInfo,
			Host:       "Internal: [Replication]",
			EventName:  eventName,
		})
	}
}

func replicateDeleteToTarget(ctx context.Context, dobj DeletedObjectReplicationInfo, objectAPI ObjectLayer, tgt *TargetClient) (rinfo replicatedTargetInfo) {
	versionID := dobj.DeleteMarkerVersionID
	if versionID == "" {
		versionID = dobj.VersionID
	}

	rinfo = dobj.ReplicationState.targetState(tgt.ARN)
	rinfo.OpType = dobj.OpType
	defer func() {
		if rinfo.ReplicationStatus == replication.Completed && tgt.ResetID != "" && dobj.OpType == replication.ExistingObjectReplicationType {
			rinfo.ResyncTimestamp = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), tgt.ResetID)
		}
	}()

	if dobj.VersionID == "" && rinfo.PrevReplicationStatus == replication.Completed && dobj.OpType != replication.ExistingObjectReplicationType {
		rinfo.ReplicationStatus = rinfo.PrevReplicationStatus
		return
	}
	if dobj.VersionID != "" && rinfo.VersionPurgeStatus == Complete {
		return
	}
	if tgt.IsOffline() {
		logger.LogIf(ctx, fmt.Errorf("remote target is offline for bucket:%s arn:%s", dobj.Bucket, tgt.ARN))
		sendEvent(eventArgs{
			BucketName: dobj.Bucket,
			Object: ObjectInfo{
				Bucket:       dobj.Bucket,
				Name:         dobj.ObjectName,
				VersionID:    dobj.VersionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			Host:      "Internal: [Replication]",
			EventName: event.ObjectReplicationNotTracked,
		})
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Failed
		} else {
			rinfo.VersionPurgeStatus = Failed
		}
		return
	}
	// early return if already replicated delete marker for existing object replication
	if dobj.DeleteMarkerVersionID != "" && dobj.OpType == replication.ExistingObjectReplicationType {
		if _, err := tgt.StatObject(ctx, tgt.Bucket, dobj.ObjectName, miniogo.StatObjectOptions{
			VersionID: versionID,
			Internal: miniogo.AdvancedGetOptions{
				ReplicationProxyRequest: "false",
			}}); isErrMethodNotAllowed(ErrorRespToObjectError(err, dobj.Bucket, dobj.ObjectName)) {
			if dobj.VersionID == "" {
				rinfo.ReplicationStatus = replication.Completed
			} else {
				rinfo.VersionPurgeStatus = Complete
			}
			return
		}
	}

	rmErr := tgt.RemoveObject(ctx, tgt.Bucket, dobj.ObjectName, miniogo.RemoveObjectOptions{
		VersionID: versionID,
		Internal: miniogo.AdvancedRemoveOptions{
			ReplicationDeleteMarker: dobj.DeleteMarkerVersionID != "",
			ReplicationMTime:        dobj.DeleteMarkerMTime.Time,
			ReplicationStatus:       miniogo.ReplicationStatusReplica,
			ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
		},
	})
	if rmErr != nil {
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Failed
		} else {
			rinfo.VersionPurgeStatus = Failed
		}
		logger.LogIf(ctx, fmt.Errorf("Unable to replicate delete marker to %s/%s(%s): %s", tgt.Bucket, dobj.ObjectName, versionID, rmErr))
	} else {
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Completed
		} else {
			rinfo.VersionPurgeStatus = Complete
		}
	}
	return
}

func getCopyObjMetadata(oi ObjectInfo, sc string) map[string]string {
	meta := make(map[string]string, len(oi.UserDefined))
	for k, v := range oi.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			continue
		}

		if equals(k, xhttp.AmzBucketReplicationStatus) {
			continue
		}

		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}
		meta[k] = v
	}

	if oi.ContentEncoding != "" {
		meta[xhttp.ContentEncoding] = oi.ContentEncoding
	}

	if oi.ContentType != "" {
		meta[xhttp.ContentType] = oi.ContentType
	}

	if oi.UserTags != "" {
		meta[xhttp.AmzObjectTagging] = oi.UserTags
		meta[xhttp.AmzTagDirective] = "REPLACE"
	}

	if sc == "" {
		sc = oi.StorageClass
	}
	// drop non standard storage classes for tiering from replication
	if sc != "" && (sc == storageclass.RRS || sc == storageclass.STANDARD) {
		meta[xhttp.AmzStorageClass] = sc
	}

	meta[xhttp.MinIOSourceETag] = oi.ETag
	meta[xhttp.MinIOSourceMTime] = oi.ModTime.Format(time.RFC3339Nano)
	meta[xhttp.AmzBucketReplicationStatus] = replication.Replica.String()
	return meta
}

type caseInsensitiveMap map[string]string

// Lookup map entry case insensitively.
func (m caseInsensitiveMap) Lookup(key string) (string, bool) {
	if len(m) == 0 {
		return "", false
	}
	for _, k := range []string{
		key,
		strings.ToLower(key),
		http.CanonicalHeaderKey(key),
	} {
		v, ok := m[k]
		if ok {
			return v, ok
		}
	}
	return "", false
}

func putReplicationOpts(ctx context.Context, sc string, objInfo ObjectInfo) (putOpts miniogo.PutObjectOptions, err error) {
	meta := make(map[string]string)
	for k, v := range objInfo.UserDefined {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			continue
		}
		if isStandardHeader(k) {
			continue
		}
		meta[k] = v
	}

	if sc == "" && (objInfo.StorageClass == storageclass.STANDARD || objInfo.StorageClass == storageclass.RRS) {
		sc = objInfo.StorageClass
	}
	putOpts = miniogo.PutObjectOptions{
		UserMetadata:    meta,
		ContentType:     objInfo.ContentType,
		ContentEncoding: objInfo.ContentEncoding,
		StorageClass:    sc,
		Internal: miniogo.AdvancedPutOptions{
			SourceVersionID:    objInfo.VersionID,
			ReplicationStatus:  miniogo.ReplicationStatusReplica,
			SourceMTime:        objInfo.ModTime,
			SourceETag:         objInfo.ETag,
			ReplicationRequest: true, // always set this to distinguish between `mc mirror` replication and serverside
		},
	}
	if objInfo.UserTags != "" {
		tag, _ := tags.ParseObjectTags(objInfo.UserTags)
		if tag != nil {
			putOpts.UserTags = tag.ToMap()
			// set tag timestamp in opts
			tagTimestamp := objInfo.ModTime
			if tagTmstampStr, ok := objInfo.UserDefined[ReservedMetadataPrefixLower+TaggingTimestamp]; ok {
				tagTimestamp, err = time.Parse(time.RFC3339Nano, tagTmstampStr)
				if err != nil {
					return putOpts, err
				}
			}
			putOpts.Internal.TaggingTimestamp = tagTimestamp
		}
	}

	lkMap := caseInsensitiveMap(objInfo.UserDefined)
	if lang, ok := lkMap.Lookup(xhttp.ContentLanguage); ok {
		putOpts.ContentLanguage = lang
	}
	if disp, ok := lkMap.Lookup(xhttp.ContentDisposition); ok {
		putOpts.ContentDisposition = disp
	}
	if cc, ok := lkMap.Lookup(xhttp.CacheControl); ok {
		putOpts.CacheControl = cc
	}
	if mode, ok := lkMap.Lookup(xhttp.AmzObjectLockMode); ok {
		rmode := miniogo.RetentionMode(mode)
		putOpts.Mode = rmode
	}
	if retainDateStr, ok := lkMap.Lookup(xhttp.AmzObjectLockRetainUntilDate); ok {
		rdate, err := time.Parse(time.RFC3339, retainDateStr)
		if err != nil {
			return putOpts, err
		}
		putOpts.RetainUntilDate = rdate
		// set retention timestamp in opts
		retTimestamp := objInfo.ModTime
		if retainTmstampStr, ok := objInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp]; ok {
			retTimestamp, err = time.Parse(time.RFC3339Nano, retainTmstampStr)
			if err != nil {
				return putOpts, err
			}
		}
		putOpts.Internal.RetentionTimestamp = retTimestamp
	}
	if lhold, ok := lkMap.Lookup(xhttp.AmzObjectLockLegalHold); ok {
		putOpts.LegalHold = miniogo.LegalHoldStatus(lhold)
		// set legalhold timestamp in opts
		lholdTimestamp := objInfo.ModTime
		if lholdTmstampStr, ok := objInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockLegalHoldTimestamp]; ok {
			lholdTimestamp, err = time.Parse(time.RFC3339Nano, lholdTmstampStr)
			if err != nil {
				return putOpts, err
			}
		}
		putOpts.Internal.LegalholdTimestamp = lholdTimestamp
	}
	if crypto.S3.IsEncrypted(objInfo.UserDefined) {
		putOpts.ServerSideEncryption = encrypt.NewSSE()
	}
	return
}

type replicationAction string

const (
	replicateMetadata replicationAction = "metadata"
	replicateNone     replicationAction = "none"
	replicateAll      replicationAction = "all"
)

// matches k1 with all keys, returns 'true' if one of them matches
func equals(k1 string, keys ...string) bool {
	for _, k2 := range keys {
		if strings.EqualFold(strings.ToLower(k1), strings.ToLower(k2)) {
			return true
		}
	}
	return false
}

// returns replicationAction by comparing metadata between source and target
func getReplicationAction(oi1 ObjectInfo, oi2 minio.ObjectInfo, opType replication.Type) replicationAction {
	// Avoid resyncing null versions created prior to enabling replication if target has a newer copy
	if opType == replication.ExistingObjectReplicationType &&
		oi1.ModTime.Unix() > oi2.LastModified.Unix() && oi1.VersionID == nullVersionID {
		return replicateNone
	}
	// needs full replication
	if oi1.ETag != oi2.ETag ||
		oi1.VersionID != oi2.VersionID ||
		oi1.Size != oi2.Size ||
		oi1.DeleteMarker != oi2.IsDeleteMarker ||
		oi1.ModTime.Unix() != oi2.LastModified.Unix() {
		return replicateAll
	}

	if oi1.ContentType != oi2.ContentType {
		return replicateMetadata
	}

	if oi1.ContentEncoding != "" {
		enc, ok := oi2.Metadata[xhttp.ContentEncoding]
		if !ok {
			enc, ok = oi2.Metadata[strings.ToLower(xhttp.ContentEncoding)]
			if !ok {
				return replicateMetadata
			}
		}
		if strings.Join(enc, ",") != oi1.ContentEncoding {
			return replicateMetadata
		}
	}

	t, _ := tags.ParseObjectTags(oi1.UserTags)
	if !reflect.DeepEqual(oi2.UserTags, t.ToMap()) {
		return replicateMetadata
	}

	// Compare only necessary headers
	compareKeys := []string{
		"Expires",
		"Cache-Control",
		"Content-Language",
		"Content-Disposition",
		"X-Amz-Object-Lock-Mode",
		"X-Amz-Object-Lock-Retain-Until-Date",
		"X-Amz-Object-Lock-Legal-Hold",
		"X-Amz-Website-Redirect-Location",
		"X-Amz-Meta-",
	}

	// compare metadata on both maps to see if meta is identical
	compareMeta1 := make(map[string]string)
	for k, v := range oi1.UserDefined {
		var found bool
		for _, prefix := range compareKeys {
			if !strings.HasPrefix(strings.ToLower(k), strings.ToLower(prefix)) {
				continue
			}
			found = true
			break
		}
		if found {
			compareMeta1[strings.ToLower(k)] = v
		}
	}

	compareMeta2 := make(map[string]string)
	for k, v := range oi2.Metadata {
		var found bool
		for _, prefix := range compareKeys {
			if !strings.HasPrefix(strings.ToLower(k), strings.ToLower(prefix)) {
				continue
			}
			found = true
			break
		}
		if found {
			compareMeta2[strings.ToLower(k)] = strings.Join(v, ",")
		}
	}

	if !reflect.DeepEqual(compareMeta1, compareMeta2) {
		return replicateMetadata
	}

	return replicateNone
}

// replicateObject replicates the specified version of the object to destination bucket
// The source object is then updated to reflect the replication status.
func replicateObject(ctx context.Context, ri ReplicateObjectInfo, objectAPI ObjectLayer, trigger string) {
	var replicationStatus replication.StatusType
	defer func() {
		if replicationStatus.Empty() {
			// replication status is empty means
			// replication was not attempted for some
			// reason, notify the state of the object
			// on disk.
			replicationStatus = ri.ReplicationStatus
		}
		auditLogInternal(ctx, ri.Bucket, ri.Name, AuditLogOptions{
			Trigger:   trigger,
			APIName:   ReplicateObjectAPI,
			VersionID: ri.VersionID,
			Status:    replicationStatus.String(),
		})
	}()

	objInfo := ri.ObjectInfo
	bucket := objInfo.Bucket
	object := objInfo.Name

	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return
	}
	tgtArns := cfg.FilterTargetArns(replication.ObjectOpts{
		Name: object,
		SSEC: crypto.SSEC.IsEncrypted(objInfo.UserDefined),
	})
	// Lock the object name before starting replication.
	// Use separate lock that doesn't collide with regular objects.
	lk := objectAPI.NewNSLock(bucket, "/[replicate]/"+object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		logger.LogIf(ctx, fmt.Errorf("failed to get lock for object: %s bucket:%s arn:%s", object, bucket, cfg.RoleArn))
		return
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	var wg sync.WaitGroup
	var rinfos replicatedInfos
	rinfos.Targets = make([]replicatedTargetInfo, len(tgtArns))
	for i, tgtArn := range tgtArns {
		tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, tgtArn)
		if tgt == nil {
			logger.LogIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, tgtArn))
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				Host:       "Internal: [Replication]",
			})
			continue
		}
		wg.Add(1)
		go func(index int, tgt *TargetClient) {
			defer wg.Done()
			rinfos.Targets[index] = replicateObjectToTarget(ctx, ri, objectAPI, tgt)
		}(i, tgt)
	}
	wg.Wait()
	// FIXME: add support for missing replication events
	// - event.ObjectReplicationMissedThreshold
	// - event.ObjectReplicationReplicatedAfterThreshold
	var eventName = event.ObjectReplicationComplete
	if rinfos.ReplicationStatus() == replication.Failed {
		eventName = event.ObjectReplicationFailed
	}
	newReplStatusInternal := rinfos.ReplicationStatusInternal()
	// Note that internal replication status(es) may match for previously replicated objects - in such cases
	// metadata should be updated with last resync timestamp.
	if objInfo.ReplicationStatusInternal != newReplStatusInternal || rinfos.ReplicationResynced() {
		popts := ObjectOptions{
			MTime:     objInfo.ModTime,
			VersionID: objInfo.VersionID,
			EvalMetadataFn: func(oi ObjectInfo) error {
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = newReplStatusInternal
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
				oi.UserDefined[xhttp.AmzBucketReplicationStatus] = string(rinfos.ReplicationStatus())
				for _, rinfo := range rinfos.Targets {
					if rinfo.ResyncTimestamp != "" {
						oi.UserDefined[targetResetHeader(rinfo.Arn)] = rinfo.ResyncTimestamp
					}
				}
				if objInfo.UserTags != "" {
					oi.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
				}
				return nil
			},
		}

		if _, err = objectAPI.PutObjectMetadata(ctx, bucket, object, popts); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to update replication metadata for %s/%s(%s): %w",
				bucket, objInfo.Name, objInfo.VersionID, err))
		}
		opType := replication.MetadataReplicationType
		if rinfos.Action() == replicateAll {
			opType = replication.ObjectReplicationType
		}
		for _, rinfo := range rinfos.Targets {
			if rinfo.ReplicationStatus != rinfo.PrevReplicationStatus {
				globalReplicationStats.Update(bucket, rinfo.Arn, rinfo.Size, rinfo.ReplicationStatus, rinfo.PrevReplicationStatus, opType)
			}
		}
	}

	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: bucket,
		Object:     objInfo,
		Host:       "Internal: [Replication]",
	})

	// re-queue failures once more - keep a retry count to avoid flooding the queue if
	// the target site is down. Leave it to scanner to catch up instead.
	if rinfos.ReplicationStatus() != replication.Completed && ri.RetryCount < 1 {
		ri.OpType = replication.HealReplicationType
		ri.ReplicationStatusInternal = rinfos.ReplicationStatusInternal()
		ri.RetryCount++
		globalReplicationPool.queueReplicaFailedTask(ri)
	}
}

// replicateObjectToTarget replicates the specified version of the object to destination bucket
// The source object is then updated to reflect the replication status.
func replicateObjectToTarget(ctx context.Context, ri ReplicateObjectInfo, objectAPI ObjectLayer, tgt *TargetClient) (rinfo replicatedTargetInfo) {
	objInfo := ri.ObjectInfo.Clone()
	bucket := objInfo.Bucket
	object := objInfo.Name
	var (
		closeOnDefer bool
		gr           *GetObjectReader
		size         int64
		err          error
	)
	sz, _ := objInfo.GetActualSize()
	// set defaults for replication action based on operation being performed - actual
	// replication action can only be determined after stat on remote. This default is
	// needed for updating replication metrics correctly when target is offline.
	var rAction replicationAction
	switch ri.OpType {
	case replication.MetadataReplicationType:
		rAction = replicateMetadata
	default:
		rAction = replicateAll
	}
	rinfo = replicatedTargetInfo{
		Size:                  sz,
		Arn:                   tgt.ARN,
		PrevReplicationStatus: objInfo.TargetReplicationStatus(tgt.ARN),
		ReplicationStatus:     replication.Failed,
		OpType:                ri.OpType,
		ReplicationAction:     rAction,
	}
	if ri.ObjectInfo.TargetReplicationStatus(tgt.ARN) == replication.Completed && !ri.ExistingObjResync.Empty() && !ri.ExistingObjResync.mustResyncTarget(tgt.ARN) {
		rinfo.ReplicationStatus = replication.Completed
		rinfo.ReplicationResynced = true
		return
	}
	if tgt.IsOffline() {
		logger.LogIf(ctx, fmt.Errorf("remote target is offline for bucket:%s arn:%s", bucket, tgt.ARN))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return
	}

	gr, err = objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{
		VersionID: objInfo.VersionID,
	})
	if err != nil {
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		logger.LogIf(ctx, fmt.Errorf("Unable to update replicate for %s/%s(%s): %w", bucket, object, objInfo.VersionID, err))
		return
	}
	defer func() {
		if closeOnDefer {
			gr.Close()
		}
	}()
	closeOnDefer = true

	objInfo = gr.ObjInfo
	size, err = objInfo.GetActualSize()
	if err != nil {
		logger.LogIf(ctx, err)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return
	}

	if tgt.Bucket == "" {
		logger.LogIf(ctx, fmt.Errorf("Unable to replicate object %s(%s), bucket is empty", objInfo.Name, objInfo.VersionID))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return rinfo
	}

	rAction = replicateAll
	oi, cerr := tgt.StatObject(ctx, tgt.Bucket, object, miniogo.StatObjectOptions{
		VersionID: objInfo.VersionID,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "false",
		}})
	if cerr == nil {
		rAction = getReplicationAction(objInfo, oi, ri.OpType)
		rinfo.ReplicationStatus = replication.Completed
		if rAction == replicateNone {
			if ri.OpType == replication.ExistingObjectReplicationType &&
				objInfo.ModTime.Unix() > oi.LastModified.Unix() && objInfo.VersionID == nullVersionID {
				logger.LogIf(ctx, fmt.Errorf("Unable to replicate %s/%s (null). Newer version exists on target", bucket, object))
				sendEvent(eventArgs{
					EventName:  event.ObjectReplicationNotTracked,
					BucketName: bucket,
					Object:     objInfo,
					Host:       "Internal: [Replication]",
				})
			}
			// object with same VersionID already exists, replication kicked off by
			// PutObject might have completed
			if objInfo.TargetReplicationStatus(tgt.ARN) == replication.Pending || objInfo.TargetReplicationStatus(tgt.ARN) == replication.Failed || ri.OpType == replication.ExistingObjectReplicationType {
				// if metadata is not updated for some reason after replication, such as
				// 503 encountered while updating metadata - make sure to set ReplicationStatus
				// as Completed.
				//
				// Note: Replication Stats would have been updated despite metadata update failure.
				gr.Close()
				closeOnDefer = false
				return replicatedTargetInfo{
					ReplicationStatus:     replication.Completed,
					Size:                  sz,
					Arn:                   tgt.ARN,
					ReplicationAction:     rAction,
					PrevReplicationStatus: objInfo.TargetReplicationStatus(tgt.ARN),
				}
			}
			return
		}
	}
	rinfo.ReplicationStatus = replication.Completed
	rinfo.Size = size
	rinfo.ReplicationAction = rAction
	defer func() {
		if rinfo.ReplicationStatus == replication.Completed && ri.OpType == replication.ExistingObjectReplicationType && tgt.ResetID != "" {
			rinfo.ResyncTimestamp = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), tgt.ResetID)
			rinfo.ReplicationResynced = true
		}
	}()
	// use core client to avoid doing multipart on PUT
	c := &miniogo.Core{Client: tgt.Client}
	if rAction != replicateAll {
		// replicate metadata for object tagging/copy with metadata replacement
		srcOpts := miniogo.CopySrcOptions{
			Bucket:    tgt.Bucket,
			Object:    object,
			VersionID: objInfo.VersionID,
		}
		dstOpts := miniogo.PutObjectOptions{
			Internal: miniogo.AdvancedPutOptions{
				SourceVersionID:    objInfo.VersionID,
				ReplicationRequest: true, // always set this to distinguish between `mc mirror` replication and serverside
			}}
		if _, err = c.CopyObject(ctx, tgt.Bucket, object, tgt.Bucket, object, getCopyObjMetadata(objInfo, tgt.StorageClass), srcOpts, dstOpts); err != nil {
			rinfo.ReplicationStatus = replication.Failed
			logger.LogIf(ctx, fmt.Errorf("Unable to replicate metadata for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
		}
	} else {
		var putOpts minio.PutObjectOptions
		putOpts, err = putReplicationOpts(ctx, tgt.StorageClass, objInfo)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("failed to get target for replication bucket:%s err:%w", bucket, err))
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				Host:       "Internal: [Replication]",
			})
			return
		}
		var headerSize int
		for k, v := range putOpts.Header() {
			headerSize += len(k) + len(v)
		}

		opts := &bandwidth.MonitorReaderOptions{
			Bucket:     objInfo.Bucket,
			HeaderSize: headerSize,
		}
		newCtx := ctx
		if globalBucketMonitor.IsThrottled(bucket) {
			var cancel context.CancelFunc
			newCtx, cancel = context.WithTimeout(ctx, throttleDeadline)
			defer cancel()
		}
		r := bandwidth.NewMonitoredReader(newCtx, globalBucketMonitor, gr, opts)
		if objInfo.isMultipart() {
			if err := replicateObjectWithMultipart(ctx, c, tgt.Bucket, object,
				r, objInfo, putOpts); err != nil {
				rinfo.ReplicationStatus = replication.Failed
				logger.LogIf(ctx, fmt.Errorf("Unable to replicate for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
			}
		} else {
			if _, err = c.PutObject(ctx, tgt.Bucket, object, r, size, "", "", putOpts); err != nil {
				rinfo.ReplicationStatus = replication.Failed
				logger.LogIf(ctx, fmt.Errorf("Unable to replicate for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
			}
		}
	}
	gr.Close()
	closeOnDefer = false
	return
}

func replicateObjectWithMultipart(ctx context.Context, c *miniogo.Core, bucket, object string, r io.Reader, objInfo ObjectInfo, opts miniogo.PutObjectOptions) (err error) {
	var uploadedParts []miniogo.CompletePart
	uploadID, err := c.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			// block and abort remote upload upon failure.
			if aerr := c.AbortMultipartUpload(ctx, bucket, object, uploadID); aerr != nil {
				aerr = fmt.Errorf("Unable to cleanup failed multipart replication %s on remote %s/%s: %w", uploadID, bucket, object, aerr)
				logger.LogIf(ctx, aerr)
			}
		}
	}()

	var (
		hr    *hash.Reader
		pInfo miniogo.ObjectPart
	)

	for _, partInfo := range objInfo.Parts {
		hr, err = hash.NewReader(r, partInfo.ActualSize, "", "", partInfo.ActualSize)
		if err != nil {
			return err
		}
		pInfo, err = c.PutObjectPart(ctx, bucket, object, uploadID, partInfo.Number, hr, partInfo.ActualSize, "", "", opts.ServerSideEncryption)
		if err != nil {
			return err
		}
		if pInfo.Size != partInfo.ActualSize {
			return fmt.Errorf("Part size mismatch: got %d, want %d", pInfo.Size, partInfo.ActualSize)
		}
		uploadedParts = append(uploadedParts, miniogo.CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	_, err = c.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, miniogo.PutObjectOptions{
		Internal: miniogo.AdvancedPutOptions{
			SourceMTime: objInfo.ModTime,
			// always set this to distinguish between `mc mirror` replication and serverside
			ReplicationRequest: true,
		}})
	return err
}

// filterReplicationStatusMetadata filters replication status metadata for COPY
func filterReplicationStatusMetadata(metadata map[string]string) map[string]string {
	// Copy on write
	dst := metadata
	var copied bool
	delKey := func(key string) {
		if _, ok := metadata[key]; !ok {
			return
		}
		if !copied {
			dst = make(map[string]string, len(metadata))
			for k, v := range metadata {
				dst[k] = v
			}
			copied = true
		}
		delete(dst, key)
	}

	delKey(xhttp.AmzBucketReplicationStatus)
	return dst
}

// DeletedObjectReplicationInfo has info on deleted object
type DeletedObjectReplicationInfo struct {
	DeletedObject
	Bucket    string
	OpType    replication.Type
	ResetID   string
	TargetArn string
}

// Replication specific APIName
const (
	ReplicateObjectAPI = "ReplicateObject"
	ReplicateDeleteAPI = "ReplicateDelete"
)

const (
	// ReplicateQueued - replication being queued trail
	ReplicateQueued = "replicate:queue"

	// ReplicateExisting - audit trail for existing objects replication
	ReplicateExisting = "replicate:existing"
	// ReplicateExistingDelete - audit trail for delete replication triggered for existing delete markers
	ReplicateExistingDelete = "replicate:existing:delete"

	// ReplicateMRF - audit trail for replication from Most Recent Failures (MRF) queue
	ReplicateMRF = "replicate:mrf"
	// ReplicateIncoming - audit trail indicating replication started [could be from incoming/existing/heal activity]
	ReplicateIncoming = "replicate:incoming"
	// ReplicateHeal - audit trail for healing of failed/pending replications
	ReplicateHeal = "replicate:heal"
	// ReplicateDelete - audit trail for delete replication
	ReplicateDelete = "replicate:delete"
)

var (
	globalReplicationPool  *ReplicationPool
	globalReplicationStats *ReplicationStats
)

// ReplicationPool describes replication pool
type ReplicationPool struct {
	objLayer                ObjectLayer
	ctx                     context.Context
	mrfWorkerKillCh         chan struct{}
	workerKillCh            chan struct{}
	replicaCh               chan ReplicateObjectInfo
	replicaDeleteCh         chan DeletedObjectReplicationInfo
	mrfReplicaCh            chan ReplicateObjectInfo
	existingReplicaCh       chan ReplicateObjectInfo
	existingReplicaDeleteCh chan DeletedObjectReplicationInfo
	workerSize              int
	mrfWorkerSize           int
	workerWg                sync.WaitGroup
	mrfWorkerWg             sync.WaitGroup
	once                    sync.Once
	mu                      sync.Mutex
}

// NewReplicationPool creates a pool of replication workers of specified size
func NewReplicationPool(ctx context.Context, o ObjectLayer, opts replicationPoolOpts) *ReplicationPool {
	pool := &ReplicationPool{
		replicaCh:               make(chan ReplicateObjectInfo, 100000),
		replicaDeleteCh:         make(chan DeletedObjectReplicationInfo, 100000),
		mrfReplicaCh:            make(chan ReplicateObjectInfo, 100000),
		workerKillCh:            make(chan struct{}, opts.Workers),
		mrfWorkerKillCh:         make(chan struct{}, opts.FailedWorkers),
		existingReplicaCh:       make(chan ReplicateObjectInfo, 100000),
		existingReplicaDeleteCh: make(chan DeletedObjectReplicationInfo, 100000),
		ctx:                     ctx,
		objLayer:                o,
	}

	pool.ResizeWorkers(opts.Workers)
	pool.ResizeFailedWorkers(opts.FailedWorkers)
	go pool.AddExistingObjectReplicateWorker()
	return pool
}

// AddMRFWorker adds a pending/failed replication worker to handle requests that could not be queued
// to the other workers
func (p *ReplicationPool) AddMRFWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case oi, ok := <-p.mrfReplicaCh:
			if !ok {
				return
			}
			replicateObject(p.ctx, oi, p.objLayer, ReplicateMRF)
		case <-p.mrfWorkerKillCh:
			return
		}
	}
}

// AddWorker adds a replication worker to the pool
func (p *ReplicationPool) AddWorker() {
	defer p.workerWg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case oi, ok := <-p.replicaCh:
			if !ok {
				return
			}
			replicateObject(p.ctx, oi, p.objLayer, ReplicateIncoming)
		case doi, ok := <-p.replicaDeleteCh:
			if !ok {
				return
			}
			replicateDelete(p.ctx, doi, p.objLayer, ReplicateDelete)
		case <-p.workerKillCh:
			return
		}
	}

}

// AddExistingObjectReplicateWorker adds a worker to queue existing objects that need to be sync'd
func (p *ReplicationPool) AddExistingObjectReplicateWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case oi, ok := <-p.existingReplicaCh:
			if !ok {
				return
			}
			replicateObject(p.ctx, oi, p.objLayer, ReplicateExisting)
		case doi, ok := <-p.existingReplicaDeleteCh:
			if !ok {
				return
			}
			replicateDelete(p.ctx, doi, p.objLayer, ReplicateExistingDelete)
		}
	}
}

// ResizeWorkers sets replication workers pool to new size
func (p *ReplicationPool) ResizeWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.workerSize < n {
		p.workerSize++
		p.workerWg.Add(1)
		go p.AddWorker()
	}
	for p.workerSize > n {
		p.workerSize--
		go func() { p.workerKillCh <- struct{}{} }()
	}
}

// ResizeFailedWorkers sets replication failed workers pool size
func (p *ReplicationPool) ResizeFailedWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.mrfWorkerSize < n {
		p.mrfWorkerSize++
		p.mrfWorkerWg.Add(1)
		go p.AddMRFWorker()
	}
	for p.mrfWorkerSize > n {
		p.mrfWorkerSize--
		go func() { p.mrfWorkerKillCh <- struct{}{} }()
	}
}

func (p *ReplicationPool) queueReplicaFailedTask(ri ReplicateObjectInfo) {
	if p == nil {
		return
	}
	select {
	case <-GlobalContext.Done():
		p.once.Do(func() {
			close(p.replicaCh)
			close(p.mrfReplicaCh)
			close(p.existingReplicaCh)
		})
	case p.mrfReplicaCh <- ri:
	default:
	}
}

func (p *ReplicationPool) queueReplicaTask(ri ReplicateObjectInfo) {
	if p == nil {
		return
	}
	var ch chan ReplicateObjectInfo
	switch ri.OpType {
	case replication.ExistingObjectReplicationType:
		ch = p.existingReplicaCh
	case replication.HealReplicationType:
		fallthrough
	default:
		ch = p.replicaCh
	}
	select {
	case <-GlobalContext.Done():
		p.once.Do(func() {
			close(p.replicaCh)
			close(p.mrfReplicaCh)
			close(p.existingReplicaCh)
		})
	case ch <- ri:
	default:
	}
}

func queueReplicateDeletesWrapper(doi DeletedObjectReplicationInfo, existingObjectResync ResyncDecision) {
	for k, v := range existingObjectResync.targets {
		if v.Replicate {
			doi.ResetID = v.ResetID
			doi.TargetArn = k

			globalReplicationPool.queueReplicaDeleteTask(doi)
		}
	}
}

func (p *ReplicationPool) queueReplicaDeleteTask(doi DeletedObjectReplicationInfo) {
	if p == nil {
		return
	}
	var ch chan DeletedObjectReplicationInfo
	switch doi.OpType {
	case replication.ExistingObjectReplicationType:
		ch = p.existingReplicaDeleteCh
	case replication.HealReplicationType:
		fallthrough
	default:
		ch = p.replicaDeleteCh
	}

	select {
	case <-GlobalContext.Done():
		p.once.Do(func() {
			close(p.replicaDeleteCh)
			close(p.existingReplicaDeleteCh)
		})
	case ch <- doi:
	default:
	}
}

type replicationPoolOpts struct {
	Workers       int
	FailedWorkers int
}

func initBackgroundReplication(ctx context.Context, objectAPI ObjectLayer) {
	globalReplicationPool = NewReplicationPool(ctx, objectAPI, replicationPoolOpts{
		Workers:       globalAPIConfig.getReplicationWorkers(),
		FailedWorkers: globalAPIConfig.getReplicationFailedWorkers(),
	})
	globalReplicationStats = NewReplicationStats(ctx, objectAPI)
	go globalReplicationStats.loadInitialReplicationMetrics(ctx)
}

// get Reader from replication target if active-active replication is in place and
// this node returns a 404
func proxyGetToReplicationTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (gr *GetObjectReader, proxy bool) {
	tgt, oi, proxy := proxyHeadToRepTarget(ctx, bucket, object, opts, proxyTargets)
	if !proxy {
		return nil, false
	}
	fn, off, length, err := NewGetObjectReader(rs, oi, opts)
	if err != nil {
		return nil, false
	}
	gopts := miniogo.GetObjectOptions{
		VersionID:            opts.VersionID,
		ServerSideEncryption: opts.ServerSideEncryption,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "true",
		},
	}
	// get correct offsets for encrypted object
	if off >= 0 && length >= 0 {
		if err := gopts.SetRange(off, off+length-1); err != nil {
			return nil, false
		}
	}
	// Make sure to match ETag when proxying.
	if err = gopts.SetMatchETag(oi.ETag); err != nil {
		return nil, false
	}
	c := miniogo.Core{Client: tgt.Client}
	obj, _, _, err := c.GetObject(ctx, bucket, object, gopts)
	if err != nil {
		return nil, false
	}
	closeReader := func() { obj.Close() }

	reader, err := fn(obj, h, closeReader)
	if err != nil {
		return nil, false
	}
	reader.ObjInfo = oi.Clone()
	return reader, true
}

func getproxyTargets(ctx context.Context, bucket, object string, opts ObjectOptions) (tgts *madmin.BucketTargets) {
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || cfg == nil {
		return &madmin.BucketTargets{}
	}
	topts := replication.ObjectOpts{Name: object}
	tgtArns := cfg.FilterTargetArns(topts)
	tgts = &madmin.BucketTargets{Targets: make([]madmin.BucketTarget, len(tgtArns))}
	for i, tgtArn := range tgtArns {
		tgt := globalBucketTargetSys.GetRemoteBucketTargetByArn(ctx, bucket, tgtArn)
		tgts.Targets[i] = tgt
	}

	return tgts
}

func proxyHeadToRepTarget(ctx context.Context, bucket, object string, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (tgt *TargetClient, oi ObjectInfo, proxy bool) {
	// this option is set when active-active replication is in place between site A -> B,
	// and site B does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return nil, oi, false
	}
	for _, t := range proxyTargets.Targets {
		tgt = globalBucketTargetSys.GetRemoteTargetClient(ctx, t.Arn)
		if tgt == nil || tgt.IsOffline() {
			continue
		}
		// if proxying explicitly disabled on remote target
		if tgt.disableProxy {
			continue
		}

		gopts := miniogo.GetObjectOptions{
			VersionID:            opts.VersionID,
			ServerSideEncryption: opts.ServerSideEncryption,
			Internal: miniogo.AdvancedGetOptions{
				ReplicationProxyRequest: "true",
			},
		}
		objInfo, err := tgt.StatObject(ctx, t.TargetBucket, object, gopts)
		if err != nil {
			continue
		}

		tags, _ := tags.MapToObjectTags(objInfo.UserTags)
		oi = ObjectInfo{
			Bucket:                    bucket,
			Name:                      object,
			ModTime:                   objInfo.LastModified,
			Size:                      objInfo.Size,
			ETag:                      objInfo.ETag,
			VersionID:                 objInfo.VersionID,
			IsLatest:                  objInfo.IsLatest,
			DeleteMarker:              objInfo.IsDeleteMarker,
			ContentType:               objInfo.ContentType,
			Expires:                   objInfo.Expires,
			StorageClass:              objInfo.StorageClass,
			ReplicationStatusInternal: objInfo.ReplicationStatus,
			UserTags:                  tags.String(),
		}
		oi.UserDefined = make(map[string]string, len(objInfo.Metadata))
		for k, v := range objInfo.Metadata {
			oi.UserDefined[k] = v[0]
		}
		ce, ok := oi.UserDefined[xhttp.ContentEncoding]
		if !ok {
			ce, ok = oi.UserDefined[strings.ToLower(xhttp.ContentEncoding)]
		}
		if ok {
			oi.ContentEncoding = ce
		}
		return tgt, oi, true
	}
	return nil, oi, false
}

// get object info from replication target if active-active replication is in place and
// this node returns a 404
func proxyHeadToReplicationTarget(ctx context.Context, bucket, object string, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (oi ObjectInfo, proxy bool) {
	_, oi, proxy = proxyHeadToRepTarget(ctx, bucket, object, opts, proxyTargets)
	return oi, proxy
}

func scheduleReplication(ctx context.Context, objInfo ObjectInfo, o ObjectLayer, dsc ReplicateDecision, opType replication.Type) {
	if dsc.Synchronous() {
		replicateObject(ctx, ReplicateObjectInfo{ObjectInfo: objInfo, OpType: opType, Dsc: dsc}, o, ReplicateIncoming)
	} else {
		globalReplicationPool.queueReplicaTask(ReplicateObjectInfo{ObjectInfo: objInfo, OpType: opType, Dsc: dsc})
	}
	if sz, err := objInfo.GetActualSize(); err == nil {
		for arn := range dsc.targetsMap {
			globalReplicationStats.Update(objInfo.Bucket, arn, sz, objInfo.ReplicationStatus, replication.StatusType(""), opType)
		}
	}
}

func scheduleReplicationDelete(ctx context.Context, dv DeletedObjectReplicationInfo, o ObjectLayer) {
	globalReplicationPool.queueReplicaDeleteTask(dv)
	for arn := range dv.ReplicationState.Targets {
		globalReplicationStats.Update(dv.Bucket, arn, 0, replication.Pending, replication.StatusType(""), replication.DeleteReplicationType)
	}
	for arn := range dv.ReplicationState.PurgeTargets {
		globalReplicationStats.Update(dv.Bucket, arn, 0, replication.Pending, replication.StatusType(""), replication.DeleteReplicationType)
	}
}

type replicationConfig struct {
	Config  *replication.Config
	remotes *madmin.BucketTargets
}

func (c replicationConfig) Empty() bool {
	return c.Config == nil
}
func (c replicationConfig) Replicate(opts replication.ObjectOpts) bool {
	return c.Config.Replicate(opts)
}

// Resync returns true if replication reset is requested
func (c replicationConfig) Resync(ctx context.Context, oi ObjectInfo, dsc *ReplicateDecision, tgtStatuses map[string]replication.StatusType) (r ResyncDecision) {
	if c.Empty() {
		return
	}
	// existing object replication does not apply to un-versioned objects
	if oi.VersionID == "" {
		return
	}

	// Now overlay existing object replication choices for target
	if oi.DeleteMarker {
		opts := replication.ObjectOpts{
			Name:           oi.Name,
			SSEC:           crypto.SSEC.IsEncrypted(oi.UserDefined),
			UserTags:       oi.UserTags,
			DeleteMarker:   oi.DeleteMarker,
			VersionID:      oi.VersionID,
			OpType:         replication.DeleteReplicationType,
			ExistingObject: true}

		tgtArns := c.Config.FilterTargetArns(opts)
		// indicates no matching target with Existing object replication enabled.
		if len(tgtArns) == 0 {
			return
		}
		for _, t := range tgtArns {
			opts.TargetArn = t
			// Update replication decision for target based on existing object replciation rule.
			dsc.Set(newReplicateTargetDecision(t, c.Replicate(opts), false))
		}
		return c.resync(oi, dsc, tgtStatuses)
	}

	// Ignore previous replication status when deciding if object can be re-replicated
	objInfo := oi.Clone()
	objInfo.ReplicationStatusInternal = ""
	objInfo.VersionPurgeStatusInternal = ""
	objInfo.ReplicationStatus = ""
	objInfo.VersionPurgeStatus = ""
	resyncdsc := mustReplicate(ctx, oi.Bucket, oi.Name, getMustReplicateOptions(objInfo, replication.ExistingObjectReplicationType, ObjectOptions{}))
	dsc = &resyncdsc
	return c.resync(oi, dsc, tgtStatuses)
}

// wrapper function for testability. Returns true if a new reset is requested on
// already replicated objects OR object qualifies for existing object replication
// and no reset requested.
func (c replicationConfig) resync(oi ObjectInfo, dsc *ReplicateDecision, tgtStatuses map[string]replication.StatusType) (r ResyncDecision) {
	r = ResyncDecision{
		targets: make(map[string]ResyncTargetDecision),
	}
	if c.remotes == nil {
		return
	}
	for _, tgt := range c.remotes.Targets {
		d, ok := dsc.targetsMap[tgt.Arn]
		if !ok {
			continue
		}
		if !d.Replicate {
			continue
		}
		r.targets[d.Arn] = resyncTarget(oi, tgt.Arn, tgt.ResetID, tgt.ResetBeforeDate, tgtStatuses[tgt.Arn])
	}
	return
}

func targetResetHeader(arn string) string {
	return fmt.Sprintf("%s-%s", ReservedMetadataPrefixLower+ReplicationReset, arn)
}

func resyncTarget(oi ObjectInfo, arn string, resetID string, resetBeforeDate time.Time, tgtStatus replication.StatusType) (rd ResyncTargetDecision) {
	rd = ResyncTargetDecision{
		ResetID:         resetID,
		ResetBeforeDate: resetBeforeDate,
	}
	rs, ok := oi.UserDefined[targetResetHeader(arn)]
	if !ok {
		rs, ok = oi.UserDefined[xhttp.MinIOReplicationResetStatus] //for backward compatibility
	}
	if !ok { // existing object replication is enabled and object version is unreplicated so far.
		if resetID != "" && oi.ModTime.Before(resetBeforeDate) { // trigger replication if `mc replicate reset` requested
			rd.Replicate = true
			return
		}
		// For existing object reset - this condition is needed
		rd.Replicate = tgtStatus == ""
		return
	}
	if resetID == "" || resetBeforeDate.Equal(timeSentinel) { // no reset in progress
		return
	}

	// if already replicated, return true if a new reset was requested.
	splits := strings.SplitN(rs, ";", 2)
	if len(splits) != 2 {
		return
	}
	newReset := splits[1] != resetID
	if !newReset && tgtStatus == replication.Completed {
		// already replicated and no reset requested
		return
	}
	rd.Replicate = newReset && oi.ModTime.Before(resetBeforeDate)
	return
}
