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
	minio "github.com/minio/minio-go/v7"
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
	iampolicy "github.com/minio/pkg/iam/policy"
)

// gets replication config associated to a given bucket name.
func getReplicationConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketReplicationConfigNotFound{Bucket: bucketName}
	}

	return globalBucketMetadataSys.GetReplicationConfig(ctx, bucketName)
}

// validateReplicationDestination returns error if replication destination bucket missing or not configured
// It also returns true if replication destination is same as this server.
func validateReplicationDestination(ctx context.Context, bucket string, rCfg *replication.Config) (bool, error) {
	arn, err := madmin.ParseARN(rCfg.RoleArn)
	if err != nil {
		return false, BucketRemoteArnInvalid{}
	}
	if arn.Type != madmin.ReplicationService {
		return false, BucketRemoteArnTypeInvalid{}
	}
	clnt := globalBucketTargetSys.GetRemoteTargetClient(ctx, rCfg.RoleArn)
	if clnt == nil {
		return false, BucketRemoteTargetNotFound{Bucket: bucket}
	}
	if found, _ := clnt.BucketExists(ctx, rCfg.GetDestination().Bucket); !found {
		return false, BucketRemoteDestinationNotFound{Bucket: rCfg.GetDestination().Bucket}
	}
	if ret, err := globalBucketObjectLockSys.Get(bucket); err == nil {
		if ret.LockEnabled {
			lock, _, _, _, err := clnt.GetObjectLockConfig(ctx, rCfg.GetDestination().Bucket)
			if err != nil || lock != "Enabled" {
				return false, BucketReplicationDestinationMissingLock{Bucket: rCfg.GetDestination().Bucket}
			}
		}
	}
	// validate replication ARN against target endpoint
	c, ok := globalBucketTargetSys.arnRemotesMap[rCfg.RoleArn]
	if ok {
		if c.EndpointURL().String() == clnt.EndpointURL().String() {
			sameTarget, _ := isLocalHost(clnt.EndpointURL().Hostname(), clnt.EndpointURL().Port(), globalMinioPort)
			return sameTarget, nil
		}
	}
	return false, BucketRemoteTargetNotFound{Bucket: bucket}
}

type mustReplicateOptions struct {
	meta   map[string]string
	status replication.StatusType
	opType replication.Type
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
func getMustReplicateOptions(o ObjectInfo, op replication.Type) mustReplicateOptions {
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
		meta:   meta,
		status: o.ReplicationStatus,
		opType: op,
	}
}

// mustReplicate returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicate(ctx context.Context, r *http.Request, bucket, object string, opts mustReplicateOptions) (replicate bool, sync bool) {
	if s3Err := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, "", r, iampolicy.GetReplicationConfigurationAction); s3Err != ErrNone {
		return
	}
	return mustReplicater(ctx, bucket, object, opts)
}

// mustReplicater returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicater(ctx context.Context, bucket, object string, mopts mustReplicateOptions) (replicate bool, sync bool) {
	if globalIsGateway {
		return replicate, sync
	}
	replStatus := mopts.ReplicationStatus()
	if replStatus == replication.Replica && !mopts.isMetadataReplication() {
		return replicate, sync
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return replicate, sync
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
	tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	// the target online status should not be used here while deciding
	// whether to replicate as the target could be temporarily down
	if tgt != nil {
		return cfg.Replicate(opts), tgt.replicateSync
	}
	return cfg.Replicate(opts), false
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
func checkReplicateDelete(ctx context.Context, bucket string, dobj ObjectToDelete, oi ObjectInfo, gerr error) (replicate, sync bool) {
	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || rcfg == nil {
		return false, sync
	}
	opts := replication.ObjectOpts{
		Name:         dobj.ObjectName,
		SSEC:         crypto.SSEC.IsEncrypted(oi.UserDefined),
		UserTags:     oi.UserTags,
		DeleteMarker: oi.DeleteMarker,
		VersionID:    dobj.VersionID,
		OpType:       replication.DeleteReplicationType,
	}
	replicate = rcfg.Replicate(opts)
	// when incoming delete is removal of a delete marker( a.k.a versioned delete),
	// GetObjectInfo returns extra information even though it returns errFileNotFound
	if gerr != nil {
		validReplStatus := false
		switch oi.ReplicationStatus {
		case replication.Pending, replication.Completed, replication.Failed:
			validReplStatus = true
		}
		if oi.DeleteMarker && (validReplStatus || replicate) {
			return true, sync
		}
		// can be the case that other cluster is down and duplicate `mc rm --vid`
		// is issued - this still needs to be replicated back to the other target
		return oi.VersionPurgeStatus == Pending || oi.VersionPurgeStatus == Failed, sync
	}
	tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, rcfg.RoleArn)
	// the target online status should not be used here while deciding
	// whether to replicate deletes as the target could be temporarily down
	if tgt == nil {
		return false, false
	}
	return replicate, tgt.replicateSync
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
func replicateDelete(ctx context.Context, dobj DeletedObjectReplicationInfo, objectAPI ObjectLayer) {
	bucket := dobj.Bucket
	versionID := dobj.DeleteMarkerVersionID
	if versionID == "" {
		versionID = dobj.VersionID
	}

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

	tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, rcfg.RoleArn)
	if tgt == nil {
		logger.LogIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, rcfg.RoleArn))
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

	rmErr := tgt.RemoveObject(ctx, rcfg.GetDestination().Bucket, dobj.ObjectName, miniogo.RemoveObjectOptions{
		VersionID: versionID,
		Internal: miniogo.AdvancedRemoveOptions{
			ReplicationDeleteMarker: dobj.DeleteMarkerVersionID != "",
			ReplicationMTime:        dobj.DeleteMarkerMTime.Time,
			ReplicationStatus:       miniogo.ReplicationStatusReplica,
			ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
		},
	})

	replicationStatus := dobj.DeleteMarkerReplicationStatus
	versionPurgeStatus := dobj.VersionPurgeStatus

	if rmErr != nil {
		if dobj.VersionID == "" {
			replicationStatus = string(replication.Failed)
		} else {
			versionPurgeStatus = Failed
		}
		logger.LogIf(ctx, fmt.Errorf("Unable to replicate delete marker to %s/%s(%s): %s", rcfg.GetDestination().Bucket, dobj.ObjectName, versionID, rmErr))
	} else {
		if dobj.VersionID == "" {
			replicationStatus = string(replication.Completed)
		} else {
			versionPurgeStatus = Complete
		}
	}
	prevStatus := dobj.DeleteMarkerReplicationStatus
	currStatus := replicationStatus
	if dobj.VersionID != "" {
		prevStatus = string(dobj.VersionPurgeStatus)
		currStatus = string(versionPurgeStatus)
	}
	// to decrement pending count later.
	globalReplicationStats.Update(dobj.Bucket, 0, replication.StatusType(currStatus), replication.StatusType(prevStatus), replication.DeleteReplicationType)

	var eventName = event.ObjectReplicationComplete
	if replicationStatus == string(replication.Failed) || versionPurgeStatus == Failed {
		eventName = event.ObjectReplicationFailed
	}

	// Update metadata on the delete marker or purge permanent delete if replication success.
	dobjInfo, err := objectAPI.DeleteObject(ctx, bucket, dobj.ObjectName, ObjectOptions{
		VersionID:                     versionID,
		DeleteMarkerReplicationStatus: replicationStatus,
		VersionPurgeStatus:            versionPurgeStatus,
		Versioned:                     globalBucketVersioningSys.Enabled(bucket),
		VersionSuspended:              globalBucketVersioningSys.Suspended(bucket),
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

func getCopyObjMetadata(oi ObjectInfo, dest replication.Destination) map[string]string {
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

	sc := dest.StorageClass
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

func putReplicationOpts(ctx context.Context, dest replication.Destination, objInfo ObjectInfo) (putOpts miniogo.PutObjectOptions, err error) {
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

	sc := dest.StorageClass
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
	}
	if lhold, ok := lkMap.Lookup(xhttp.AmzObjectLockLegalHold); ok {
		putOpts.LegalHold = miniogo.LegalHoldStatus(lhold)
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
		if strings.ToLower(k1) == strings.ToLower(k2) {
			return true
		}
	}
	return false
}

// returns replicationAction by comparing metadata between source and target
func getReplicationAction(oi1 ObjectInfo, oi2 minio.ObjectInfo) replicationAction {
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
func replicateObject(ctx context.Context, ri ReplicateObjectInfo, objectAPI ObjectLayer) {
	auditLogInternal(context.Background(), ri.Bucket, ri.Name, AuditLogOptions{
		Trigger:   ReplicationIncomingActivity,
		APIName:   "s3:ReplicateObject",
		VersionID: ri.VersionID,
		Status:    ri.ReplicationStatus.String(),
	})
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
	tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	if tgt == nil {
		logger.LogIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, cfg.RoleArn))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return
	}
	var closeOnDefer bool
	gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{
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
	size, err := objInfo.GetActualSize()
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

	dest := cfg.GetDestination()
	if dest.Bucket == "" {
		logger.LogIf(ctx, fmt.Errorf("Unable to replicate object %s(%s), bucket is empty", objInfo.Name, objInfo.VersionID))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
		return
	}

	rtype := replicateAll
	oi, err := tgt.StatObject(ctx, dest.Bucket, object, miniogo.StatObjectOptions{
		VersionID: objInfo.VersionID,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "false",
		}})
	if err == nil {
		rtype = getReplicationAction(objInfo, oi)
		if rtype == replicateNone {
			// object with same VersionID already exists, replication kicked off by
			// PutObject might have completed
			if objInfo.ReplicationStatus == replication.Pending || objInfo.ReplicationStatus == replication.Failed {
				// if metadata is not updated for some reason after replication, such as 503 encountered while updating metadata - make sure
				// to set ReplicationStatus as Completed.Note that replication Stats would have been updated despite metadata update failure.
				gr.Close()
				closeOnDefer = false
				popts := ObjectOptions{
					MTime:       objInfo.ModTime,
					VersionID:   objInfo.VersionID,
					UserDefined: make(map[string]string, len(objInfo.UserDefined)),
				}
				for k, v := range objInfo.UserDefined {
					popts.UserDefined[k] = v
				}
				popts.UserDefined[xhttp.AmzBucketReplicationStatus] = replication.Completed.String()
				if objInfo.UserTags != "" {
					popts.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
				}
				if _, err = objectAPI.PutObjectMetadata(ctx, bucket, object, popts); err != nil {
					logger.LogIf(ctx, fmt.Errorf("Unable to update replication metadata for %s/%s(%s): %w", bucket, objInfo.Name, objInfo.VersionID, err))
				} else {
					auditLogInternal(context.Background(), ri.Bucket, ri.Name, AuditLogOptions{
						Trigger:   ReplicationIncomingActivity,
						APIName:   "s3:ReplicateObject",
						VersionID: ri.VersionID,
						Status:    ri.ReplicationStatus.String(),
					})
				}
			}
			return
		}
	}
	replicationStatus := replication.Completed
	// use core client to avoid doing multipart on PUT
	c := &miniogo.Core{Client: tgt.Client}
	if rtype != replicateAll {
		// replicate metadata for object tagging/copy with metadata replacement
		srcOpts := miniogo.CopySrcOptions{
			Bucket:    dest.Bucket,
			Object:    object,
			VersionID: objInfo.VersionID,
		}
		dstOpts := miniogo.PutObjectOptions{
			Internal: miniogo.AdvancedPutOptions{
				SourceVersionID:    objInfo.VersionID,
				ReplicationRequest: true, // always set this to distinguish between `mc mirror` replication and serverside
			}}
		if _, err = c.CopyObject(ctx, dest.Bucket, object, dest.Bucket, object, getCopyObjMetadata(objInfo, dest), srcOpts, dstOpts); err != nil {
			replicationStatus = replication.Failed
			logger.LogIf(ctx, fmt.Errorf("Unable to replicate metadata for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
		}
	} else {
		putOpts, err := putReplicationOpts(ctx, dest, objInfo)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("failed to get target for replication bucket:%s cfg:%s err:%w", bucket, cfg.RoleArn, err))
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
		newCtx, cancel := context.WithTimeout(ctx, globalOperationTimeout.Timeout())
		defer cancel()
		r := bandwidth.NewMonitoredReader(newCtx, globalBucketMonitor, gr, opts)
		if len(objInfo.Parts) > 1 {
			if uploadID, err := replicateObjectWithMultipart(ctx, c, dest.Bucket, object, r, objInfo, putOpts); err != nil {
				replicationStatus = replication.Failed
				logger.LogIf(ctx, fmt.Errorf("Unable to replicate for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
				defer c.AbortMultipartUpload(ctx, dest.Bucket, object, uploadID)
			}
		} else {
			if _, err = c.PutObject(ctx, dest.Bucket, object, r, size, "", "", putOpts); err != nil {
				replicationStatus = replication.Failed
				logger.LogIf(ctx, fmt.Errorf("Unable to replicate for object %s/%s(%s): %s", bucket, objInfo.Name, objInfo.VersionID, err))
			}
		}
	}
	gr.Close()
	closeOnDefer = false

	prevReplStatus := objInfo.ReplicationStatus
	objInfo.UserDefined[xhttp.AmzBucketReplicationStatus] = replicationStatus.String()
	if objInfo.UserTags != "" {
		objInfo.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
	}
	if ri.OpType == replication.ExistingObjectReplicationType {
		objInfo.UserDefined[xhttp.MinIOReplicationResetStatus] = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), ri.ResetID)
	}
	// FIXME: add support for missing replication events
	// - event.ObjectReplicationMissedThreshold
	// - event.ObjectReplicationReplicatedAfterThreshold
	var eventName = event.ObjectReplicationComplete
	if replicationStatus == replication.Failed {
		eventName = event.ObjectReplicationFailed
	}

	// Leave metadata in `PENDING` state if inline replication fails to save iops
	if ri.OpType == replication.HealReplicationType ||
		replicationStatus == replication.Completed {
		popts := ObjectOptions{
			MTime:       objInfo.ModTime,
			VersionID:   objInfo.VersionID,
			UserDefined: make(map[string]string, len(objInfo.UserDefined)),
		}
		for k, v := range objInfo.UserDefined {
			popts.UserDefined[k] = v
		}
		popts.UserDefined[xhttp.AmzBucketReplicationStatus] = replication.Completed.String()
		if objInfo.UserTags != "" {
			popts.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
		}
		if _, err = objectAPI.PutObjectMetadata(ctx, bucket, object, popts); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to update replication metadata for %s/%s(%s): %w", bucket, objInfo.Name, objInfo.VersionID, err))
		} else {
			auditLogInternal(context.Background(), objInfo.Bucket, objInfo.Name, AuditLogOptions{
				Trigger:   ReplicationIncomingActivity,
				APIName:   "s3:ReplicateObject",
				VersionID: objInfo.VersionID,
				Status:    replicationStatus.String(),
			})
		}

		opType := replication.MetadataReplicationType
		if rtype == replicateAll {
			opType = replication.ObjectReplicationType
		}
		globalReplicationStats.Update(bucket, size, replicationStatus, prevReplStatus, opType)
		sendEvent(eventArgs{
			EventName:  eventName,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
	}
	// re-queue failures once more - keep a retry count to avoid flooding the queue if
	// the target site is down. Leave it to scanner to catch up instead.
	if replicationStatus != replication.Completed && ri.RetryCount < 1 {
		ri.OpType = replication.HealReplicationType
		ri.RetryCount++
		globalReplicationPool.queueReplicaFailedTask(ri)
	}
}

func replicateObjectWithMultipart(ctx context.Context, c *miniogo.Core, bucket, object string, r io.Reader, objInfo ObjectInfo, opts miniogo.PutObjectOptions) (uploadID string, err error) {
	var uploadedParts []miniogo.CompletePart
	uploadID, err = c.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		return
	}
	var (
		hr    *hash.Reader
		pInfo miniogo.ObjectPart
	)
	for _, partInfo := range objInfo.Parts {
		hr, err = hash.NewReader(r, partInfo.Size, "", "", partInfo.Size)
		if err != nil {
			return
		}
		pInfo, err = c.PutObjectPart(ctx, bucket, object, uploadID, partInfo.Number, hr, partInfo.Size, "", "", opts.ServerSideEncryption)
		if err != nil {
			return
		}
		if pInfo.Size != partInfo.Size {
			return uploadID, fmt.Errorf("Part size mismatch: got %d, want %d", pInfo.Size, partInfo.Size)
		}
		uploadedParts = append(uploadedParts, miniogo.CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	_, err = c.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, miniogo.PutObjectOptions{Internal: miniogo.AdvancedPutOptions{
		SourceMTime:        objInfo.ModTime,
		ReplicationRequest: true, // always set this to distinguish between `mc mirror` replication and serverside
	}})
	return
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
	Bucket  string
	OpType  replication.Type
	ResetID string
}

const (
	// ReplicationQueuedActivity - replication being queued activity trail
	ReplicationQueuedActivity = "replication:queue"
	// ReplicationExistingActivity - activity trail for existing objects replication
	ReplicationExistingActivity = "replication:existing"
	// ReplicationMRFActivity - activity trail for replication from Most Recent Failures (MRF) queue
	ReplicationMRFActivity = "replication:mrf"
	// ReplicationIncomingActivity - activity trail indicating replication started [could be from incoming/existing/heal activity]
	ReplicationIncomingActivity = "replication:incoming"
	// ReplicationHealActivity - activity trail for healing of failed/pending replications
	ReplicationHealActivity = "replication:heal"
	// ReplicationDeleteActivity - activity trail for delete replication
	ReplicationDeleteActivity = "replication:delete"
	// ReplicationExistingDeleteActivity - activity trail for delete replication triggered for existing delete markers
	ReplicationExistingDeleteActivity = "replication:delete:existing"
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
			replicateObject(p.ctx, oi, p.objLayer)
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
			replicateObject(p.ctx, oi, p.objLayer)
		case doi, ok := <-p.replicaDeleteCh:
			if !ok {
				return
			}
			replicateDelete(p.ctx, doi, p.objLayer)
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
			replicateObject(p.ctx, oi, p.objLayer)
		case doi, ok := <-p.existingReplicaDeleteCh:
			if !ok {
				return
			}
			replicateDelete(p.ctx, doi, p.objLayer)
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
		auditLogInternal(context.Background(), ri.Bucket, ri.Name, AuditLogOptions{
			Trigger:   ReplicationMRFActivity,
			APIName:   "s3:ReplicateObject",
			VersionID: ri.VersionID,
			Status:    ri.ReplicationStatus.String(),
		})
	default:
	}
}

func (p *ReplicationPool) queueReplicaTask(ri ReplicateObjectInfo) {
	if p == nil {
		return
	}
	var ch chan ReplicateObjectInfo
	trigger := ReplicationQueuedActivity
	switch ri.OpType {
	case replication.ExistingObjectReplicationType:
		ch = p.existingReplicaCh
		trigger = ReplicationExistingActivity
	case replication.HealReplicationType:
		ch = p.replicaCh
		trigger = ReplicationHealActivity
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
		auditLogInternal(context.Background(), ri.Bucket, ri.Name, AuditLogOptions{
			Trigger:   trigger,
			APIName:   "s3:ReplicateObject",
			VersionID: ri.VersionID,
			Status:    string(ri.ReplicationStatus),
		})
	default:
	}
}

func (p *ReplicationPool) queueReplicaDeleteTask(doi DeletedObjectReplicationInfo) {
	if p == nil {
		return
	}
	trigger := ReplicationDeleteActivity
	var ch chan DeletedObjectReplicationInfo
	switch doi.OpType {
	case replication.ExistingObjectReplicationType:
		ch = p.existingReplicaDeleteCh
		trigger = ReplicationExistingDeleteActivity
	case replication.HealReplicationType:
		ch = p.replicaDeleteCh
		trigger = ReplicationHealActivity
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
		replStatus := doi.DeleteMarkerReplicationStatus
		if doi.VersionPurgeStatus != "" {
			replStatus = string(doi.VersionPurgeStatus)
		}
		auditLogInternal(context.Background(), doi.Bucket, doi.ObjectName, AuditLogOptions{
			Trigger:   trigger,
			APIName:   "s3:ReplicateDelete",
			VersionID: doi.VersionID,
			Status:    replStatus,
		})
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
}

// get Reader from replication target if active-active replication is in place and
// this node returns a 404
func proxyGetToReplicationTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, proxy bool) {
	tgt, oi, proxy, err := proxyHeadToRepTarget(ctx, bucket, object, opts)
	if !proxy || err != nil {
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

// isProxyable returns true if replication config found for this bucket
func isProxyable(ctx context.Context, bucket string) bool {
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return false
	}
	dest := cfg.GetDestination()
	return dest.Bucket == bucket
}

func proxyHeadToRepTarget(ctx context.Context, bucket, object string, opts ObjectOptions) (tgt *TargetClient, oi ObjectInfo, proxy bool, err error) {
	// this option is set when active-active replication is in place between site A -> B,
	// and site B does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return nil, oi, false, nil
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return nil, oi, false, err
	}
	dest := cfg.GetDestination()
	if dest.Bucket != bucket { // not active-active
		return nil, oi, false, err
	}
	ssec := false
	if opts.ServerSideEncryption != nil {
		ssec = opts.ServerSideEncryption.Type() == encrypt.SSEC
	}
	ropts := replication.ObjectOpts{
		Name: object,
		SSEC: ssec,
	}
	if !cfg.Replicate(ropts) { // no matching rule for object prefix
		return nil, oi, false, nil
	}
	tgt = globalBucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	if tgt == nil {
		return nil, oi, false, fmt.Errorf("target is offline or not configured")
	}
	// if proxying explicitly disabled on remote target
	if tgt.disableProxy {
		return nil, oi, false, nil
	}
	gopts := miniogo.GetObjectOptions{
		VersionID:            opts.VersionID,
		ServerSideEncryption: opts.ServerSideEncryption,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "true",
		},
	}

	objInfo, err := tgt.StatObject(ctx, dest.Bucket, object, gopts)
	if err != nil {
		return nil, oi, false, err
	}

	tags, _ := tags.MapToObjectTags(objInfo.UserTags)
	oi = ObjectInfo{
		Bucket:            bucket,
		Name:              object,
		ModTime:           objInfo.LastModified,
		Size:              objInfo.Size,
		ETag:              objInfo.ETag,
		VersionID:         objInfo.VersionID,
		IsLatest:          objInfo.IsLatest,
		DeleteMarker:      objInfo.IsDeleteMarker,
		ContentType:       objInfo.ContentType,
		Expires:           objInfo.Expires,
		StorageClass:      objInfo.StorageClass,
		ReplicationStatus: replication.StatusType(objInfo.ReplicationStatus),
		UserTags:          tags.String(),
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
	return tgt, oi, true, nil
}

// get object info from replication target if active-active replication is in place and
// this node returns a 404
func proxyHeadToReplicationTarget(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, proxy bool, err error) {
	_, oi, proxy, err = proxyHeadToRepTarget(ctx, bucket, object, opts)
	return oi, proxy, err
}

func scheduleReplication(ctx context.Context, objInfo ObjectInfo, o ObjectLayer, sync bool, opType replication.Type) {
	if sync {
		replicateObject(ctx, ReplicateObjectInfo{ObjectInfo: objInfo, OpType: opType}, o)
	} else {
		globalReplicationPool.queueReplicaTask(ReplicateObjectInfo{ObjectInfo: objInfo, OpType: opType})
	}
	if sz, err := objInfo.GetActualSize(); err == nil {
		globalReplicationStats.Update(objInfo.Bucket, sz, objInfo.ReplicationStatus, replication.StatusType(""), opType)
	}
}

func scheduleReplicationDelete(ctx context.Context, dv DeletedObjectReplicationInfo, o ObjectLayer, sync bool) {
	globalReplicationPool.queueReplicaDeleteTask(dv)
	globalReplicationStats.Update(dv.Bucket, 0, replication.Pending, replication.StatusType(""), replication.DeleteReplicationType)
}

type replicationConfig struct {
	Config          *replication.Config
	ResetID         string
	ResetBeforeDate time.Time
}

func (c replicationConfig) Empty() bool {
	return c.Config == nil
}
func (c replicationConfig) Replicate(opts replication.ObjectOpts) bool {
	return c.Config.Replicate(opts)
}

// Resync returns true if replication reset is requested
func (c replicationConfig) Resync(ctx context.Context, oi ObjectInfo) bool {
	if c.Empty() {
		return false
	}
	// existing object replication does not apply to un-versioned objects
	if oi.VersionID == "" || oi.VersionID == nullVersionID {
		return false
	}

	var replicate bool
	if oi.DeleteMarker {
		if c.Replicate(replication.ObjectOpts{
			Name:           oi.Name,
			SSEC:           crypto.SSEC.IsEncrypted(oi.UserDefined),
			UserTags:       oi.UserTags,
			DeleteMarker:   oi.DeleteMarker,
			VersionID:      oi.VersionID,
			OpType:         replication.DeleteReplicationType,
			ExistingObject: true}) {
			replicate = true
		}
	} else {
		// Ignore previous replication status when deciding if object can be re-replicated
		objInfo := oi.Clone()
		objInfo.ReplicationStatus = replication.StatusType("")
		replicate, _ = mustReplicater(ctx, oi.Bucket, oi.Name, getMustReplicateOptions(objInfo, replication.ExistingObjectReplicationType))
	}
	return c.resync(oi, replicate)
}

// wrapper function for testability. Returns true if a new reset is requested on
// already replicated objects OR object qualifies for existing object replication
// and no reset requested.
func (c replicationConfig) resync(oi ObjectInfo, replicate bool) bool {
	if !replicate {
		return false
	}
	rs, ok := oi.UserDefined[xhttp.MinIOReplicationResetStatus]
	if !ok { // existing object replication is enabled and object version is unreplicated so far.
		if c.ResetID != "" && oi.ModTime.Before(c.ResetBeforeDate) { // trigger replication if `mc replicate reset` requested
			return true
		}
		return oi.ReplicationStatus != replication.Completed
	}
	if c.ResetID == "" || c.ResetBeforeDate.Equal(timeSentinel) { // no reset in progress
		return false
	}
	// if already replicated, return true if a new reset was requested.
	splits := strings.SplitN(rs, ";", 2)
	newReset := splits[1] != c.ResetID
	if !newReset && oi.ReplicationStatus == replication.Completed {
		// already replicated and no reset requested
		return false
	}
	return newReset && oi.ModTime.Before(c.ResetBeforeDate)
}
