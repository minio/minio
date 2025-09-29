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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/bucket/bandwidth"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/tinylib/msgp/msgp"
	"github.com/zeebo/xxh3"
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

	// ReplicationSsecChecksumHeader - the encrypted checksum of the SSE-C encrypted object.
	ReplicationSsecChecksumHeader = "X-Minio-Replication-Ssec-Crc"
)

// gets replication config associated to a given bucket name.
func getReplicationConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	rCfg, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucketName)
	if err != nil && !errors.Is(err, BucketReplicationConfigNotFound{Bucket: bucketName}) {
		return rCfg, err
	}
	return rCfg, nil
}

// validateReplicationDestination returns error if replication destination bucket missing or not configured
// It also returns true if replication destination is same as this server.
func validateReplicationDestination(ctx context.Context, bucket string, rCfg *replication.Config, opts *validateReplicationDestinationOptions) (bool, APIError) {
	if opts == nil {
		opts = &validateReplicationDestinationOptions{}
	}
	var arns []string
	if rCfg.RoleArn != "" {
		arns = append(arns, rCfg.RoleArn)
	} else {
		for _, rule := range rCfg.Rules {
			arns = append(arns, rule.Destination.String())
		}
	}
	var sameTarget bool
	for _, arnStr := range arns {
		arn, err := madmin.ParseARN(arnStr)
		if err != nil {
			return sameTarget, errorCodes.ToAPIErrWithErr(ErrBucketRemoteArnInvalid, err)
		}
		if arn.Type != madmin.ReplicationService {
			return sameTarget, toAPIError(ctx, BucketRemoteArnTypeInvalid{Bucket: bucket})
		}
		clnt := globalBucketTargetSys.GetRemoteTargetClient(bucket, arnStr)
		if clnt == nil {
			return sameTarget, toAPIError(ctx, BucketRemoteTargetNotFound{Bucket: bucket})
		}
		if opts.CheckRemoteBucket { // validate remote bucket
			found, err := clnt.BucketExists(ctx, arn.Bucket)
			if err != nil {
				return sameTarget, errorCodes.ToAPIErrWithErr(ErrRemoteDestinationNotFoundError, err)
			}
			if !found {
				return sameTarget, errorCodes.ToAPIErrWithErr(ErrRemoteDestinationNotFoundError, BucketRemoteTargetNotFound{Bucket: arn.Bucket})
			}
			if ret, err := globalBucketObjectLockSys.Get(bucket); err == nil {
				if ret.LockEnabled {
					lock, _, _, _, err := clnt.GetObjectLockConfig(ctx, arn.Bucket)
					if err != nil {
						return sameTarget, errorCodes.ToAPIErrWithErr(ErrReplicationDestinationMissingLock, err)
					}
					if lock != objectlock.Enabled {
						return sameTarget, errorCodes.ToAPIErrWithErr(ErrReplicationDestinationMissingLock, nil)
					}
				}
			}
		}
		// if checked bucket, then check the ready is unnecessary
		if !opts.CheckRemoteBucket && opts.CheckReady {
			endpoint := clnt.EndpointURL().String()
			if errInt, ok := opts.checkReadyErr.Load(endpoint); !ok {
				err = checkRemoteEndpoint(ctx, clnt.EndpointURL())
				opts.checkReadyErr.Store(endpoint, err)
			} else {
				if errInt == nil {
					err = nil
				} else {
					err, _ = errInt.(error)
				}
			}
			switch err.(type) {
			case BucketRemoteIdenticalToSource:
				return true, errorCodes.ToAPIErrWithErr(ErrBucketRemoteIdenticalToSource, fmt.Errorf("remote target endpoint %s is self referential", clnt.EndpointURL().String()))
			default:
			}
		}
		// validate replication ARN against target endpoint
		selfTarget, _ := isLocalHost(clnt.EndpointURL().Hostname(), clnt.EndpointURL().Port(), globalMinioPort)
		if !sameTarget {
			sameTarget = selfTarget
		}
	}

	if len(arns) == 0 {
		return false, toAPIError(ctx, BucketRemoteTargetNotFound{Bucket: bucket})
	}
	return sameTarget, toAPIError(ctx, nil)
}

// performs a http request to remote endpoint to check if deployment id of remote endpoint is same as
// local cluster deployment id. This is to prevent replication to self, especially in case of a loadbalancer
// in front of MinIO.
func checkRemoteEndpoint(ctx context.Context, epURL *url.URL) error {
	reqURL := &url.URL{
		Scheme: epURL.Scheme,
		Host:   epURL.Host,
		Path:   healthCheckPathPrefix + healthCheckReadinessPath,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{
		Transport: globalRemoteTargetTransport,
		Timeout:   10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if err == nil {
		// Drain the connection.
		xhttp.DrainBody(resp.Body)
	}
	if resp != nil {
		amzid := resp.Header.Get(xhttp.AmzRequestHostID)
		if _, ok := globalNodeNamesHex[amzid]; ok {
			return BucketRemoteIdenticalToSource{
				Endpoint: epURL.String(),
			}
		}
	}
	return nil
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

func (o ObjectInfo) getMustReplicateOptions(op replication.Type, opts ObjectOptions) mustReplicateOptions {
	return getMustReplicateOptions(o.UserDefined, o.UserTags, o.ReplicationStatus, op, opts)
}

func getMustReplicateOptions(userDefined map[string]string, userTags string, status replication.StatusType, op replication.Type, opts ObjectOptions) mustReplicateOptions {
	meta := cloneMSS(userDefined)
	if userTags != "" {
		meta[xhttp.AmzObjectTagging] = userTags
	}

	return mustReplicateOptions{
		meta:               meta,
		status:             status,
		opType:             op,
		replicationRequest: opts.ReplicationRequest,
	}
}

// mustReplicate returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicate(ctx context.Context, bucket, object string, mopts mustReplicateOptions) (dsc ReplicateDecision) {
	// object layer not initialized we return with no decision.
	if newObjectLayerFn() == nil {
		return dsc
	}

	// Disable server-side replication on object prefixes which are excluded
	// from versioning via the MinIO bucket versioning extension.
	if !globalBucketVersioningSys.PrefixEnabled(bucket, object) {
		return dsc
	}

	replStatus := mopts.ReplicationStatus()
	if replStatus == replication.Replica && !mopts.isMetadataReplication() {
		return dsc
	}

	if mopts.replicationRequest { // incoming replication request on target cluster
		return dsc
	}

	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		replLogOnceIf(ctx, err, bucket)
		return dsc
	}
	if cfg == nil {
		return dsc
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
		tgt := globalBucketTargetSys.GetRemoteTargetClient(bucket, tgtArn)
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
		replLogOnceIf(ctx, err, bucket)
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
		replLogOnceIf(ctx, err, bucket)
		return dsc
	}
	// If incoming request is a replication request, it does not need to be re-replicated.
	if delOpts.ReplicationRequest {
		return dsc
	}
	// Skip replication if this object's prefix is excluded from being
	// versioned.
	if !delOpts.Versioned {
		return dsc
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
	dsc.targetsMap = make(map[string]replicateTargetDecision, len(tgtArns))
	if len(tgtArns) == 0 {
		return dsc
	}
	var sync, replicate bool
	for _, tgtArn := range tgtArns {
		opts.TargetArn = tgtArn
		replicate = rcfg.Replicate(opts)
		// when incoming delete is removal of a delete marker(a.k.a versioned delete),
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
			}
			// can be the case that other cluster is down and duplicate `mc rm --vid`
			// is issued - this still needs to be replicated back to the other target
			if !oi.VersionPurgeStatus.Empty() {
				replicate = oi.VersionPurgeStatus == replication.VersionPurgePending || oi.VersionPurgeStatus == replication.VersionPurgeFailed
				dsc.Set(newReplicateTargetDecision(tgtArn, replicate, sync))
			}
			continue
		}
		tgt := globalBucketTargetSys.GetRemoteTargetClient(bucket, tgtArn)
		// the target online status should not be used here while deciding
		// whether to replicate deletes as the target could be temporarily down
		tgtDsc := newReplicateTargetDecision(tgtArn, false, false)
		if tgt != nil {
			tgtDsc = newReplicateTargetDecision(tgtArn, replicate, tgt.replicateSync)
		}
		dsc.Set(tgtDsc)
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
func replicateDelete(ctx context.Context, dobj DeletedObjectReplicationInfo, objectAPI ObjectLayer) {
	var replicationStatus replication.StatusType
	bucket := dobj.Bucket
	versionID := dobj.DeleteMarkerVersionID
	if versionID == "" {
		versionID = dobj.VersionID
	}

	defer func() {
		replStatus := string(replicationStatus)
		auditLogInternal(context.Background(), AuditLogOptions{
			Event:     dobj.EventType,
			APIName:   ReplicateDeleteAPI,
			Bucket:    bucket,
			Object:    dobj.ObjectName,
			VersionID: versionID,
			Status:    replStatus,
		})
	}()

	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || rcfg == nil {
		replLogOnceIf(ctx, fmt.Errorf("unable to obtain replication config for bucket: %s: err: %s", bucket, err), bucket)
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			UserAgent: "Internal: [Replication]",
			Host:      globalLocalNodeName,
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}
	dsc, err := parseReplicateDecision(ctx, bucket, dobj.ReplicationState.ReplicateDecisionStr)
	if err != nil {
		replLogOnceIf(ctx, fmt.Errorf("unable to parse replication decision parameters for bucket: %s, err: %s, decision: %s",
			bucket, err, dobj.ReplicationState.ReplicateDecisionStr), dobj.ReplicationState.ReplicateDecisionStr)
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			UserAgent: "Internal: [Replication]",
			Host:      globalLocalNodeName,
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}

	// Lock the object name before starting replication operation.
	// Use separate lock that doesn't collide with regular objects.
	lk := objectAPI.NewNSLock(bucket, "/[replicate]/"+dobj.ObjectName)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		globalReplicationPool.Get().queueMRFSave(dobj.ToMRFEntry())
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			UserAgent: "Internal: [Replication]",
			Host:      globalLocalNodeName,
			EventName: event.ObjectReplicationNotTracked,
		})
		return
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	rinfos := replicatedInfos{Targets: make([]replicatedTargetInfo, 0, len(dsc.targetsMap))}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, tgtEntry := range dsc.targetsMap {
		if !tgtEntry.Replicate {
			continue
		}
		// if dobj.TargetArn is not empty string, this is a case of specific target being re-synced.
		if dobj.TargetArn != "" && dobj.TargetArn != tgtEntry.Arn {
			continue
		}
		tgtClnt := globalBucketTargetSys.GetRemoteTargetClient(bucket, tgtEntry.Arn)
		if tgtClnt == nil {
			// Skip stale targets if any and log them to be missing at least once.
			replLogOnceIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, tgtEntry.Arn), tgtEntry.Arn)
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object: ObjectInfo{
					Bucket:       bucket,
					Name:         dobj.ObjectName,
					VersionID:    versionID,
					DeleteMarker: dobj.DeleteMarker,
				},
				UserAgent: "Internal: [Replication]",
				Host:      globalLocalNodeName,
			})
			continue
		}
		wg.Add(1)
		go func(tgt *TargetClient) {
			defer wg.Done()
			tgtInfo := replicateDeleteToTarget(ctx, dobj, tgt)

			mu.Lock()
			rinfos.Targets = append(rinfos.Targets, tgtInfo)
			mu.Unlock()
		}(tgtClnt)
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
			globalReplicationStats.Load().Update(dobj.Bucket, rinfo, replicationStatus,
				prevStatus)
		}
	}

	eventName := event.ObjectReplicationComplete
	if replicationStatus == replication.Failed {
		eventName = event.ObjectReplicationFailed
		globalReplicationPool.Get().queueMRFSave(dobj.ToMRFEntry())
	}
	drs := getReplicationState(rinfos, dobj.ReplicationState, dobj.VersionID)
	if replicationStatus != prevStatus {
		drs.ReplicationTimeStamp = UTCNow()
	}

	dobjInfo, err := objectAPI.DeleteObject(ctx, bucket, dobj.ObjectName, ObjectOptions{
		VersionID:         versionID,
		MTime:             dobj.DeleteMarkerMTime.Time,
		DeleteReplication: drs,
		Versioned:         globalBucketVersioningSys.PrefixEnabled(bucket, dobj.ObjectName),
		// Objects matching prefixes should not leave delete markers,
		// dramatically reduces namespace pollution while keeping the
		// benefits of replication, make sure to apply version suspension
		// only at bucket level instead.
		VersionSuspended: globalBucketVersioningSys.Suspended(bucket),
	})
	if err != nil && !isErrVersionNotFound(err) { // VersionNotFound would be reported by pool that object version is missing on.
		sendEvent(eventArgs{
			BucketName: bucket,
			Object: ObjectInfo{
				Bucket:       bucket,
				Name:         dobj.ObjectName,
				VersionID:    versionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			UserAgent: "Internal: [Replication]",
			Host:      globalLocalNodeName,
			EventName: eventName,
		})
	} else {
		sendEvent(eventArgs{
			BucketName: bucket,
			Object:     dobjInfo,
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
			EventName:  eventName,
		})
	}
}

func replicateDeleteToTarget(ctx context.Context, dobj DeletedObjectReplicationInfo, tgt *TargetClient) (rinfo replicatedTargetInfo) {
	versionID := dobj.DeleteMarkerVersionID
	if versionID == "" {
		versionID = dobj.VersionID
	}

	rinfo = dobj.ReplicationState.targetState(tgt.ARN)
	rinfo.OpType = dobj.OpType
	rinfo.endpoint = tgt.EndpointURL().Host
	rinfo.secure = tgt.EndpointURL().Scheme == "https"
	defer func() {
		if rinfo.ReplicationStatus == replication.Completed && tgt.ResetID != "" && dobj.OpType == replication.ExistingObjectReplicationType {
			rinfo.ResyncTimestamp = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), tgt.ResetID)
		}
	}()

	if dobj.VersionID == "" && rinfo.PrevReplicationStatus == replication.Completed && dobj.OpType != replication.ExistingObjectReplicationType {
		rinfo.ReplicationStatus = rinfo.PrevReplicationStatus
		return rinfo
	}
	if dobj.VersionID != "" && rinfo.VersionPurgeStatus == replication.VersionPurgeComplete {
		return rinfo
	}
	if globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
		replLogOnceIf(ctx, fmt.Errorf("remote target is offline for bucket:%s arn:%s", dobj.Bucket, tgt.ARN), "replication-target-offline-delete-"+tgt.ARN)
		sendEvent(eventArgs{
			BucketName: dobj.Bucket,
			Object: ObjectInfo{
				Bucket:       dobj.Bucket,
				Name:         dobj.ObjectName,
				VersionID:    dobj.VersionID,
				DeleteMarker: dobj.DeleteMarker,
			},
			UserAgent: "Internal: [Replication]",
			Host:      globalLocalNodeName,
			EventName: event.ObjectReplicationNotTracked,
		})
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Failed
		} else {
			rinfo.VersionPurgeStatus = replication.VersionPurgeFailed
		}
		return rinfo
	}
	// early return if already replicated delete marker for existing object replication/ healing delete markers
	if dobj.DeleteMarkerVersionID != "" {
		toi, err := tgt.StatObject(ctx, tgt.Bucket, dobj.ObjectName, minio.StatObjectOptions{
			VersionID: versionID,
			Internal: minio.AdvancedGetOptions{
				ReplicationProxyRequest:           "false",
				IsReplicationReadyForDeleteMarker: true,
			},
		})
		serr := ErrorRespToObjectError(err, dobj.Bucket, dobj.ObjectName, dobj.VersionID)
		switch {
		case isErrMethodNotAllowed(serr):
			// delete marker already replicated
			if dobj.VersionID == "" && rinfo.VersionPurgeStatus.Empty() {
				rinfo.ReplicationStatus = replication.Completed
				return rinfo
			}
		case isErrObjectNotFound(serr), isErrVersionNotFound(serr):
			// version being purged is already not found on target.
			if !rinfo.VersionPurgeStatus.Empty() {
				rinfo.VersionPurgeStatus = replication.VersionPurgeComplete
				return rinfo
			}
		case isErrReadQuorum(serr), isErrWriteQuorum(serr):
			// destination has some quorum issues, perform removeObject() anyways
			// to complete the operation.
		default:
			if err != nil && minio.IsNetworkOrHostDown(err, true) && !globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
				globalBucketTargetSys.markOffline(tgt.EndpointURL())
			}
			// mark delete marker replication as failed if target cluster not ready to receive
			// this request yet (object version not replicated yet)
			if err != nil && !toi.ReplicationReady {
				rinfo.ReplicationStatus = replication.Failed
				rinfo.Err = err
				return rinfo
			}
		}
	}
	rmErr := tgt.RemoveObject(ctx, tgt.Bucket, dobj.ObjectName, minio.RemoveObjectOptions{
		VersionID: versionID,
		Internal: minio.AdvancedRemoveOptions{
			ReplicationDeleteMarker: dobj.DeleteMarkerVersionID != "",
			ReplicationMTime:        dobj.DeleteMarkerMTime.Time,
			ReplicationStatus:       minio.ReplicationStatusReplica,
			ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
		},
	})
	if rmErr != nil {
		rinfo.Err = rmErr
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Failed
		} else {
			rinfo.VersionPurgeStatus = replication.VersionPurgeFailed
		}
		replLogIf(ctx, fmt.Errorf("unable to replicate delete marker to %s: %s/%s(%s): %w", tgt.EndpointURL(), tgt.Bucket, dobj.ObjectName, versionID, rmErr))
		if rmErr != nil && minio.IsNetworkOrHostDown(rmErr, true) && !globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			globalBucketTargetSys.markOffline(tgt.EndpointURL())
		}
	} else {
		if dobj.VersionID == "" {
			rinfo.ReplicationStatus = replication.Completed
		} else {
			rinfo.VersionPurgeStatus = replication.VersionPurgeComplete
		}
	}
	return rinfo
}

func getCopyObjMetadata(oi ObjectInfo, sc string) map[string]string {
	meta := make(map[string]string, len(oi.UserDefined))
	for k, v := range oi.UserDefined {
		if stringsHasPrefixFold(k, ReservedMetadataPrefixLower) {
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

	meta[xhttp.AmzObjectTagging] = oi.UserTags
	meta[xhttp.AmzTagDirective] = "REPLACE"

	if sc == "" {
		sc = oi.StorageClass
	}
	// drop non standard storage classes for tiering from replication
	if sc != "" && (sc == storageclass.RRS || sc == storageclass.STANDARD) {
		meta[xhttp.AmzStorageClass] = sc
	}

	meta[xhttp.MinIOSourceETag] = oi.ETag
	meta[xhttp.MinIOSourceMTime] = oi.ModTime.UTC().Format(time.RFC3339Nano)
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

func putReplicationOpts(ctx context.Context, sc string, objInfo ObjectInfo) (putOpts minio.PutObjectOptions, isMP bool, err error) {
	meta := make(map[string]string)
	isSSEC := crypto.SSEC.IsEncrypted(objInfo.UserDefined)

	for k, v := range objInfo.UserDefined {
		_, isValidSSEHeader := validSSEReplicationHeaders[k]
		// In case of SSE-C objects copy the allowed internal headers as well
		if !isSSEC || !isValidSSEHeader {
			if stringsHasPrefixFold(k, ReservedMetadataPrefixLower) {
				continue
			}
			if isStandardHeader(k) {
				continue
			}
		}
		if isValidSSEHeader {
			meta[validSSEReplicationHeaders[k]] = v
		} else {
			meta[k] = v
		}
	}
	isMP = objInfo.isMultipart()
	if len(objInfo.Checksum) > 0 {
		// Add encrypted CRC to metadata for SSE-C objects.
		if isSSEC {
			meta[ReplicationSsecChecksumHeader] = base64.StdEncoding.EncodeToString(objInfo.Checksum)
		} else {
			cs, mp := getCRCMeta(objInfo, 0, nil)
			// Set object checksum.
			maps.Copy(meta, cs)
			isMP = mp
			if !objInfo.isMultipart() && cs[xhttp.AmzChecksumType] == xhttp.AmzChecksumTypeFullObject {
				// For objects where checksum is full object, it will be the same.
				// Therefore, we use the cheaper PutObject replication.
				isMP = false
			}
		}
	}

	if sc == "" && (objInfo.StorageClass == storageclass.STANDARD || objInfo.StorageClass == storageclass.RRS) {
		sc = objInfo.StorageClass
	}
	putOpts = minio.PutObjectOptions{
		UserMetadata:    meta,
		ContentType:     objInfo.ContentType,
		ContentEncoding: objInfo.ContentEncoding,
		Expires:         objInfo.Expires,
		StorageClass:    sc,
		Internal: minio.AdvancedPutOptions{
			SourceVersionID:    objInfo.VersionID,
			ReplicationStatus:  minio.ReplicationStatusReplica,
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
					return putOpts, false, err
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
		rmode := minio.RetentionMode(mode)
		putOpts.Mode = rmode
	}
	if retainDateStr, ok := lkMap.Lookup(xhttp.AmzObjectLockRetainUntilDate); ok {
		rdate, err := amztime.ISO8601Parse(retainDateStr)
		if err != nil {
			return putOpts, false, err
		}
		putOpts.RetainUntilDate = rdate
		// set retention timestamp in opts
		retTimestamp := objInfo.ModTime
		if retainTmstampStr, ok := objInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp]; ok {
			retTimestamp, err = time.Parse(time.RFC3339Nano, retainTmstampStr)
			if err != nil {
				return putOpts, false, err
			}
		}
		putOpts.Internal.RetentionTimestamp = retTimestamp
	}
	if lhold, ok := lkMap.Lookup(xhttp.AmzObjectLockLegalHold); ok {
		putOpts.LegalHold = minio.LegalHoldStatus(lhold)
		// set legalhold timestamp in opts
		lholdTimestamp := objInfo.ModTime
		if lholdTmstampStr, ok := objInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockLegalHoldTimestamp]; ok {
			lholdTimestamp, err = time.Parse(time.RFC3339Nano, lholdTmstampStr)
			if err != nil {
				return putOpts, false, err
			}
		}
		putOpts.Internal.LegalholdTimestamp = lholdTimestamp
	}
	if crypto.S3.IsEncrypted(objInfo.UserDefined) {
		putOpts.ServerSideEncryption = encrypt.NewSSE()
	}

	if crypto.S3KMS.IsEncrypted(objInfo.UserDefined) {
		// If KMS key ID replication is enabled (as by default)
		// we include the object's KMS key ID. In any case, we
		// always set the SSE-KMS header. If no KMS key ID is
		// specified, MinIO is supposed to use whatever default
		// config applies on the site or bucket.
		var keyID string
		if kms.ReplicateKeyID() {
			keyID = objInfo.KMSKeyID()
		}

		sseEnc, err := encrypt.NewSSEKMS(keyID, nil)
		if err != nil {
			return putOpts, false, err
		}
		putOpts.ServerSideEncryption = sseEnc
	}
	return putOpts, isMP, err
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
		if strings.EqualFold(k1, k2) {
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
	sz, _ := oi1.GetActualSize()

	// needs full replication
	if oi1.ETag != oi2.ETag ||
		oi1.VersionID != oi2.VersionID ||
		sz != oi2.Size ||
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
	oi2Map := make(map[string]string)
	maps.Copy(oi2Map, oi2.UserTags)
	if (oi2.UserTagCount > 0 && !reflect.DeepEqual(oi2Map, t.ToMap())) || (oi2.UserTagCount != len(t.ToMap())) {
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
			if !stringsHasPrefixFold(k, prefix) {
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
			if !stringsHasPrefixFold(k, prefix) {
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
	var replicationStatus replication.StatusType
	defer func() {
		if replicationStatus.Empty() {
			// replication status is empty means
			// replication was not attempted for some
			// reason, notify the state of the object
			// on disk.
			replicationStatus = ri.ReplicationStatus
		}
		auditLogInternal(ctx, AuditLogOptions{
			Event:     ri.EventType,
			APIName:   ReplicateObjectAPI,
			Bucket:    ri.Bucket,
			Object:    ri.Name,
			VersionID: ri.VersionID,
			Status:    replicationStatus.String(),
		})
	}()

	bucket := ri.Bucket
	object := ri.Name

	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || cfg == nil {
		replLogOnceIf(ctx, err, "get-replication-config-"+bucket)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     ri.ToObjectInfo(),
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return
	}
	tgtArns := cfg.FilterTargetArns(replication.ObjectOpts{
		Name:     object,
		SSEC:     ri.SSEC,
		UserTags: ri.UserTags,
	})
	// Lock the object name before starting replication.
	// Use separate lock that doesn't collide with regular objects.
	lk := objectAPI.NewNSLock(bucket, "/[replicate]/"+object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     ri.ToObjectInfo(),
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		globalReplicationPool.Get().queueMRFSave(ri.ToMRFEntry())
		return
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	rinfos := replicatedInfos{Targets: make([]replicatedTargetInfo, 0, len(tgtArns))}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, tgtArn := range tgtArns {
		tgt := globalBucketTargetSys.GetRemoteTargetClient(bucket, tgtArn)
		if tgt == nil {
			replLogOnceIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, tgtArn), tgtArn)
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     ri.ToObjectInfo(),
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			continue
		}
		wg.Add(1)
		go func(tgt *TargetClient) {
			defer wg.Done()

			var tgtInfo replicatedTargetInfo
			if ri.OpType == replication.ObjectReplicationType {
				// all incoming calls go through optimized path.
				tgtInfo = ri.replicateObject(ctx, objectAPI, tgt)
			} else {
				tgtInfo = ri.replicateAll(ctx, objectAPI, tgt)
			}

			mu.Lock()
			rinfos.Targets = append(rinfos.Targets, tgtInfo)
			mu.Unlock()
		}(tgt)
	}
	wg.Wait()

	replicationStatus = rinfos.ReplicationStatus() // used in defer function
	// FIXME: add support for missing replication events
	// - event.ObjectReplicationMissedThreshold
	// - event.ObjectReplicationReplicatedAfterThreshold
	eventName := event.ObjectReplicationComplete
	if replicationStatus == replication.Failed {
		eventName = event.ObjectReplicationFailed
	}
	newReplStatusInternal := rinfos.ReplicationStatusInternal()
	// Note that internal replication status(es) may match for previously replicated objects - in such cases
	// metadata should be updated with last resync timestamp.
	objInfo := ri.ToObjectInfo()
	if ri.ReplicationStatusInternal != newReplStatusInternal || rinfos.ReplicationResynced() {
		popts := ObjectOptions{
			MTime:     ri.ModTime,
			VersionID: ri.VersionID,
			EvalMetadataFn: func(oi *ObjectInfo, gerr error) (dsc ReplicateDecision, err error) {
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = newReplStatusInternal
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
				oi.UserDefined[xhttp.AmzBucketReplicationStatus] = string(rinfos.ReplicationStatus())
				for _, rinfo := range rinfos.Targets {
					if rinfo.ResyncTimestamp != "" {
						oi.UserDefined[targetResetHeader(rinfo.Arn)] = rinfo.ResyncTimestamp
					}
				}
				if ri.UserTags != "" {
					oi.UserDefined[xhttp.AmzObjectTagging] = ri.UserTags
				}
				return dsc, nil
			},
		}

		uobjInfo, _ := objectAPI.PutObjectMetadata(ctx, bucket, object, popts)
		if uobjInfo.Name != "" {
			objInfo = uobjInfo
		}

		opType := replication.MetadataReplicationType
		if rinfos.Action() == replicateAll {
			opType = replication.ObjectReplicationType
		}
		for _, rinfo := range rinfos.Targets {
			if rinfo.ReplicationStatus != rinfo.PrevReplicationStatus {
				rinfo.OpType = opType // update optype to reflect correct operation.
				globalReplicationStats.Load().Update(bucket, rinfo, rinfo.ReplicationStatus, rinfo.PrevReplicationStatus)
			}
		}
	}

	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: bucket,
		Object:     objInfo,
		UserAgent:  "Internal: [Replication]",
		Host:       globalLocalNodeName,
	})

	// re-queue failures once more - keep a retry count to avoid flooding the queue if
	// the target site is down. Leave it to scanner to catch up instead.
	if rinfos.ReplicationStatus() != replication.Completed {
		ri.OpType = replication.HealReplicationType
		ri.EventType = ReplicateMRF
		ri.ReplicationStatusInternal = rinfos.ReplicationStatusInternal()
		ri.RetryCount++
		globalReplicationPool.Get().queueMRFSave(ri.ToMRFEntry())
	}
}

// replicateObject replicates object data for specified version of the object to destination bucket
// The source object is then updated to reflect the replication status.
func (ri ReplicateObjectInfo) replicateObject(ctx context.Context, objectAPI ObjectLayer, tgt *TargetClient) (rinfo replicatedTargetInfo) {
	startTime := time.Now()
	bucket := ri.Bucket
	object := ri.Name

	rAction := replicateAll
	rinfo = replicatedTargetInfo{
		Size:                  ri.ActualSize,
		Arn:                   tgt.ARN,
		PrevReplicationStatus: ri.TargetReplicationStatus(tgt.ARN),
		ReplicationStatus:     replication.Failed,
		OpType:                ri.OpType,
		ReplicationAction:     rAction,
		endpoint:              tgt.EndpointURL().Host,
		secure:                tgt.EndpointURL().Scheme == "https",
	}
	if ri.TargetReplicationStatus(tgt.ARN) == replication.Completed && !ri.ExistingObjResync.Empty() && !ri.ExistingObjResync.mustResyncTarget(tgt.ARN) {
		rinfo.ReplicationStatus = replication.Completed
		rinfo.ReplicationResynced = true
		return rinfo
	}

	if globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
		replLogOnceIf(ctx, fmt.Errorf("remote target is offline for bucket:%s arn:%s retry:%d", bucket, tgt.ARN, ri.RetryCount), "replication-target-offline"+tgt.ARN)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     ri.ToObjectInfo(),
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}

	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)

	gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{}, ObjectOptions{
		VersionID:          ri.VersionID,
		Versioned:          versioned,
		VersionSuspended:   versionSuspended,
		ReplicationRequest: true,
	})
	if err != nil {
		if !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			objInfo := ri.ToObjectInfo()
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			replLogOnceIf(ctx, fmt.Errorf("unable to read source object %s/%s(%s): %w", bucket, object, objInfo.VersionID, err), object+":"+objInfo.VersionID)
		}
		return rinfo
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	// make sure we have the latest metadata for metrics calculation
	rinfo.PrevReplicationStatus = objInfo.TargetReplicationStatus(tgt.ARN)

	// Set the encrypted size for SSE-C objects
	var size int64
	if crypto.SSEC.IsEncrypted(objInfo.UserDefined) {
		size = objInfo.Size
	} else {
		size, err = objInfo.GetActualSize()
		if err != nil {
			replLogIf(ctx, err)
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			return rinfo
		}
	}

	if tgt.Bucket == "" {
		replLogIf(ctx, fmt.Errorf("unable to replicate object %s(%s), bucket is empty for target %s", objInfo.Name, objInfo.VersionID, tgt.EndpointURL()))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}
	defer func() {
		if rinfo.ReplicationStatus == replication.Completed && ri.OpType == replication.ExistingObjectReplicationType && tgt.ResetID != "" {
			rinfo.ResyncTimestamp = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), tgt.ResetID)
			rinfo.ReplicationResynced = true
		}
		rinfo.Duration = time.Since(startTime)
	}()

	rinfo.ReplicationStatus = replication.Completed
	rinfo.Size = size
	rinfo.ReplicationAction = rAction
	// use core client to avoid doing multipart on PUT
	c := &minio.Core{Client: tgt.Client}

	putOpts, isMP, err := putReplicationOpts(ctx, tgt.StorageClass, objInfo)
	if err != nil {
		replLogIf(ctx, fmt.Errorf("failure setting options for replication bucket:%s err:%w", bucket, err))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}

	var headerSize int
	for k, v := range putOpts.Header() {
		headerSize += len(k) + len(v)
	}

	opts := &bandwidth.MonitorReaderOptions{
		BucketOptions: bandwidth.BucketOptions{
			Name:           ri.Bucket,
			ReplicationARN: tgt.ARN,
		},
		HeaderSize: headerSize,
	}
	newCtx := ctx
	if globalBucketMonitor.IsThrottled(bucket, tgt.ARN) && objInfo.Size < minLargeObjSize {
		var cancel context.CancelFunc
		newCtx, cancel = context.WithTimeout(ctx, throttleDeadline)
		defer cancel()
	}
	r := bandwidth.NewMonitoredReader(newCtx, globalBucketMonitor, gr, opts)
	if isMP {
		rinfo.Err = replicateObjectWithMultipart(ctx, c, tgt.Bucket, object, r, objInfo, putOpts)
	} else {
		_, rinfo.Err = c.PutObject(ctx, tgt.Bucket, object, r, size, "", "", putOpts)
	}
	if rinfo.Err != nil {
		if minio.ToErrorResponse(rinfo.Err).Code != "PreconditionFailed" {
			rinfo.ReplicationStatus = replication.Failed
			replLogIf(ctx, fmt.Errorf("unable to replicate for object %s/%s(%s): to (target: %s): %w",
				bucket, objInfo.Name, objInfo.VersionID, tgt.EndpointURL(), rinfo.Err))
		}
		if minio.IsNetworkOrHostDown(rinfo.Err, true) && !globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			globalBucketTargetSys.markOffline(tgt.EndpointURL())
		}
	}
	return rinfo
}

// replicateAll replicates metadata for specified version of the object to destination bucket
// if the destination version is missing it automatically does fully copy as well.
// The source object is then updated to reflect the replication status.
func (ri ReplicateObjectInfo) replicateAll(ctx context.Context, objectAPI ObjectLayer, tgt *TargetClient) (rinfo replicatedTargetInfo) {
	startTime := time.Now()
	bucket := ri.Bucket
	object := ri.Name

	// set defaults for replication action based on operation being performed - actual
	// replication action can only be determined after stat on remote. This default is
	// needed for updating replication metrics correctly when target is offline.
	rAction := replicateMetadata

	rinfo = replicatedTargetInfo{
		Size:                  ri.ActualSize,
		Arn:                   tgt.ARN,
		PrevReplicationStatus: ri.TargetReplicationStatus(tgt.ARN),
		ReplicationStatus:     replication.Failed,
		OpType:                ri.OpType,
		ReplicationAction:     rAction,
		endpoint:              tgt.EndpointURL().Host,
		secure:                tgt.EndpointURL().Scheme == "https",
	}

	if globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
		replLogOnceIf(ctx, fmt.Errorf("remote target is offline for bucket:%s arn:%s retry:%d", bucket, tgt.ARN, ri.RetryCount), "replication-target-offline-heal"+tgt.ARN)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     ri.ToObjectInfo(),
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}

	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)

	gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{},
		ObjectOptions{
			VersionID:          ri.VersionID,
			Versioned:          versioned,
			VersionSuspended:   versionSuspended,
			ReplicationRequest: true,
		})
	if err != nil {
		if !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			objInfo := ri.ToObjectInfo()
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			replLogIf(ctx, fmt.Errorf("unable to replicate to target %s for %s/%s(%s): %w", tgt.EndpointURL(), bucket, object, objInfo.VersionID, err))
		}
		return rinfo
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	// make sure we have the latest metadata for metrics calculation
	rinfo.PrevReplicationStatus = objInfo.TargetReplicationStatus(tgt.ARN)

	// use latest ObjectInfo to check if previous replication attempt succeeded
	if objInfo.TargetReplicationStatus(tgt.ARN) == replication.Completed && !ri.ExistingObjResync.Empty() && !ri.ExistingObjResync.mustResyncTarget(tgt.ARN) {
		rinfo.ReplicationStatus = replication.Completed
		rinfo.ReplicationResynced = true
		return rinfo
	}

	size, err := objInfo.GetActualSize()
	if err != nil {
		replLogIf(ctx, err)
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}

	// Set the encrypted size for SSE-C objects
	isSSEC := crypto.SSEC.IsEncrypted(objInfo.UserDefined)
	if isSSEC {
		size = objInfo.Size
	}

	if tgt.Bucket == "" {
		replLogIf(ctx, fmt.Errorf("unable to replicate object %s(%s) to %s, target bucket is missing", objInfo.Name, objInfo.VersionID, tgt.EndpointURL()))
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationNotTracked,
			BucketName: bucket,
			Object:     objInfo,
			UserAgent:  "Internal: [Replication]",
			Host:       globalLocalNodeName,
		})
		return rinfo
	}
	defer func() {
		if rinfo.ReplicationStatus == replication.Completed && ri.OpType == replication.ExistingObjectReplicationType && tgt.ResetID != "" {
			rinfo.ResyncTimestamp = fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), tgt.ResetID)
			rinfo.ReplicationResynced = true
		}
		rinfo.Duration = time.Since(startTime)
	}()
	sOpts := minio.StatObjectOptions{
		VersionID: objInfo.VersionID,
		Internal: minio.AdvancedGetOptions{
			ReplicationProxyRequest: "false",
		},
	}
	sOpts.Set(xhttp.AmzTagDirective, "ACCESS")
	oi, cerr := tgt.StatObject(ctx, tgt.Bucket, object, sOpts)
	if cerr == nil {
		rAction = getReplicationAction(objInfo, oi, ri.OpType)
		rinfo.ReplicationStatus = replication.Completed
		if rAction == replicateNone {
			if ri.OpType == replication.ExistingObjectReplicationType &&
				objInfo.ModTime.Unix() > oi.LastModified.Unix() && objInfo.VersionID == nullVersionID {
				replLogIf(ctx, fmt.Errorf("unable to replicate %s/%s (null). Newer version exists on target %s", bucket, object, tgt.EndpointURL()))
				sendEvent(eventArgs{
					EventName:  event.ObjectReplicationNotTracked,
					BucketName: bucket,
					Object:     objInfo,
					UserAgent:  "Internal: [Replication]",
					Host:       globalLocalNodeName,
				})
			}
			// object with same VersionID already exists, replication kicked off by
			// PutObject might have completed
			if objInfo.TargetReplicationStatus(tgt.ARN) == replication.Pending ||
				objInfo.TargetReplicationStatus(tgt.ARN) == replication.Failed ||
				ri.OpType == replication.ExistingObjectReplicationType {
				// if metadata is not updated for some reason after replication, such as
				// 503 encountered while updating metadata - make sure to set ReplicationStatus
				// as Completed.
				//
				// Note: Replication Stats would have been updated despite metadata update failure.
				rinfo.ReplicationAction = rAction
				rinfo.ReplicationStatus = replication.Completed
			}
			return rinfo
		}
	} else {
		// SSEC objects will refuse HeadObject without the decryption key.
		// Ignore the error, since we know the object exists and versioning prevents overwriting existing versions.
		if isSSEC && strings.Contains(cerr.Error(), errorCodes[ErrSSEEncryptedObject].Description) {
			rinfo.ReplicationStatus = replication.Completed
			rinfo.ReplicationAction = replicateNone
			goto applyAction
		}
		// if target returns error other than NoSuchKey, defer replication attempt
		if minio.IsNetworkOrHostDown(cerr, true) && !globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			globalBucketTargetSys.markOffline(tgt.EndpointURL())
		}

		serr := ErrorRespToObjectError(cerr, bucket, object, objInfo.VersionID)
		switch {
		case isErrMethodNotAllowed(serr):
			rAction = replicateAll
		case isErrObjectNotFound(serr), isErrVersionNotFound(serr):
			rAction = replicateAll
		case isErrReadQuorum(serr), isErrWriteQuorum(serr):
			rAction = replicateAll
		default:
			rinfo.Err = cerr
			replLogIf(ctx, fmt.Errorf("unable to replicate %s/%s (%s). Target (%s) returned %s error on HEAD",
				bucket, object, objInfo.VersionID, tgt.EndpointURL(), cerr))
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			return rinfo
		}
	}
applyAction:
	rinfo.ReplicationStatus = replication.Completed
	rinfo.Size = size
	rinfo.ReplicationAction = rAction
	// use core client to avoid doing multipart on PUT
	c := &minio.Core{Client: tgt.Client}
	if rAction != replicateAll {
		// replicate metadata for object tagging/copy with metadata replacement
		srcOpts := minio.CopySrcOptions{
			Bucket:    tgt.Bucket,
			Object:    object,
			VersionID: objInfo.VersionID,
		}
		dstOpts := minio.PutObjectOptions{
			Internal: minio.AdvancedPutOptions{
				SourceVersionID:    objInfo.VersionID,
				ReplicationRequest: true, // always set this to distinguish between `mc mirror` replication and serverside
			},
		}
		// default timestamps to ModTime unless present in metadata
		lkMap := caseInsensitiveMap(objInfo.UserDefined)
		if _, ok := lkMap.Lookup(xhttp.AmzObjectLockLegalHold); ok {
			dstOpts.Internal.LegalholdTimestamp = objInfo.ModTime
		}
		if _, ok := lkMap.Lookup(xhttp.AmzObjectLockRetainUntilDate); ok {
			dstOpts.Internal.RetentionTimestamp = objInfo.ModTime
		}
		if objInfo.UserTags != "" {
			dstOpts.Internal.TaggingTimestamp = objInfo.ModTime
		}
		if tagTmStr, ok := lkMap.Lookup(ReservedMetadataPrefixLower + TaggingTimestamp); ok {
			ondiskTimestamp, err := time.Parse(time.RFC3339, tagTmStr)
			if err == nil {
				dstOpts.Internal.TaggingTimestamp = ondiskTimestamp
			}
		}
		if retTmStr, ok := lkMap.Lookup(ReservedMetadataPrefixLower + ObjectLockRetentionTimestamp); ok {
			ondiskTimestamp, err := time.Parse(time.RFC3339, retTmStr)
			if err == nil {
				dstOpts.Internal.RetentionTimestamp = ondiskTimestamp
			}
		}
		if lholdTmStr, ok := lkMap.Lookup(ReservedMetadataPrefixLower + ObjectLockLegalHoldTimestamp); ok {
			ondiskTimestamp, err := time.Parse(time.RFC3339, lholdTmStr)
			if err == nil {
				dstOpts.Internal.LegalholdTimestamp = ondiskTimestamp
			}
		}
		if _, rinfo.Err = c.CopyObject(ctx, tgt.Bucket, object, tgt.Bucket, object, getCopyObjMetadata(objInfo, tgt.StorageClass), srcOpts, dstOpts); rinfo.Err != nil {
			rinfo.ReplicationStatus = replication.Failed
			replLogIf(ctx, fmt.Errorf("unable to replicate metadata for object %s/%s(%s) to target %s: %w", bucket, objInfo.Name, objInfo.VersionID, tgt.EndpointURL(), rinfo.Err))
		}
	} else {
		putOpts, isMP, err := putReplicationOpts(ctx, tgt.StorageClass, objInfo)
		if err != nil {
			replLogIf(ctx, fmt.Errorf("failed to set replicate options for object %s/%s(%s) (target %s) err:%w", bucket, objInfo.Name, objInfo.VersionID, tgt.EndpointURL(), err))
			sendEvent(eventArgs{
				EventName:  event.ObjectReplicationNotTracked,
				BucketName: bucket,
				Object:     objInfo,
				UserAgent:  "Internal: [Replication]",
				Host:       globalLocalNodeName,
			})
			return rinfo
		}
		var headerSize int
		for k, v := range putOpts.Header() {
			headerSize += len(k) + len(v)
		}

		opts := &bandwidth.MonitorReaderOptions{
			BucketOptions: bandwidth.BucketOptions{
				Name:           objInfo.Bucket,
				ReplicationARN: tgt.ARN,
			},
			HeaderSize: headerSize,
		}
		newCtx := ctx
		if globalBucketMonitor.IsThrottled(bucket, tgt.ARN) && objInfo.Size < minLargeObjSize {
			var cancel context.CancelFunc
			newCtx, cancel = context.WithTimeout(ctx, throttleDeadline)
			defer cancel()
		}
		r := bandwidth.NewMonitoredReader(newCtx, globalBucketMonitor, gr, opts)
		if isMP {
			rinfo.Err = replicateObjectWithMultipart(ctx, c, tgt.Bucket, object, r, objInfo, putOpts)
		} else {
			_, rinfo.Err = c.PutObject(ctx, tgt.Bucket, object, r, size, "", "", putOpts)
		}
		if rinfo.Err != nil {
			if minio.ToErrorResponse(rinfo.Err).Code != "PreconditionFailed" {
				rinfo.ReplicationStatus = replication.Failed
				replLogIf(ctx, fmt.Errorf("unable to replicate for object %s/%s(%s) to target %s: %w",
					bucket, objInfo.Name, objInfo.VersionID, tgt.EndpointURL(), rinfo.Err))
			}
			if minio.IsNetworkOrHostDown(rinfo.Err, true) && !globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
				globalBucketTargetSys.markOffline(tgt.EndpointURL())
			}
		}
	}
	return rinfo
}

func replicateObjectWithMultipart(ctx context.Context, c *minio.Core, bucket, object string, r io.Reader, objInfo ObjectInfo, opts minio.PutObjectOptions) (err error) {
	var uploadedParts []minio.CompletePart
	// new multipart must not set mtime as it may lead to erroneous cleanups at various intervals.
	opts.Internal.SourceMTime = time.Time{} // this value is saved properly in CompleteMultipartUpload()
	var uploadID string
	attempts := 1
	for attempts <= 3 {
		nctx, cancel := context.WithTimeout(ctx, time.Minute)
		uploadID, err = c.NewMultipartUpload(nctx, bucket, object, opts)
		cancel()
		if err == nil {
			break
		}
		if minio.ToErrorResponse(err).Code == "PreconditionFailed" {
			return nil
		}
		attempts++
		time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
	}
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			// block and abort remote upload upon failure.
			attempts := 1
			for attempts <= 3 {
				actx, acancel := context.WithTimeout(ctx, time.Minute)
				aerr := c.AbortMultipartUpload(actx, bucket, object, uploadID)
				acancel()
				if aerr == nil {
					return
				}
				attempts++
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
			}
		}
	}()

	var (
		hr     *hash.Reader
		isSSEC = crypto.SSEC.IsEncrypted(objInfo.UserDefined)
	)

	var objectSize int64
	for _, partInfo := range objInfo.Parts {
		if isSSEC {
			hr, err = hash.NewReader(ctx, io.LimitReader(r, partInfo.Size), partInfo.Size, "", "", partInfo.ActualSize)
		} else {
			hr, err = hash.NewReader(ctx, io.LimitReader(r, partInfo.ActualSize), partInfo.ActualSize, "", "", partInfo.ActualSize)
		}
		if err != nil {
			return err
		}

		cHeader := http.Header{}
		cHeader.Add(xhttp.MinIOSourceReplicationRequest, "true")
		if !isSSEC {
			cs, _ := getCRCMeta(objInfo, partInfo.Number, nil)
			for k, v := range cs {
				cHeader.Add(k, v)
			}
		}
		popts := minio.PutObjectPartOptions{
			SSE:          opts.ServerSideEncryption,
			CustomHeader: cHeader,
		}

		var size int64
		if isSSEC {
			size = partInfo.Size
		} else {
			size = partInfo.ActualSize
		}
		objectSize += size
		pInfo, err := c.PutObjectPart(ctx, bucket, object, uploadID, partInfo.Number, hr, size, popts)
		if err != nil {
			return err
		}
		if pInfo.Size != size {
			return fmt.Errorf("ssec(%t): Part size mismatch: got %d, want %d", isSSEC, pInfo.Size, size)
		}
		uploadedParts = append(uploadedParts, minio.CompletePart{
			PartNumber:        pInfo.PartNumber,
			ETag:              pInfo.ETag,
			ChecksumCRC32:     pInfo.ChecksumCRC32,
			ChecksumCRC32C:    pInfo.ChecksumCRC32C,
			ChecksumSHA1:      pInfo.ChecksumSHA1,
			ChecksumSHA256:    pInfo.ChecksumSHA256,
			ChecksumCRC64NVME: pInfo.ChecksumCRC64NVME,
		})
	}
	userMeta := map[string]string{
		xhttp.MinIOReplicationActualObjectSize: objInfo.UserDefined[ReservedMetadataPrefix+"actual-size"],
	}
	if isSSEC && objInfo.UserDefined[ReplicationSsecChecksumHeader] != "" {
		userMeta[ReplicationSsecChecksumHeader] = objInfo.UserDefined[ReplicationSsecChecksumHeader]
	}

	// really big value but its okay on heavily loaded systems. This is just tail end timeout.
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Minute)
	defer ccancel()

	if len(objInfo.Checksum) > 0 {
		cs, _ := getCRCMeta(objInfo, 0, nil)
		for k, v := range cs {
			userMeta[k] = strings.Split(v, "-")[0]
		}
	}
	_, err = c.CompleteMultipartUpload(cctx, bucket, object, uploadID, uploadedParts, minio.PutObjectOptions{
		UserMetadata: userMeta,
		Internal: minio.AdvancedPutOptions{
			SourceMTime: objInfo.ModTime,
			SourceETag:  objInfo.ETag,
			// always set this to distinguish between `mc mirror` replication and serverside
			ReplicationRequest: true,
		},
	})
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
			maps.Copy(dst, metadata)
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
	EventType string
	OpType    replication.Type
	ResetID   string
	TargetArn string
}

// ToMRFEntry returns the relevant info needed by MRF
func (di DeletedObjectReplicationInfo) ToMRFEntry() MRFReplicateEntry {
	versionID := di.DeleteMarkerVersionID
	if versionID == "" {
		versionID = di.VersionID
	}
	return MRFReplicateEntry{
		Bucket:    di.Bucket,
		Object:    di.ObjectName,
		versionID: versionID,
	}
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
	// ReplicateIncoming - audit trail of inline replication
	ReplicateIncoming = "replicate:incoming"
	// ReplicateIncomingDelete - audit trail of inline replication of deletes.
	ReplicateIncomingDelete = "replicate:incoming:delete"

	// ReplicateHeal - audit trail for healing of failed/pending replications
	ReplicateHeal = "replicate:heal"
	// ReplicateHealDelete - audit trail of healing of failed/pending delete replications.
	ReplicateHealDelete = "replicate:heal:delete"
)

var (
	globalReplicationPool  = once.NewSingleton[ReplicationPool]()
	globalReplicationStats atomic.Pointer[ReplicationStats]
)

// ReplicationPool describes replication pool
type ReplicationPool struct {
	// atomic ops:
	activeWorkers    int32
	activeLrgWorkers int32
	activeMRFWorkers int32

	objLayer    ObjectLayer
	ctx         context.Context
	priority    string
	maxWorkers  int
	maxLWorkers int
	stats       *ReplicationStats

	mu       sync.RWMutex
	mrfMU    sync.Mutex
	resyncer *replicationResyncer

	// workers:
	workers    []chan ReplicationWorkerOperation
	lrgworkers []chan ReplicationWorkerOperation

	// mrf:
	mrfWorkerKillCh chan struct{}
	mrfReplicaCh    chan ReplicationWorkerOperation
	mrfSaveCh       chan MRFReplicateEntry
	mrfStopCh       chan struct{}
	mrfWorkerSize   int
}

// ReplicationWorkerOperation is a shared interface of replication operations.
type ReplicationWorkerOperation interface {
	ToMRFEntry() MRFReplicateEntry
}

const (
	// WorkerMaxLimit max number of workers per node for "fast" mode
	WorkerMaxLimit = 500

	// WorkerMinLimit min number of workers per node for "slow" mode
	WorkerMinLimit = 50

	// WorkerAutoDefault is default number of workers for "auto" mode
	WorkerAutoDefault = 100

	// MRFWorkerMaxLimit max number of mrf workers per node for "fast" mode
	MRFWorkerMaxLimit = 8

	// MRFWorkerMinLimit min number of mrf workers per node for "slow" mode
	MRFWorkerMinLimit = 2

	// MRFWorkerAutoDefault is default number of mrf workers for "auto" mode
	MRFWorkerAutoDefault = 4

	// LargeWorkerCount is default number of workers assigned to large uploads ( >= 128MiB)
	LargeWorkerCount = 10
)

// NewReplicationPool creates a pool of replication workers of specified size
func NewReplicationPool(ctx context.Context, o ObjectLayer, opts replicationPoolOpts, stats *ReplicationStats) *ReplicationPool {
	var workers, failedWorkers int
	priority := "auto"
	maxWorkers := WorkerMaxLimit
	if opts.Priority != "" {
		priority = opts.Priority
	}
	if opts.MaxWorkers > 0 {
		maxWorkers = opts.MaxWorkers
	}
	switch priority {
	case "fast":
		workers = WorkerMaxLimit
		failedWorkers = MRFWorkerMaxLimit
	case "slow":
		workers = WorkerMinLimit
		failedWorkers = MRFWorkerMinLimit
	default:
		workers = WorkerAutoDefault
		failedWorkers = MRFWorkerAutoDefault
	}
	if maxWorkers > 0 && workers > maxWorkers {
		workers = maxWorkers
	}

	if maxWorkers > 0 && failedWorkers > maxWorkers {
		failedWorkers = maxWorkers
	}
	maxLWorkers := LargeWorkerCount
	if opts.MaxLWorkers > 0 {
		maxLWorkers = opts.MaxLWorkers
	}
	pool := &ReplicationPool{
		workers:         make([]chan ReplicationWorkerOperation, 0, workers),
		lrgworkers:      make([]chan ReplicationWorkerOperation, 0, maxLWorkers),
		mrfReplicaCh:    make(chan ReplicationWorkerOperation, 100000),
		mrfWorkerKillCh: make(chan struct{}, failedWorkers),
		resyncer:        newresyncer(),
		mrfSaveCh:       make(chan MRFReplicateEntry, 100000),
		mrfStopCh:       make(chan struct{}, 1),
		ctx:             ctx,
		objLayer:        o,
		stats:           stats,
		priority:        priority,
		maxWorkers:      maxWorkers,
		maxLWorkers:     maxLWorkers,
	}

	pool.ResizeLrgWorkers(maxLWorkers, 0)
	pool.ResizeWorkers(workers, 0)
	pool.ResizeFailedWorkers(failedWorkers)
	go pool.resyncer.PersistToDisk(ctx, o)
	go pool.processMRF()
	go pool.persistMRF()
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
			switch v := oi.(type) {
			case ReplicateObjectInfo:
				p.stats.incQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)
				atomic.AddInt32(&p.activeMRFWorkers, 1)
				replicateObject(p.ctx, v, p.objLayer)
				atomic.AddInt32(&p.activeMRFWorkers, -1)
				p.stats.decQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)

			default:
				bugLogIf(p.ctx, fmt.Errorf("unknown mrf replication type: %T", oi), "unknown-mrf-replicate-type")
			}
		case <-p.mrfWorkerKillCh:
			return
		}
	}
}

// AddWorker adds a replication worker to the pool.
// An optional pointer to a tracker that will be atomically
// incremented when operations are running can be provided.
func (p *ReplicationPool) AddWorker(input <-chan ReplicationWorkerOperation, opTracker *int32) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case oi, ok := <-input:
			if !ok {
				return
			}
			switch v := oi.(type) {
			case ReplicateObjectInfo:
				if opTracker != nil {
					atomic.AddInt32(opTracker, 1)
				}
				p.stats.incQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)
				replicateObject(p.ctx, v, p.objLayer)
				p.stats.decQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)
				if opTracker != nil {
					atomic.AddInt32(opTracker, -1)
				}
			case DeletedObjectReplicationInfo:
				if opTracker != nil {
					atomic.AddInt32(opTracker, 1)
				}
				p.stats.incQ(v.Bucket, 0, true, v.OpType)

				replicateDelete(p.ctx, v, p.objLayer)
				p.stats.decQ(v.Bucket, 0, true, v.OpType)

				if opTracker != nil {
					atomic.AddInt32(opTracker, -1)
				}
			default:
				bugLogIf(p.ctx, fmt.Errorf("unknown replication type: %T", oi), "unknown-replicate-type")
			}
		}
	}
}

// AddLargeWorker adds a replication worker to the static pool for large uploads.
func (p *ReplicationPool) AddLargeWorker(input <-chan ReplicationWorkerOperation, opTracker *int32) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case oi, ok := <-input:
			if !ok {
				return
			}
			switch v := oi.(type) {
			case ReplicateObjectInfo:
				if opTracker != nil {
					atomic.AddInt32(opTracker, 1)
				}
				p.stats.incQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)
				replicateObject(p.ctx, v, p.objLayer)
				p.stats.decQ(v.Bucket, v.Size, v.DeleteMarker, v.OpType)
				if opTracker != nil {
					atomic.AddInt32(opTracker, -1)
				}
			case DeletedObjectReplicationInfo:
				if opTracker != nil {
					atomic.AddInt32(opTracker, 1)
				}
				replicateDelete(p.ctx, v, p.objLayer)
				if opTracker != nil {
					atomic.AddInt32(opTracker, -1)
				}
			default:
				bugLogIf(p.ctx, fmt.Errorf("unknown replication type: %T", oi), "unknown-replicate-type")
			}
		}
	}
}

// ResizeLrgWorkers sets replication workers pool for large transfers(>=128MiB) to new size.
// checkOld can be set to an expected value.
// If the worker count changed
func (p *ReplicationPool) ResizeLrgWorkers(n, checkOld int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if (checkOld > 0 && len(p.lrgworkers) != checkOld) || n == len(p.lrgworkers) || n < 1 {
		// Either already satisfied or worker count changed while we waited for the lock.
		return
	}
	for len(p.lrgworkers) < n {
		input := make(chan ReplicationWorkerOperation, 100000)
		p.lrgworkers = append(p.lrgworkers, input)

		go p.AddLargeWorker(input, &p.activeLrgWorkers)
	}
	for len(p.lrgworkers) > n {
		worker := p.lrgworkers[len(p.lrgworkers)-1]
		p.lrgworkers = p.lrgworkers[:len(p.lrgworkers)-1]
		xioutil.SafeClose(worker)
	}
}

// ActiveWorkers returns the number of active workers handling replication traffic.
func (p *ReplicationPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&p.activeWorkers))
}

// ActiveMRFWorkers returns the number of active workers handling replication failures.
func (p *ReplicationPool) ActiveMRFWorkers() int {
	return int(atomic.LoadInt32(&p.activeMRFWorkers))
}

// ActiveLrgWorkers returns the number of active workers handling traffic > 128MiB object size.
func (p *ReplicationPool) ActiveLrgWorkers() int {
	return int(atomic.LoadInt32(&p.activeLrgWorkers))
}

// ResizeWorkers sets replication workers pool to new size.
// checkOld can be set to an expected value.
// If the worker count changed
func (p *ReplicationPool) ResizeWorkers(n, checkOld int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if (checkOld > 0 && len(p.workers) != checkOld) || n == len(p.workers) || n < 1 {
		// Either already satisfied or worker count changed while we waited for the lock.
		return
	}
	for len(p.workers) < n {
		input := make(chan ReplicationWorkerOperation, 10000)
		p.workers = append(p.workers, input)

		go p.AddWorker(input, &p.activeWorkers)
	}
	for len(p.workers) > n {
		worker := p.workers[len(p.workers)-1]
		p.workers = p.workers[:len(p.workers)-1]
		xioutil.SafeClose(worker)
	}
}

// ResizeWorkerPriority sets replication failed workers pool size
func (p *ReplicationPool) ResizeWorkerPriority(pri string, maxWorkers, maxLWorkers int) {
	var workers, mrfWorkers int
	p.mu.Lock()
	switch pri {
	case "fast":
		workers = WorkerMaxLimit
		mrfWorkers = MRFWorkerMaxLimit
	case "slow":
		workers = WorkerMinLimit
		mrfWorkers = MRFWorkerMinLimit
	default:
		workers = WorkerAutoDefault
		mrfWorkers = MRFWorkerAutoDefault
		if len(p.workers) < WorkerAutoDefault {
			workers = min(len(p.workers)+1, WorkerAutoDefault)
		}
		if p.mrfWorkerSize < MRFWorkerAutoDefault {
			mrfWorkers = min(p.mrfWorkerSize+1, MRFWorkerAutoDefault)
		}
	}
	if maxWorkers > 0 && workers > maxWorkers {
		workers = maxWorkers
	}

	if maxWorkers > 0 && mrfWorkers > maxWorkers {
		mrfWorkers = maxWorkers
	}
	if maxLWorkers <= 0 {
		maxLWorkers = LargeWorkerCount
	}
	p.priority = pri
	p.maxWorkers = maxWorkers
	p.mu.Unlock()
	p.ResizeWorkers(workers, 0)
	p.ResizeFailedWorkers(mrfWorkers)
	p.ResizeLrgWorkers(maxLWorkers, 0)
}

// ResizeFailedWorkers sets replication failed workers pool size
func (p *ReplicationPool) ResizeFailedWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.mrfWorkerSize < n {
		p.mrfWorkerSize++
		go p.AddMRFWorker()
	}
	for p.mrfWorkerSize > n {
		p.mrfWorkerSize--
		go func() { p.mrfWorkerKillCh <- struct{}{} }()
	}
}

const (
	minLargeObjSize = 128 * humanize.MiByte // 128MiB
)

// getWorkerCh gets a worker channel deterministically based on bucket and object names.
// Must be able to grab read lock from p.

func (p *ReplicationPool) getWorkerCh(bucket, object string, sz int64) chan<- ReplicationWorkerOperation {
	h := xxh3.HashString(bucket + object)
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.workers) == 0 {
		return nil
	}
	return p.workers[h%uint64(len(p.workers))]
}

func (p *ReplicationPool) queueReplicaTask(ri ReplicateObjectInfo) {
	if p == nil {
		return
	}
	// if object is large, queue it to a static set of large workers
	if ri.Size >= int64(minLargeObjSize) {
		h := xxh3.HashString(ri.Bucket + ri.Name)
		select {
		case <-p.ctx.Done():
		case p.lrgworkers[h%uint64(len(p.lrgworkers))] <- ri:
		default:
			p.queueMRFSave(ri.ToMRFEntry())
			p.mu.RLock()
			maxLWorkers := p.maxLWorkers
			existing := len(p.lrgworkers)
			p.mu.RUnlock()
			maxLWorkers = min(maxLWorkers, LargeWorkerCount)
			if p.ActiveLrgWorkers() < maxLWorkers {
				workers := min(existing+1, maxLWorkers)
				p.ResizeLrgWorkers(workers, existing)
			}
		}
		return
	}

	var ch, healCh chan<- ReplicationWorkerOperation
	switch ri.OpType {
	case replication.HealReplicationType, replication.ExistingObjectReplicationType:
		ch = p.mrfReplicaCh
		healCh = p.getWorkerCh(ri.Name, ri.Bucket, ri.Size)
	default:
		ch = p.getWorkerCh(ri.Name, ri.Bucket, ri.Size)
	}
	if ch == nil && healCh == nil {
		return
	}

	select {
	case <-p.ctx.Done():
	case healCh <- ri:
	case ch <- ri:
	default:
		globalReplicationPool.Get().queueMRFSave(ri.ToMRFEntry())
		p.mu.RLock()
		prio := p.priority
		maxWorkers := p.maxWorkers
		p.mu.RUnlock()
		switch prio {
		case "fast":
			replLogOnceIf(GlobalContext, fmt.Errorf("Unable to keep up with incoming traffic"), string(replicationSubsystem), logger.WarningKind)
		case "slow":
			replLogOnceIf(GlobalContext, fmt.Errorf("Unable to keep up with incoming traffic - we recommend increasing replication priority with `mc admin config set api replication_priority=auto`"), string(replicationSubsystem), logger.WarningKind)
		default:
			maxWorkers = min(maxWorkers, WorkerMaxLimit)
			if p.ActiveWorkers() < maxWorkers {
				p.mu.RLock()
				workers := min(len(p.workers)+1, maxWorkers)
				existing := len(p.workers)
				p.mu.RUnlock()
				p.ResizeWorkers(workers, existing)
			}
			maxMRFWorkers := min(maxWorkers, MRFWorkerMaxLimit)
			if p.ActiveMRFWorkers() < maxMRFWorkers {
				p.mu.RLock()
				workers := min(p.mrfWorkerSize+1, maxMRFWorkers)
				p.mu.RUnlock()
				p.ResizeFailedWorkers(workers)
			}
		}
	}
}

func queueReplicateDeletesWrapper(doi DeletedObjectReplicationInfo, existingObjectResync ResyncDecision) {
	for k, v := range existingObjectResync.targets {
		if v.Replicate {
			doi.ResetID = v.ResetID
			doi.TargetArn = k

			globalReplicationPool.Get().queueReplicaDeleteTask(doi)
		}
	}
}

func (p *ReplicationPool) queueReplicaDeleteTask(doi DeletedObjectReplicationInfo) {
	if p == nil {
		return
	}
	var ch chan<- ReplicationWorkerOperation
	switch doi.OpType {
	case replication.HealReplicationType, replication.ExistingObjectReplicationType:
		fallthrough
	default:
		ch = p.getWorkerCh(doi.Bucket, doi.ObjectName, 0)
	}

	select {
	case <-p.ctx.Done():
	case ch <- doi:
	default:
		p.queueMRFSave(doi.ToMRFEntry())
		p.mu.RLock()
		prio := p.priority
		maxWorkers := p.maxWorkers
		p.mu.RUnlock()
		switch prio {
		case "fast":
			replLogOnceIf(GlobalContext, fmt.Errorf("Unable to keep up with incoming deletes"), string(replicationSubsystem), logger.WarningKind)
		case "slow":
			replLogOnceIf(GlobalContext, fmt.Errorf("Unable to keep up with incoming deletes - we recommend increasing replication priority with `mc admin config set api replication_priority=auto`"), string(replicationSubsystem), logger.WarningKind)
		default:
			maxWorkers = min(maxWorkers, WorkerMaxLimit)
			if p.ActiveWorkers() < maxWorkers {
				p.mu.RLock()
				workers := min(len(p.workers)+1, maxWorkers)
				existing := len(p.workers)
				p.mu.RUnlock()
				p.ResizeWorkers(workers, existing)
			}
		}
	}
}

type replicationPoolOpts struct {
	Priority    string
	MaxWorkers  int
	MaxLWorkers int
}

func initBackgroundReplication(ctx context.Context, objectAPI ObjectLayer) {
	stats := NewReplicationStats(ctx, objectAPI)
	globalReplicationPool.Set(NewReplicationPool(ctx, objectAPI, globalAPIConfig.getReplicationOpts(), stats))
	globalReplicationStats.Store(stats)
	go stats.trackEWMA()
}

type proxyResult struct {
	Proxy bool
	Err   error
}

// get Reader from replication target if active-active replication is in place and
// this node returns a 404
func proxyGetToReplicationTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (gr *GetObjectReader, proxy proxyResult, err error) {
	tgt, oi, proxy := proxyHeadToRepTarget(ctx, bucket, object, rs, opts, proxyTargets)
	if !proxy.Proxy {
		return nil, proxy, nil
	}
	fn, _, _, err := NewGetObjectReader(nil, oi, opts, h)
	if err != nil {
		return nil, proxy, err
	}
	gopts := minio.GetObjectOptions{
		VersionID:            opts.VersionID,
		ServerSideEncryption: opts.ServerSideEncryption,
		Internal: minio.AdvancedGetOptions{
			ReplicationProxyRequest: "true",
		},
		PartNumber: opts.PartNumber,
	}
	// get correct offsets for encrypted object
	if rs != nil {
		h, err := rs.ToHeader()
		if err != nil {
			return nil, proxy, err
		}
		gopts.Set(xhttp.Range, h)
	}
	// Make sure to match ETag when proxying.
	if err = gopts.SetMatchETag(oi.ETag); err != nil {
		return nil, proxy, err
	}
	c := minio.Core{Client: tgt.Client}
	obj, _, h, err := c.GetObject(ctx, tgt.Bucket, object, gopts)
	if err != nil {
		return nil, proxy, err
	}
	closeReader := func() { obj.Close() }
	reader, err := fn(obj, h, closeReader)
	if err != nil {
		return nil, proxy, err
	}
	reader.ObjInfo = oi.Clone()
	if rs != nil {
		contentSize, err := parseSizeFromContentRange(h)
		if err != nil {
			return nil, proxy, err
		}
		reader.ObjInfo.Size = contentSize
	}

	return reader, proxyResult{Proxy: true}, nil
}

func getProxyTargets(ctx context.Context, bucket, object string, opts ObjectOptions) (tgts *madmin.BucketTargets) {
	if opts.VersionSuspended {
		return &madmin.BucketTargets{}
	}
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) {
		return &madmin.BucketTargets{}
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || cfg == nil {
		replLogOnceIf(ctx, err, bucket)

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

func proxyHeadToRepTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (tgt *TargetClient, oi ObjectInfo, proxy proxyResult) {
	// this option is set when active-active replication is in place between site A -> B,
	// and site B does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return nil, oi, proxy
	}
	var perr error
	for _, t := range proxyTargets.Targets {
		tgt = globalBucketTargetSys.GetRemoteTargetClient(bucket, t.Arn)
		if tgt == nil || globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			continue
		}
		// if proxying explicitly disabled on remote target
		if tgt.disableProxy {
			continue
		}

		gopts := minio.GetObjectOptions{
			VersionID:            opts.VersionID,
			ServerSideEncryption: opts.ServerSideEncryption,
			Internal: minio.AdvancedGetOptions{
				ReplicationProxyRequest: "true",
			},
			PartNumber: opts.PartNumber,
		}
		if rs != nil {
			h, err := rs.ToHeader()
			if err != nil {
				replLogIf(ctx, fmt.Errorf("invalid range header for %s/%s(%s) - %w", bucket, object, opts.VersionID, err))
				continue
			}
			gopts.Set(xhttp.Range, h)
		}

		objInfo, err := tgt.StatObject(ctx, t.TargetBucket, object, gopts)
		if err != nil {
			perr = err
			if isErrInvalidRange(ErrorRespToObjectError(err, bucket, object)) {
				return nil, oi, proxyResult{Err: err}
			}
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
			ReplicationStatus:         replication.StatusType(objInfo.ReplicationStatus),
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
		return tgt, oi, proxyResult{Proxy: true}
	}
	proxy.Err = perr
	return nil, oi, proxy
}

// get object info from replication target if active-active replication is in place and
// this node returns a 404
func proxyHeadToReplicationTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (oi ObjectInfo, proxy proxyResult) {
	_, oi, proxy = proxyHeadToRepTarget(ctx, bucket, object, rs, opts, proxyTargets)
	return oi, proxy
}

func scheduleReplication(ctx context.Context, oi ObjectInfo, o ObjectLayer, dsc ReplicateDecision, opType replication.Type) {
	tgtStatuses := replicationStatusesMap(oi.ReplicationStatusInternal)
	purgeStatuses := versionPurgeStatusesMap(oi.VersionPurgeStatusInternal)
	tm, _ := time.Parse(time.RFC3339Nano, oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp])
	rstate := oi.ReplicationState()
	rstate.ReplicateDecisionStr = dsc.String()
	asz, _ := oi.GetActualSize()

	ri := ReplicateObjectInfo{
		Name:                       oi.Name,
		Size:                       oi.Size,
		ActualSize:                 asz,
		Bucket:                     oi.Bucket,
		VersionID:                  oi.VersionID,
		ETag:                       oi.ETag,
		ModTime:                    oi.ModTime,
		ReplicationStatus:          oi.ReplicationStatus,
		ReplicationStatusInternal:  oi.ReplicationStatusInternal,
		DeleteMarker:               oi.DeleteMarker,
		VersionPurgeStatusInternal: oi.VersionPurgeStatusInternal,
		VersionPurgeStatus:         oi.VersionPurgeStatus,

		ReplicationState:     rstate,
		OpType:               opType,
		Dsc:                  dsc,
		TargetStatuses:       tgtStatuses,
		TargetPurgeStatuses:  purgeStatuses,
		ReplicationTimestamp: tm,
		SSEC:                 crypto.SSEC.IsEncrypted(oi.UserDefined),
		UserTags:             oi.UserTags,
	}
	if ri.SSEC {
		ri.Checksum = oi.Checksum
	}
	if dsc.Synchronous() {
		replicateObject(ctx, ri, o)
	} else {
		globalReplicationPool.Get().queueReplicaTask(ri)
	}
}

// proxyTaggingToRepTarget proxies tagging requests to remote targets for
// active-active replicated setups
func proxyTaggingToRepTarget(ctx context.Context, bucket, object string, tags *tags.Tags, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (proxy proxyResult) {
	// this option is set when active-active replication is in place between site A -> B,
	// and request hits site B that does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return proxy
	}
	var wg sync.WaitGroup
	errs := make([]error, len(proxyTargets.Targets))
	for idx, t := range proxyTargets.Targets {
		tgt := globalBucketTargetSys.GetRemoteTargetClient(bucket, t.Arn)
		if tgt == nil || globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			continue
		}
		// if proxying explicitly disabled on remote target
		if tgt.disableProxy {
			continue
		}
		idx := idx
		wg.Add(1)
		go func(idx int, tgt *TargetClient) {
			defer wg.Done()
			var err error
			if tags != nil {
				popts := minio.PutObjectTaggingOptions{
					VersionID: opts.VersionID,
					Internal: minio.AdvancedObjectTaggingOptions{
						ReplicationProxyRequest: "true",
					},
				}
				err = tgt.PutObjectTagging(ctx, tgt.Bucket, object, tags, popts)
			} else {
				dopts := minio.RemoveObjectTaggingOptions{
					VersionID: opts.VersionID,
					Internal: minio.AdvancedObjectTaggingOptions{
						ReplicationProxyRequest: "true",
					},
				}
				err = tgt.RemoveObjectTagging(ctx, tgt.Bucket, object, dopts)
			}
			if err != nil {
				errs[idx] = err
			}
		}(idx, tgt)
	}
	wg.Wait()

	var (
		terr        error
		taggedCount int
	)
	for _, err := range errs {
		if err == nil {
			taggedCount++
			continue
		}
		if err != nil {
			terr = err
		}
	}
	// don't return error if at least one target was tagged successfully
	if taggedCount == 0 && terr != nil {
		proxy.Err = terr
	}
	return proxy
}

// proxyGetTaggingToRepTarget proxies get tagging requests to remote targets for
// active-active replicated setups
func proxyGetTaggingToRepTarget(ctx context.Context, bucket, object string, opts ObjectOptions, proxyTargets *madmin.BucketTargets) (tgs *tags.Tags, proxy proxyResult) {
	// this option is set when active-active replication is in place between site A -> B,
	// and request hits site B that does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return nil, proxy
	}
	var wg sync.WaitGroup
	errs := make([]error, len(proxyTargets.Targets))
	tagSlc := make([]map[string]string, len(proxyTargets.Targets))
	for idx, t := range proxyTargets.Targets {
		tgt := globalBucketTargetSys.GetRemoteTargetClient(bucket, t.Arn)
		if tgt == nil || globalBucketTargetSys.isOffline(tgt.EndpointURL()) {
			continue
		}
		// if proxying explicitly disabled on remote target
		if tgt.disableProxy {
			continue
		}
		idx := idx
		wg.Add(1)
		go func(idx int, tgt *TargetClient) {
			defer wg.Done()
			var err error
			gopts := minio.GetObjectTaggingOptions{
				VersionID: opts.VersionID,
				Internal: minio.AdvancedObjectTaggingOptions{
					ReplicationProxyRequest: "true",
				},
			}
			tgs, err = tgt.GetObjectTagging(ctx, tgt.Bucket, object, gopts)
			if err != nil {
				errs[idx] = err
			} else {
				tagSlc[idx] = tgs.ToMap()
			}
		}(idx, tgt)
	}
	wg.Wait()
	for idx, err := range errs {
		errCode := minio.ToErrorResponse(err).Code
		if err != nil && errCode != "NoSuchKey" && errCode != "NoSuchVersion" {
			return nil, proxyResult{Err: err}
		}
		if err == nil {
			tgs, _ = tags.MapToObjectTags(tagSlc[idx])
		}
	}
	if len(errs) == 1 {
		proxy.Err = errs[0]
	}
	return tgs, proxy
}

func scheduleReplicationDelete(ctx context.Context, dv DeletedObjectReplicationInfo, o ObjectLayer) {
	globalReplicationPool.Get().queueReplicaDeleteTask(dv)
	for arn := range dv.ReplicationState.Targets {
		globalReplicationStats.Load().Update(dv.Bucket, replicatedTargetInfo{Arn: arn, Size: 0, Duration: 0, OpType: replication.DeleteReplicationType}, replication.Pending, replication.StatusType(""))
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
func (c replicationConfig) Resync(ctx context.Context, oi ObjectInfo, dsc ReplicateDecision, tgtStatuses map[string]replication.StatusType) (r ResyncDecision) {
	if c.Empty() {
		return r
	}

	// Now overlay existing object replication choices for target
	if oi.DeleteMarker {
		opts := replication.ObjectOpts{
			Name:           oi.Name,
			DeleteMarker:   oi.DeleteMarker,
			VersionID:      oi.VersionID,
			OpType:         replication.DeleteReplicationType,
			ExistingObject: true,
		}

		tgtArns := c.Config.FilterTargetArns(opts)
		// indicates no matching target with Existing object replication enabled.
		if len(tgtArns) == 0 {
			return r
		}
		for _, t := range tgtArns {
			opts.TargetArn = t
			// Update replication decision for target based on existing object replciation rule.
			dsc.Set(newReplicateTargetDecision(t, c.Replicate(opts), false))
		}
		return c.resync(oi, dsc, tgtStatuses)
	}

	// Ignore previous replication status when deciding if object can be re-replicated
	userDefined := cloneMSS(oi.UserDefined)
	delete(userDefined, xhttp.AmzBucketReplicationStatus)

	rdsc := mustReplicate(ctx, oi.Bucket, oi.Name, getMustReplicateOptions(userDefined, oi.UserTags, "", replication.ExistingObjectReplicationType, ObjectOptions{}))
	return c.resync(oi, rdsc, tgtStatuses)
}

// wrapper function for testability. Returns true if a new reset is requested on
// already replicated objects OR object qualifies for existing object replication
// and no reset requested.
func (c replicationConfig) resync(oi ObjectInfo, dsc ReplicateDecision, tgtStatuses map[string]replication.StatusType) (r ResyncDecision) {
	r = ResyncDecision{
		targets: make(map[string]ResyncTargetDecision, len(dsc.targetsMap)),
	}
	if c.remotes == nil {
		return r
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
	return r
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
		rs, ok = oi.UserDefined[xhttp.MinIOReplicationResetStatus] // for backward compatibility
	}
	if !ok { // existing object replication is enabled and object version is unreplicated so far.
		if resetID != "" && oi.ModTime.Before(resetBeforeDate) { // trigger replication if `mc replicate reset` requested
			rd.Replicate = true
			return rd
		}
		// For existing object reset - this condition is needed
		rd.Replicate = tgtStatus == ""
		return rd
	}
	if resetID == "" || resetBeforeDate.Equal(timeSentinel) { // no reset in progress
		return rd
	}

	// if already replicated, return true if a new reset was requested.
	splits := strings.SplitN(rs, ";", 2)
	if len(splits) != 2 {
		return rd
	}
	newReset := splits[1] != resetID
	if !newReset && tgtStatus == replication.Completed {
		// already replicated and no reset requested
		return rd
	}
	rd.Replicate = newReset && oi.ModTime.Before(resetBeforeDate)
	return rd
}

const resyncTimeInterval = time.Minute * 1

// PersistToDisk persists in-memory resync metadata stats to disk at periodic intervals
func (s *replicationResyncer) PersistToDisk(ctx context.Context, objectAPI ObjectLayer) {
	resyncTimer := time.NewTimer(resyncTimeInterval)
	defer resyncTimer.Stop()

	// For each bucket name, store the last timestamp of the
	// successful save of replication status in the backend disks.
	lastResyncStatusSave := make(map[string]time.Time)

	for {
		select {
		case <-resyncTimer.C:
			s.RLock()
			for bucket, brs := range s.statusMap {
				var updt bool
				// Save the replication status if one resync to any bucket target is still not finished
				for _, st := range brs.TargetsMap {
					if st.LastUpdate.Equal(timeSentinel) {
						updt = true
						break
					}
				}
				// Save the replication status if a new stats update is found and not saved in the backend yet
				if brs.LastUpdate.After(lastResyncStatusSave[bucket]) {
					updt = true
				}
				if updt {
					if err := saveResyncStatus(ctx, bucket, brs, objectAPI); err != nil {
						replLogIf(ctx, fmt.Errorf("could not save resync metadata to drive for %s - %w", bucket, err))
					} else {
						lastResyncStatusSave[bucket] = brs.LastUpdate
					}
				}
			}
			s.RUnlock()

			resyncTimer.Reset(resyncTimeInterval)
		case <-ctx.Done():
			// server could be restarting - need
			// to exit immediately
			return
		}
	}
}

const (
	resyncWorkerCnt        = 10 // limit of number of bucket resyncs is progress at any given time
	resyncParallelRoutines = 10 // number of parallel resync ops per bucket
)

func newresyncer() *replicationResyncer {
	rs := replicationResyncer{
		statusMap:      make(map[string]BucketReplicationResyncStatus),
		workerSize:     resyncWorkerCnt,
		resyncCancelCh: make(chan struct{}, resyncWorkerCnt),
		workerCh:       make(chan struct{}, resyncWorkerCnt),
	}
	for i := 0; i < rs.workerSize; i++ {
		rs.workerCh <- struct{}{}
	}
	return &rs
}

// mark status of replication resync on remote target for the bucket
func (s *replicationResyncer) markStatus(status ResyncStatusType, opts resyncOpts, objAPI ObjectLayer) {
	s.Lock()
	defer s.Unlock()

	m := s.statusMap[opts.bucket]
	st := m.TargetsMap[opts.arn]
	st.LastUpdate = UTCNow()
	st.ResyncStatus = status
	m.TargetsMap[opts.arn] = st
	m.LastUpdate = UTCNow()
	s.statusMap[opts.bucket] = m

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	saveResyncStatus(ctx, opts.bucket, m, objAPI)
}

// update replication resync stats for bucket's remote target
func (s *replicationResyncer) incStats(ts TargetReplicationResyncStatus, opts resyncOpts) {
	s.Lock()
	defer s.Unlock()
	m := s.statusMap[opts.bucket]
	st := m.TargetsMap[opts.arn]
	st.Object = ts.Object
	st.ReplicatedCount += ts.ReplicatedCount
	st.FailedCount += ts.FailedCount
	st.ReplicatedSize += ts.ReplicatedSize
	st.FailedSize += ts.FailedSize
	m.TargetsMap[opts.arn] = st
	m.LastUpdate = UTCNow()
	s.statusMap[opts.bucket] = m
}

// resyncBucket resyncs all qualifying objects as per replication rules for the target
// ARN
func (s *replicationResyncer) resyncBucket(ctx context.Context, objectAPI ObjectLayer, heal bool, opts resyncOpts) {
	select {
	case <-s.workerCh: // block till a worker is available
	case <-ctx.Done():
		return
	}

	resyncStatus := ResyncFailed
	defer func() {
		s.markStatus(resyncStatus, opts, objectAPI)
		globalSiteResyncMetrics.incBucket(opts, resyncStatus)
		s.workerCh <- struct{}{}
	}()
	// Allocate new results channel to receive ObjectInfo.
	objInfoCh := make(chan itemOrErr[ObjectInfo])
	cfg, err := getReplicationConfig(ctx, opts.bucket)
	if err != nil {
		replLogIf(ctx, fmt.Errorf("replication resync of %s for arn %s failed with %w", opts.bucket, opts.arn, err))
		return
	}
	tgts, err := globalBucketTargetSys.ListBucketTargets(ctx, opts.bucket)
	if err != nil {
		replLogIf(ctx, fmt.Errorf("replication resync of %s for arn %s failed  %w", opts.bucket, opts.arn, err))
		return
	}
	rcfg := replicationConfig{
		Config:  cfg,
		remotes: tgts,
	}
	tgtArns := cfg.FilterTargetArns(
		replication.ObjectOpts{
			OpType:    replication.ResyncReplicationType,
			TargetArn: opts.arn,
		})
	if len(tgtArns) != 1 {
		replLogIf(ctx, fmt.Errorf("replication resync failed for %s - arn specified %s is missing in the replication config", opts.bucket, opts.arn))
		return
	}
	tgt := globalBucketTargetSys.GetRemoteTargetClient(opts.bucket, opts.arn)
	if tgt == nil {
		replLogIf(ctx, fmt.Errorf("replication resync failed for %s - target could not be created for arn %s", opts.bucket, opts.arn))
		return
	}
	// mark resync status as resync started
	if !heal {
		s.markStatus(ResyncStarted, opts, objectAPI)
	}

	// Walk through all object versions - Walk() is always in ascending order needed to ensure
	// delete marker replicated to target after object version is first created.
	if err := objectAPI.Walk(ctx, opts.bucket, "", objInfoCh, WalkOptions{}); err != nil {
		replLogIf(ctx, err)
		return
	}

	s.RLock()
	m := s.statusMap[opts.bucket]
	st := m.TargetsMap[opts.arn]
	s.RUnlock()
	var lastCheckpoint string
	if st.ResyncStatus == ResyncStarted || st.ResyncStatus == ResyncFailed {
		lastCheckpoint = st.Object
	}
	workers := make([]chan ReplicateObjectInfo, resyncParallelRoutines)
	resultCh := make(chan TargetReplicationResyncStatus, 1)
	defer xioutil.SafeClose(resultCh)
	go func() {
		for r := range resultCh {
			s.incStats(r, opts)
			globalSiteResyncMetrics.updateMetric(r, opts.resyncID)
		}
	}()

	var wg sync.WaitGroup
	for i := range resyncParallelRoutines {
		wg.Add(1)
		workers[i] = make(chan ReplicateObjectInfo, 100)
		i := i
		go func(ctx context.Context, idx int) {
			defer wg.Done()
			for roi := range workers[idx] {
				select {
				case <-ctx.Done():
					return
				case <-s.resyncCancelCh:
				default:
				}
				traceFn := s.trace(tgt.ResetID, fmt.Sprintf("%s/%s (%s)", opts.bucket, roi.Name, roi.VersionID))
				if roi.DeleteMarker || !roi.VersionPurgeStatus.Empty() {
					versionID := ""
					dmVersionID := ""
					if roi.VersionPurgeStatus.Empty() {
						dmVersionID = roi.VersionID
					} else {
						versionID = roi.VersionID
					}

					doi := DeletedObjectReplicationInfo{
						DeletedObject: DeletedObject{
							ObjectName:            roi.Name,
							DeleteMarkerVersionID: dmVersionID,
							VersionID:             versionID,
							ReplicationState:      roi.ReplicationState,
							DeleteMarkerMTime:     DeleteMarkerMTime{roi.ModTime},
							DeleteMarker:          roi.DeleteMarker,
						},
						Bucket:    roi.Bucket,
						OpType:    replication.ExistingObjectReplicationType,
						EventType: ReplicateExistingDelete,
					}
					replicateDelete(ctx, doi, objectAPI)
				} else {
					roi.OpType = replication.ExistingObjectReplicationType
					roi.EventType = ReplicateExisting
					replicateObject(ctx, roi, objectAPI)
				}

				st := TargetReplicationResyncStatus{
					Object: roi.Name,
					Bucket: roi.Bucket,
				}

				_, err := tgt.StatObject(ctx, tgt.Bucket, roi.Name, minio.StatObjectOptions{
					VersionID: roi.VersionID,
					Internal: minio.AdvancedGetOptions{
						ReplicationProxyRequest: "false",
					},
				})
				sz := roi.Size
				if err != nil {
					if roi.DeleteMarker && isErrMethodNotAllowed(ErrorRespToObjectError(err, opts.bucket, roi.Name)) {
						st.ReplicatedCount++
					} else {
						st.FailedCount++
					}
					sz = 0
				} else {
					st.ReplicatedCount++
					st.ReplicatedSize += roi.Size
				}
				traceFn(sz, err)
				select {
				case <-ctx.Done():
					return
				case <-s.resyncCancelCh:
					return
				case resultCh <- st:
				}
			}
		}(ctx, i)
	}
	for res := range objInfoCh {
		if res.Err != nil {
			resyncStatus = ResyncFailed
			replLogIf(ctx, res.Err)
			return
		}
		select {
		case <-s.resyncCancelCh:
			resyncStatus = ResyncCanceled
			return
		case <-ctx.Done():
			return
		default:
		}
		if heal && lastCheckpoint != "" && lastCheckpoint != res.Item.Name {
			continue
		}
		lastCheckpoint = ""
		roi := getHealReplicateObjectInfo(res.Item, rcfg)
		if !roi.ExistingObjResync.mustResync() {
			continue
		}
		select {
		case <-s.resyncCancelCh:
			return
		case <-ctx.Done():
			return
		default:
			h := xxh3.HashString(roi.Bucket + roi.Name)
			workers[h%uint64(resyncParallelRoutines)] <- roi
		}
	}
	for i := range resyncParallelRoutines {
		xioutil.SafeClose(workers[i])
	}
	wg.Wait()
	resyncStatus = ResyncCompleted
}

// start replication resync for the remote target ARN specified
func (s *replicationResyncer) start(ctx context.Context, objAPI ObjectLayer, opts resyncOpts) error {
	if opts.bucket == "" {
		return fmt.Errorf("bucket name is empty")
	}
	if opts.arn == "" {
		return fmt.Errorf("target ARN specified for resync is empty")
	}
	// Check if the current bucket has quota restrictions, if not skip it
	cfg, err := getReplicationConfig(ctx, opts.bucket)
	if err != nil {
		return err
	}
	tgtArns := cfg.FilterTargetArns(
		replication.ObjectOpts{
			OpType:    replication.ResyncReplicationType,
			TargetArn: opts.arn,
		})

	if len(tgtArns) == 0 {
		return fmt.Errorf("arn %s specified for resync not found in replication config", opts.arn)
	}
	globalReplicationPool.Get().resyncer.RLock()
	data, ok := globalReplicationPool.Get().resyncer.statusMap[opts.bucket]
	globalReplicationPool.Get().resyncer.RUnlock()
	if !ok {
		data, err = loadBucketResyncMetadata(ctx, opts.bucket, objAPI)
		if err != nil {
			return err
		}
	}
	// validate if resync is in progress for this arn
	for tArn, st := range data.TargetsMap {
		if opts.arn == tArn && (st.ResyncStatus == ResyncStarted || st.ResyncStatus == ResyncPending) {
			return fmt.Errorf("Resync of bucket %s is already in progress for remote bucket %s", opts.bucket, opts.arn)
		}
	}

	status := TargetReplicationResyncStatus{
		ResyncID:         opts.resyncID,
		ResyncBeforeDate: opts.resyncBefore,
		StartTime:        UTCNow(),
		ResyncStatus:     ResyncPending,
		Bucket:           opts.bucket,
	}
	data.TargetsMap[opts.arn] = status
	if err = saveResyncStatus(ctx, opts.bucket, data, objAPI); err != nil {
		return err
	}

	globalReplicationPool.Get().resyncer.Lock()
	defer globalReplicationPool.Get().resyncer.Unlock()
	brs, ok := globalReplicationPool.Get().resyncer.statusMap[opts.bucket]
	if !ok {
		brs = BucketReplicationResyncStatus{
			Version:    resyncMetaVersion,
			TargetsMap: make(map[string]TargetReplicationResyncStatus),
		}
	}
	brs.TargetsMap[opts.arn] = status
	globalReplicationPool.Get().resyncer.statusMap[opts.bucket] = brs
	go globalReplicationPool.Get().resyncer.resyncBucket(GlobalContext, objAPI, false, opts)
	return nil
}

func (s *replicationResyncer) trace(resyncID string, path string) func(sz int64, err error) {
	startTime := time.Now()
	return func(sz int64, err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceReplicationResync) > 0 {
			globalTrace.Publish(replicationResyncTrace(resyncID, startTime, duration, path, err, sz))
		}
	}
}

func replicationResyncTrace(resyncID string, startTime time.Time, duration time.Duration, path string, err error, sz int64) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	funcName := fmt.Sprintf("replication.(resyncID=%s)", resyncID)
	return madmin.TraceInfo{
		TraceType: madmin.TraceReplicationResync,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  funcName,
		Duration:  duration,
		Path:      path,
		Error:     errStr,
		Bytes:     sz,
	}
}

// delete resync metadata from replication resync state in memory
func (p *ReplicationPool) deleteResyncMetadata(ctx context.Context, bucket string) {
	if p == nil {
		return
	}
	p.resyncer.Lock()
	delete(p.resyncer.statusMap, bucket)
	defer p.resyncer.Unlock()

	globalSiteResyncMetrics.deleteBucket(bucket)
}

// initResync - initializes bucket replication resync for all buckets.
func (p *ReplicationPool) initResync(ctx context.Context, buckets []string, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}
	// Load bucket metadata sys in background
	go p.startResyncRoutine(ctx, buckets, objAPI)
	return nil
}

func (p *ReplicationPool) startResyncRoutine(ctx context.Context, buckets []string, objAPI ObjectLayer) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Run the replication resync in a loop
	for {
		if err := p.loadResync(ctx, buckets, objAPI); err == nil {
			<-ctx.Done()
			return
		}
		duration := max(time.Duration(r.Float64()*float64(time.Minute)),
			// Make sure to sleep at least a second to avoid high CPU ticks.
			time.Second)
		time.Sleep(duration)
	}
}

// Loads bucket replication resync statuses into memory.
func (p *ReplicationPool) loadResync(ctx context.Context, buckets []string, objAPI ObjectLayer) error {
	// Make sure only one node running resync on the cluster.
	ctx, cancel := globalLeaderLock.GetLock(ctx)
	defer cancel()

	for index := range buckets {
		bucket := buckets[index]

		meta, err := loadBucketResyncMetadata(ctx, bucket, objAPI)
		if err != nil {
			if !errors.Is(err, errVolumeNotFound) {
				replLogIf(ctx, err)
			}
			continue
		}

		p.resyncer.Lock()
		p.resyncer.statusMap[bucket] = meta
		p.resyncer.Unlock()

		tgts := meta.cloneTgtStats()
		for arn, st := range tgts {
			switch st.ResyncStatus {
			case ResyncFailed, ResyncStarted, ResyncPending:
				go p.resyncer.resyncBucket(ctx, objAPI, true, resyncOpts{
					bucket:       bucket,
					arn:          arn,
					resyncID:     st.ResyncID,
					resyncBefore: st.ResyncBeforeDate,
				})
			}
		}
	}
	return nil
}

// load bucket resync metadata from disk
func loadBucketResyncMetadata(ctx context.Context, bucket string, objAPI ObjectLayer) (brs BucketReplicationResyncStatus, e error) {
	brs = newBucketResyncStatus(bucket)
	resyncDirPath := path.Join(bucketMetaPrefix, bucket, replicationDir)
	data, err := readConfig(GlobalContext, objAPI, pathJoin(resyncDirPath, resyncFileName))
	if err != nil && err != errConfigNotFound {
		return brs, err
	}
	if len(data) == 0 {
		// Seems to be empty.
		return brs, nil
	}
	if len(data) <= 4 {
		return brs, fmt.Errorf("replication resync: no data")
	}
	// Read resync meta header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case resyncMetaFormat:
	default:
		return brs, fmt.Errorf("resyncMeta: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case resyncMetaVersion:
	default:
		return brs, fmt.Errorf("resyncMeta: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}
	// OK, parse data.
	if _, err = brs.UnmarshalMsg(data[4:]); err != nil {
		return brs, err
	}

	switch brs.Version {
	case resyncMetaVersionV1:
	default:
		return brs, fmt.Errorf("unexpected resync meta version: %d", brs.Version)
	}
	return brs, nil
}

// save resync status to resync.bin
func saveResyncStatus(ctx context.Context, bucket string, brs BucketReplicationResyncStatus, objectAPI ObjectLayer) error {
	data := make([]byte, 4, brs.Msgsize()+4)

	// Initialize the resync meta header.
	binary.LittleEndian.PutUint16(data[0:2], resyncMetaFormat)
	binary.LittleEndian.PutUint16(data[2:4], resyncMetaVersion)

	buf, err := brs.MarshalMsg(data)
	if err != nil {
		return err
	}

	configFile := path.Join(bucketMetaPrefix, bucket, replicationDir, resyncFileName)
	return saveConfig(ctx, objectAPI, configFile, buf)
}

// getReplicationDiff returns un-replicated objects in a channel.
// If a non-nil channel is returned it must be consumed fully or
// the provided context must be canceled.
func getReplicationDiff(ctx context.Context, objAPI ObjectLayer, bucket string, opts madmin.ReplDiffOpts) (chan madmin.DiffInfo, error) {
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		replLogOnceIf(ctx, err, bucket)
		return nil, err
	}
	tgts, err := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
	if err != nil {
		replLogIf(ctx, err)
		return nil, err
	}

	objInfoCh := make(chan itemOrErr[ObjectInfo], 10)
	if err := objAPI.Walk(ctx, bucket, opts.Prefix, objInfoCh, WalkOptions{}); err != nil {
		replLogIf(ctx, err)
		return nil, err
	}
	rcfg := replicationConfig{
		Config:  cfg,
		remotes: tgts,
	}
	diffCh := make(chan madmin.DiffInfo, 4000)
	go func() {
		defer xioutil.SafeClose(diffCh)
		for res := range objInfoCh {
			if res.Err != nil {
				diffCh <- madmin.DiffInfo{Err: res.Err}
				return
			}
			if contextCanceled(ctx) {
				// Just consume input...
				continue
			}
			obj := res.Item

			// Ignore object prefixes which are excluded
			// from versioning via the MinIO bucket versioning extension.
			if globalBucketVersioningSys.PrefixSuspended(bucket, obj.Name) {
				continue
			}
			roi := getHealReplicateObjectInfo(obj, rcfg)
			switch roi.ReplicationStatus {
			case replication.Completed, replication.Replica:
				if !opts.Verbose {
					continue
				}
				fallthrough
			default:
				// ignore pre-existing objects that don't satisfy replication rule(s)
				if roi.ReplicationStatus.Empty() && !roi.ExistingObjResync.mustResync() {
					continue
				}
				tgtsMap := make(map[string]madmin.TgtDiffInfo)
				for arn, st := range roi.TargetStatuses {
					if opts.ARN == "" || opts.ARN == arn {
						if !opts.Verbose && (st == replication.Completed || st == replication.Replica) {
							continue
						}
						tgtsMap[arn] = madmin.TgtDiffInfo{
							ReplicationStatus: st.String(),
						}
					}
				}
				for arn, st := range roi.TargetPurgeStatuses {
					if opts.ARN == "" || opts.ARN == arn {
						if !opts.Verbose && st == replication.VersionPurgeComplete {
							continue
						}
						t, ok := tgtsMap[arn]
						if !ok {
							t = madmin.TgtDiffInfo{}
						}
						t.DeleteReplicationStatus = string(st)
						tgtsMap[arn] = t
					}
				}
				select {
				case diffCh <- madmin.DiffInfo{
					Object:                  obj.Name,
					VersionID:               obj.VersionID,
					LastModified:            obj.ModTime,
					IsDeleteMarker:          obj.DeleteMarker,
					ReplicationStatus:       string(roi.ReplicationStatus),
					DeleteReplicationStatus: string(roi.VersionPurgeStatus),
					ReplicationTimestamp:    roi.ReplicationTimestamp,
					Targets:                 tgtsMap,
				}:
				case <-ctx.Done():
					continue
				}
			}
		}
	}()
	return diffCh, nil
}

// QueueReplicationHeal is a wrapper for queueReplicationHeal
func QueueReplicationHeal(ctx context.Context, bucket string, oi ObjectInfo, retryCount int) {
	// ignore modtime zero objects
	if oi.ModTime.IsZero() {
		return
	}
	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		replLogOnceIf(ctx, err, bucket)
		return
	}
	tgts, _ := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
	queueReplicationHeal(ctx, bucket, oi, replicationConfig{
		Config:  rcfg,
		remotes: tgts,
	}, retryCount)
}

// queueReplicationHeal enqueues objects that failed replication OR eligible for resyncing through
// an ongoing resync operation or via existing objects replication configuration setting.
func queueReplicationHeal(ctx context.Context, bucket string, oi ObjectInfo, rcfg replicationConfig, retryCount int) (roi ReplicateObjectInfo) {
	// ignore modtime zero objects
	if oi.ModTime.IsZero() {
		return roi
	}

	if isVeeamSOSAPIObject(oi.Name) {
		return roi
	}
	if rcfg.Config == nil || rcfg.remotes == nil {
		return roi
	}
	roi = getHealReplicateObjectInfo(oi, rcfg)
	roi.RetryCount = uint32(retryCount)
	if !roi.Dsc.ReplicateAny() {
		return roi
	}
	// early return if replication already done, otherwise we need to determine if this
	// version is an existing object that needs healing.
	if oi.ReplicationStatus == replication.Completed && oi.VersionPurgeStatus.Empty() && !roi.ExistingObjResync.mustResync() {
		return roi
	}

	if roi.DeleteMarker || !roi.VersionPurgeStatus.Empty() {
		versionID := ""
		dmVersionID := ""
		if roi.VersionPurgeStatus.Empty() {
			dmVersionID = roi.VersionID
		} else {
			versionID = roi.VersionID
		}

		dv := DeletedObjectReplicationInfo{
			DeletedObject: DeletedObject{
				ObjectName:            roi.Name,
				DeleteMarkerVersionID: dmVersionID,
				VersionID:             versionID,
				ReplicationState:      roi.ReplicationState,
				DeleteMarkerMTime:     DeleteMarkerMTime{roi.ModTime},
				DeleteMarker:          roi.DeleteMarker,
			},
			Bucket:    roi.Bucket,
			OpType:    replication.HealReplicationType,
			EventType: ReplicateHealDelete,
		}
		// heal delete marker replication failure or versioned delete replication failure
		if roi.ReplicationStatus == replication.Pending ||
			roi.ReplicationStatus == replication.Failed ||
			roi.VersionPurgeStatus == replication.VersionPurgeFailed || roi.VersionPurgeStatus == replication.VersionPurgePending {
			globalReplicationPool.Get().queueReplicaDeleteTask(dv)
			return roi
		}
		// if replication status is Complete on DeleteMarker and existing object resync required
		if roi.ExistingObjResync.mustResync() && (roi.ReplicationStatus == replication.Completed || roi.ReplicationStatus.Empty()) {
			queueReplicateDeletesWrapper(dv, roi.ExistingObjResync)
			return roi
		}
		return roi
	}
	if roi.ExistingObjResync.mustResync() {
		roi.OpType = replication.ExistingObjectReplicationType
	}
	switch roi.ReplicationStatus {
	case replication.Pending, replication.Failed:
		roi.EventType = ReplicateHeal
		globalReplicationPool.Get().queueReplicaTask(roi)
		return roi
	}
	if roi.ExistingObjResync.mustResync() {
		roi.EventType = ReplicateExisting
		globalReplicationPool.Get().queueReplicaTask(roi)
	}
	return roi
}

const (
	mrfSaveInterval  = 5 * time.Minute
	mrfQueueInterval = mrfSaveInterval + time.Minute // A minute higher than save interval

	mrfRetryLimit = 3 // max number of retries before letting scanner catch up on this object version
	mrfMaxEntries = 1000000
)

func (p *ReplicationPool) persistMRF() {
	if !p.initialized() {
		return
	}

	entries := make(map[string]MRFReplicateEntry)
	mTimer := time.NewTimer(mrfSaveInterval)
	defer mTimer.Stop()

	saveMRFToDisk := func() {
		if len(entries) == 0 {
			return
		}

		// queue all entries for healing before overwriting the node mrf file
		if !contextCanceled(p.ctx) {
			p.queueMRFHeal()
		}

		p.saveMRFEntries(p.ctx, entries)

		entries = make(map[string]MRFReplicateEntry)
	}
	for {
		select {
		case <-mTimer.C:
			saveMRFToDisk()
			mTimer.Reset(mrfSaveInterval)
		case <-p.ctx.Done():
			p.mrfStopCh <- struct{}{}
			xioutil.SafeClose(p.mrfSaveCh)
			// We try to save if possible, but we don't care beyond that.
			saveMRFToDisk()
			return
		case e, ok := <-p.mrfSaveCh:
			if !ok {
				return
			}
			entries[e.versionID] = e

			if len(entries) >= mrfMaxEntries {
				saveMRFToDisk()
			}
		}
	}
}

func (p *ReplicationPool) queueMRFSave(entry MRFReplicateEntry) {
	if !p.initialized() {
		return
	}
	if entry.RetryCount > mrfRetryLimit { // let scanner catch up if retry count exceeded
		atomic.AddUint64(&p.stats.mrfStats.TotalDroppedCount, 1)
		atomic.AddUint64(&p.stats.mrfStats.TotalDroppedBytes, uint64(entry.sz))
		return
	}

	select {
	case <-GlobalContext.Done():
		return
	case <-p.mrfStopCh:
		return
	default:
		select {
		case p.mrfSaveCh <- entry:
		default:
			atomic.AddUint64(&p.stats.mrfStats.TotalDroppedCount, 1)
			atomic.AddUint64(&p.stats.mrfStats.TotalDroppedBytes, uint64(entry.sz))
		}
	}
}

func (p *ReplicationPool) persistToDrive(ctx context.Context, v MRFReplicateEntries) {
	newReader := func() io.ReadCloser {
		r, w := io.Pipe()
		go func() {
			// Initialize MRF meta header.
			var data [4]byte
			binary.LittleEndian.PutUint16(data[0:2], mrfMetaFormat)
			binary.LittleEndian.PutUint16(data[2:4], mrfMetaVersion)
			mw := msgp.NewWriter(w)
			n, err := mw.Write(data[:])
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if n != len(data) {
				w.CloseWithError(io.ErrShortWrite)
				return
			}
			err = v.EncodeMsg(mw)
			mw.Flush()
			w.CloseWithError(err)
		}()
		return r
	}

	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	for _, localDrive := range localDrives {
		r := newReader()
		err := localDrive.CreateFile(ctx, "", minioMetaBucket, pathJoin(replicationMRFDir, globalLocalNodeNameHex+".bin"), -1, r)
		r.Close()
		if err == nil {
			break
		}
	}
}

// save mrf entries to nodenamehex.bin
func (p *ReplicationPool) saveMRFEntries(ctx context.Context, entries map[string]MRFReplicateEntry) {
	if !p.initialized() {
		return
	}
	atomic.StoreUint64(&p.stats.mrfStats.LastFailedCount, uint64(len(entries)))
	if len(entries) == 0 {
		return
	}

	v := MRFReplicateEntries{
		Entries: entries,
		Version: mrfMetaVersion,
	}

	p.persistToDrive(ctx, v)
}

// load mrf entries from disk
func (p *ReplicationPool) loadMRF() (mrfRec MRFReplicateEntries, err error) {
	loadMRF := func(rc io.ReadCloser) (re MRFReplicateEntries, err error) {
		defer rc.Close()

		if !p.initialized() {
			return re, nil
		}
		var data [4]byte
		n, err := rc.Read(data[:])
		if err != nil {
			return re, err
		}
		if n != len(data) {
			return re, errors.New("replication mrf: no data")
		}
		// Read resync meta header
		switch binary.LittleEndian.Uint16(data[0:2]) {
		case mrfMetaFormat:
		default:
			return re, fmt.Errorf("replication mrf: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
		}
		switch binary.LittleEndian.Uint16(data[2:4]) {
		case mrfMetaVersion:
		default:
			return re, fmt.Errorf("replication mrf: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
		}

		// OK, parse data.
		// ignore any parsing errors, we do not care this file is generated again anyways.
		re.DecodeMsg(msgp.NewReader(rc))

		return re, nil
	}

	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	for _, localDrive := range localDrives {
		rc, err := localDrive.ReadFileStream(p.ctx, minioMetaBucket, pathJoin(replicationMRFDir, globalLocalNodeNameHex+".bin"), 0, -1)
		if err != nil {
			continue
		}

		mrfRec, err = loadMRF(rc)
		if err != nil {
			continue
		}

		// finally delete the file after processing mrf entries
		localDrive.Delete(p.ctx, minioMetaBucket, pathJoin(replicationMRFDir, globalLocalNodeNameHex+".bin"), DeleteOptions{})
		break
	}

	return mrfRec, nil
}

func (p *ReplicationPool) processMRF() {
	if !p.initialized() {
		return
	}
	pTimer := time.NewTimer(mrfQueueInterval)
	defer pTimer.Stop()
	for {
		select {
		case <-pTimer.C:
			// skip healing if all targets are offline
			var offlineCnt int
			tgts := globalBucketTargetSys.ListTargets(p.ctx, "", "")
			for _, tgt := range tgts {
				if globalBucketTargetSys.isOffline(tgt.URL()) {
					offlineCnt++
				}
			}
			if len(tgts) == offlineCnt {
				pTimer.Reset(mrfQueueInterval)
				continue
			}
			if err := p.queueMRFHeal(); err != nil && !osIsNotExist(err) {
				replLogIf(p.ctx, err)
			}
			pTimer.Reset(mrfQueueInterval)
		case <-p.ctx.Done():
			return
		}
	}
}

// process sends error logs to the heal channel for an attempt to heal replication.
func (p *ReplicationPool) queueMRFHeal() error {
	p.mrfMU.Lock()
	defer p.mrfMU.Unlock()

	if !p.initialized() {
		return errServerNotInitialized
	}

	mrfRec, err := p.loadMRF()
	if err != nil {
		return err
	}

	// queue replication heal in a goroutine to avoid holding up mrf save routine
	go func() {
		for vID, e := range mrfRec.Entries {
			ctx, cancel := context.WithTimeout(p.ctx, time.Second) // Do not waste more than a second on this.

			oi, err := p.objLayer.GetObjectInfo(ctx, e.Bucket, e.Object, ObjectOptions{
				VersionID: vID,
			})
			cancel()
			if err != nil {
				continue
			}

			QueueReplicationHeal(p.ctx, e.Bucket, oi, e.RetryCount)
		}
	}()

	return nil
}

func (p *ReplicationPool) initialized() bool {
	return p != nil && p.objLayer != nil
}

// getMRF returns MRF entries for this node.
func (p *ReplicationPool) getMRF(ctx context.Context, bucket string) (ch <-chan madmin.ReplicationMRF, err error) {
	mrfRec, err := p.loadMRF()
	if err != nil {
		return nil, err
	}

	mrfCh := make(chan madmin.ReplicationMRF, 100)
	go func() {
		defer xioutil.SafeClose(mrfCh)
		for vID, e := range mrfRec.Entries {
			if bucket != "" && e.Bucket != bucket {
				continue
			}
			select {
			case mrfCh <- madmin.ReplicationMRF{
				NodeName:   globalLocalNodeName,
				Object:     e.Object,
				VersionID:  vID,
				Bucket:     e.Bucket,
				RetryCount: e.RetryCount,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return mrfCh, nil
}

// validateReplicationDestinationOptions is used to configure the validation of the replication destination.
// validateReplicationDestination uses this to configure the validation.
type validateReplicationDestinationOptions struct {
	CheckRemoteBucket bool
	CheckReady        bool

	checkReadyErr sync.Map
}

func getCRCMeta(oi ObjectInfo, partNum int, h http.Header) (cs map[string]string, isMP bool) {
	meta := make(map[string]string)
	cs, isMP = oi.decryptChecksums(partNum, h)
	for k, v := range cs {
		if k == xhttp.AmzChecksumType {
			continue
		}
		cktype := hash.ChecksumStringToType(k)
		if cktype.IsSet() {
			meta[cktype.Key()] = v
			meta[xhttp.AmzChecksumAlgo] = cktype.String()
		}
	}
	return meta, isMP
}
