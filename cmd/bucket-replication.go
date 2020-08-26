/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"net/http"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/event"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// gets replication config associated to a given bucket name.
func getReplicationConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
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
	clnt := globalBucketTargetSys.GetReplicationTargetClient(ctx, rCfg.RoleArn)
	if clnt == nil {
		return false, BucketRemoteTargetNotFound{Bucket: bucket}
	}
	if found, _ := clnt.BucketExists(ctx, rCfg.GetDestination().Bucket); !found {
		return false, BucketReplicationDestinationNotFound{Bucket: rCfg.GetDestination().Bucket}
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

func mustReplicateWeb(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string, permErr APIErrorCode) bool {
	if permErr != ErrNone {
		return false
	}
	return mustReplicater(ctx, r, bucket, object, meta, replStatus)
}

// mustReplicate returns true if object meets replication criteria.
func mustReplicate(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string) bool {
	if s3Err := isPutActionAllowed(getRequestAuthType(r), bucket, object, r, iampolicy.GetReplicationConfigurationAction); s3Err != ErrNone {
		return false
	}
	return mustReplicater(ctx, r, bucket, object, meta, replStatus)
}

// mustReplicater returns true if object meets replication criteria.
func mustReplicater(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string) bool {
	if globalIsGateway {
		return false
	}
	if rs, ok := meta[xhttp.AmzBucketReplicationStatus]; ok {
		replStatus = rs
	}
	if replication.StatusType(replStatus) == replication.Replica {
		return false
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return false
	}
	opts := replication.ObjectOpts{
		Name: object,
		SSEC: crypto.SSEC.IsEncrypted(meta),
	}
	tagStr, ok := meta[xhttp.AmzObjectTagging]
	if ok {
		opts.UserTags = tagStr
	}
	return cfg.Replicate(opts)
}

func putReplicationOpts(dest replication.Destination, objInfo ObjectInfo) (putOpts miniogo.PutObjectOptions) {
	meta := make(map[string]string)
	for k, v := range objInfo.UserDefined {
		if k == xhttp.AmzBucketReplicationStatus {
			continue
		}
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			continue
		}
		meta[k] = v
	}
	tag, err := tags.ParseObjectTags(objInfo.UserTags)
	if err != nil {
		return
	}
	sc := dest.StorageClass
	if sc == "" {
		sc = objInfo.StorageClass
	}
	putOpts = miniogo.PutObjectOptions{
		UserMetadata:         meta,
		UserTags:             tag.ToMap(),
		ContentType:          objInfo.ContentType,
		ContentEncoding:      objInfo.ContentEncoding,
		StorageClass:         sc,
		ReplicationVersionID: objInfo.VersionID,
		ReplicationStatus:    miniogo.ReplicationStatusReplica,
		ReplicationMTime:     objInfo.ModTime,
		ReplicationETag:      objInfo.ETag,
	}
	if mode, ok := objInfo.UserDefined[xhttp.AmzObjectLockMode]; ok {
		rmode := miniogo.RetentionMode(mode)
		putOpts.Mode = rmode
	}
	if retainDateStr, ok := objInfo.UserDefined[xhttp.AmzObjectLockRetainUntilDate]; ok {
		rdate, err := time.Parse(time.RFC3339, retainDateStr)
		if err != nil {
			return
		}
		putOpts.RetainUntilDate = rdate
	}
	if lhold, ok := objInfo.UserDefined[xhttp.AmzObjectLockLegalHold]; ok {
		putOpts.LegalHold = miniogo.LegalHoldStatus(lhold)
	}
	if crypto.S3.IsEncrypted(objInfo.UserDefined) {
		putOpts.ServerSideEncryption = encrypt.NewSSE()
	}
	return
}

// replicateObject replicates the specified version of the object to destination bucket
// The source object is then updated to reflect the replication status.
func replicateObject(ctx context.Context, bucket, object, versionID string, objectAPI ObjectLayer, eventArg *eventArgs, healPending bool) {
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}
	tgt := globalBucketTargetSys.GetReplicationTargetClient(ctx, cfg.RoleArn)
	if tgt == nil {
		return
	}
	gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		return
	}
	defer gr.Close()
	objInfo := gr.ObjInfo
	size, err := objInfo.GetActualSize()
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}

	dest := cfg.GetDestination()
	if dest.Bucket == "" {
		return
	}
	// In the rare event that replication is in pending state either due to
	// server shut down/crash before replication completed or healing and PutObject
	// race - do an additional stat to see if the version ID exists
	if healPending {
		_, err := tgt.StatObject(ctx, dest.Bucket, object, miniogo.StatObjectOptions{VersionID: objInfo.VersionID})
		if err == nil {
			// object with same VersionID already exists, replication kicked off by
			// PutObject might have completed.
			return
		}
	}
	putOpts := putReplicationOpts(dest, objInfo)

	replicationStatus := replication.Complete
	startTime := time.Now()
	_, err = tgt.PutObject(ctx, dest.Bucket, object, gr, size, "", "", putOpts)
	if err != nil {
		replicationStatus = replication.Failed
		// Notify replication failure  event.
		if eventArg == nil {
			eventArg = &eventArgs{
				BucketName: bucket,
				Object:     objInfo,
				Host:       "Internal: [Replication]",
			}
		}
		eventArg.EventName = event.OperationReplicationFailed
		eventArg.Object.UserDefined[xhttp.AmzBucketReplicationStatus] = replicationStatus.String()
		sendEvent(*eventArg)
	} else {
		globalConnStats.incReplicationOutputBytes(int(size))
		httpReplicationDuration.Observe(time.Since(startTime).Seconds())
	}
	objInfo.UserDefined[xhttp.AmzBucketReplicationStatus] = replicationStatus.String()
	if objInfo.UserTags != "" {
		objInfo.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
	}
	objInfo.metadataOnly = true // Perform only metadata updates.
	if _, err = objectAPI.CopyObject(ctx, bucket, object, bucket, object, objInfo, ObjectOptions{
		VersionID: objInfo.VersionID,
	}, ObjectOptions{VersionID: objInfo.VersionID}); err != nil {
		logger.LogIf(ctx, err)
	}
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
