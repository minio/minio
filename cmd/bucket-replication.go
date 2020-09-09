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
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/bandwidth"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/event"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
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

func mustReplicateWeb(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string, permErr APIErrorCode) bool {
	if permErr != ErrNone {
		return false
	}
	return mustReplicater(ctx, r, bucket, object, meta, replStatus)
}

// mustReplicate returns true if object meets replication criteria.
func mustReplicate(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string) bool {
	if s3Err := isPutActionAllowed(getRequestAuthType(r), bucket, "", r, iampolicy.GetReplicationConfigurationAction); s3Err != ErrNone {
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

func putReplicationOpts(ctx context.Context, dest replication.Destination, objInfo ObjectInfo) (putOpts miniogo.PutObjectOptions) {
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
		UserMetadata:    meta,
		UserTags:        tag.ToMap(),
		ContentType:     objInfo.ContentType,
		ContentEncoding: objInfo.ContentEncoding,
		StorageClass:    sc,
		Internal: miniogo.AdvancedPutOptions{
			SourceVersionID:   objInfo.VersionID,
			ReplicationStatus: miniogo.ReplicationStatusReplica,
			SourceMTime:       objInfo.ModTime,
			SourceETag:        objInfo.ETag,
		},
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
func replicateObject(ctx context.Context, objInfo ObjectInfo, objectAPI ObjectLayer) {
	bucket := objInfo.Bucket
	object := objInfo.Name

	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}
	tgt := globalBucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	if tgt == nil {
		logger.LogIf(ctx, fmt.Errorf("failed to get target for bucket:%s arn:%s", bucket, cfg.RoleArn))
		return
	}
	gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{
		VersionID: objInfo.VersionID,
	})
	if err != nil {
		return
	}
	objInfo = gr.ObjInfo
	size, err := objInfo.GetActualSize()
	if err != nil {
		logger.LogIf(ctx, err)
		gr.Close()
		return
	}

	dest := cfg.GetDestination()
	if dest.Bucket == "" {
		gr.Close()
		return
	}

	// if heal encounters a pending replication status, either replication
	// has failed due to server shutdown or crawler and PutObject replication are in contention.
	healPending := objInfo.ReplicationStatus == replication.Pending

	// In the rare event that replication is in pending state either due to
	// server shut down/crash before replication completed or healing and PutObject
	// race - do an additional stat to see if the version ID exists
	if healPending {
		_, err := tgt.StatObject(ctx, dest.Bucket, object, miniogo.StatObjectOptions{VersionID: objInfo.VersionID})
		if err == nil {
			gr.Close()
			// object with same VersionID already exists, replication kicked off by
			// PutObject might have completed.
			return
		}
	}

	target, err := globalBucketMetadataSys.GetBucketTarget(bucket, cfg.RoleArn)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("failed to get target for replication bucket:%s cfg:%s err:%s", bucket, cfg.RoleArn, err))
		return
	}
	putOpts := putReplicationOpts(ctx, dest, objInfo)
	replicationStatus := replication.Complete

	// Setup bandwidth throttling
	totalNodesCount := len(GetRemotePeers(globalEndpoints)) + 1
	b := target.BandwidthLimit / int64(totalNodesCount)
	var headerSize int
	for k, v := range putOpts.Header() {
		headerSize += len(k) + len(v)
	}
	r := bandwidth.NewMonitoredReader(ctx, globalBucketMonitor, objInfo.Bucket, objInfo.Name, gr, headerSize, b)

	_, err = tgt.PutObject(ctx, dest.Bucket, object, r, size, "", "", putOpts)
	r.Close()
	if err != nil {
		replicationStatus = replication.Failed
	}
	objInfo.UserDefined[xhttp.AmzBucketReplicationStatus] = replicationStatus.String()
	if objInfo.UserTags != "" {
		objInfo.UserDefined[xhttp.AmzObjectTagging] = objInfo.UserTags
	}

	// FIXME: add support for missing replication events
	// - event.ObjectReplicationNotTracked
	// - event.ObjectReplicationMissedThreshold
	// - event.ObjectReplicationReplicatedAfterThreshold
	if replicationStatus == replication.Failed {
		sendEvent(eventArgs{
			EventName:  event.ObjectReplicationFailed,
			BucketName: bucket,
			Object:     objInfo,
			Host:       "Internal: [Replication]",
		})
	}

	objInfo.metadataOnly = true // Perform only metadata updates.
	if _, err = objectAPI.CopyObject(ctx, bucket, object, bucket, object, objInfo, ObjectOptions{
		VersionID: objInfo.VersionID,
	}, ObjectOptions{
		VersionID: objInfo.VersionID,
	}); err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to update replication metadata for %s: %s", objInfo.VersionID, err))
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

type replicationState struct {
	// add future metrics here
	replicaCh chan ObjectInfo
}

func (r *replicationState) queueReplicaTask(oi ObjectInfo) {
	select {
	case r.replicaCh <- oi:
	default:
	}
}

var (
	globalReplicationState *replicationState
	// TODO: currently keeping it conservative
	// but eventually can be tuned in future,
	// take only half the CPUs for replication
	// conservatively.
	globalReplicationConcurrent = runtime.GOMAXPROCS(0) / 2
)

func newReplicationState() *replicationState {

	// fix minimum concurrent replication to 1 for single CPU setup
	if globalReplicationConcurrent == 0 {
		globalReplicationConcurrent = 1
	}
	rs := &replicationState{
		replicaCh: make(chan ObjectInfo, 10000),
	}
	go func() {
		<-GlobalContext.Done()
		close(rs.replicaCh)
	}()
	return rs
}

// addWorker creates a new worker to process tasks
func (r *replicationState) addWorker(ctx context.Context, objectAPI ObjectLayer) {
	// Add a new worker.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case oi, ok := <-r.replicaCh:
				if !ok {
					return
				}
				replicateObject(ctx, oi, objectAPI)
			}
		}
	}()
}

func initBackgroundReplication(ctx context.Context, objectAPI ObjectLayer) {
	if globalReplicationState == nil {
		return
	}

	// Start with globalReplicationConcurrent.
	for i := 0; i < globalReplicationConcurrent; i++ {
		globalReplicationState.addWorker(ctx, objectAPI)
	}
}
