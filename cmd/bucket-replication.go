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
	"strings"
	"sync"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/event"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

// BucketReplicationSys represents replication subsystem
type BucketReplicationSys struct {
	sync.RWMutex
	targetsMap    map[string]*miniogo.Core
	targetsARNMap map[string]string
}

// GetConfig - gets replication config associated to a given bucket name.
func (sys *BucketReplicationSys) GetConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketReplicationConfigNotFound{Bucket: bucketName}
	}

	return globalBucketMetadataSys.GetReplicationConfig(ctx, bucketName)
}

// SetTarget - sets a new minio-go client replication target for this bucket.
func (sys *BucketReplicationSys) SetTarget(ctx context.Context, bucket string, tgt *madmin.BucketReplicationTarget) error {
	if globalIsGateway {
		return nil
	}
	// delete replication targets that were removed
	if tgt.Empty() {
		sys.Lock()
		if currTgt, ok := sys.targetsMap[bucket]; ok {
			delete(sys.targetsARNMap, currTgt.EndpointURL().String())
		}
		delete(sys.targetsMap, bucket)
		sys.Unlock()
		return nil
	}
	clnt, err := getReplicationTargetClient(tgt)
	if err != nil {
		return BucketReplicationTargetNotFound{Bucket: tgt.TargetBucket}
	}
	ok, err := clnt.BucketExists(ctx, tgt.TargetBucket)
	if err != nil {
		return err
	}
	if !ok {
		return BucketReplicationDestinationNotFound{Bucket: tgt.TargetBucket}
	}
	sys.Lock()
	sys.targetsMap[bucket] = clnt
	sys.targetsARNMap[tgt.URL()] = tgt.Arn
	sys.Unlock()
	return nil
}

// GetTargetClient returns minio-go client for target instance
func (sys *BucketReplicationSys) GetTargetClient(ctx context.Context, bucket string) *miniogo.Core {
	var clnt *miniogo.Core
	sys.RLock()
	if c, ok := sys.targetsMap[bucket]; ok {
		clnt = c
	}
	sys.RUnlock()
	return clnt
}

// validateDestination returns error if replication destination bucket missing or not configured
// It also returns true if replication destination is same as this server.
func (sys *BucketReplicationSys) validateDestination(ctx context.Context, bucket string, rCfg *replication.Config) (bool, error) {
	clnt := sys.GetTargetClient(ctx, bucket)
	if clnt == nil {
		return false, BucketReplicationTargetNotFound{Bucket: bucket}
	}
	if found, _ := clnt.BucketExists(ctx, rCfg.GetDestination().Bucket); !found {
		return false, BucketReplicationDestinationNotFound{Bucket: rCfg.GetDestination().Bucket}
	}
	// validate replication ARN against target endpoint
	for k, v := range sys.targetsARNMap {
		if v == rCfg.ReplicationArn {
			if k == clnt.EndpointURL().String() {
				sameTarget, _ := isLocalHost(clnt.EndpointURL().Hostname(), clnt.EndpointURL().Port(), globalMinioPort)
				return sameTarget, nil
			}
		}
	}
	return false, BucketReplicationTargetNotFound{Bucket: bucket}
}

// NewBucketReplicationSys - creates new replication system.
func NewBucketReplicationSys() *BucketReplicationSys {
	return &BucketReplicationSys{
		targetsMap:    make(map[string]*miniogo.Core),
		targetsARNMap: make(map[string]string),
	}
}

// Init initializes the bucket replication subsystem for buckets with replication config
func (sys *BucketReplicationSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, replication is not supported.
	if globalIsGateway {
		return nil
	}

	// Load bucket replication targets once during boot.
	sys.load(ctx, buckets, objAPI)
	return nil
}

// create minio-go clients for buckets having replication targets
func (sys *BucketReplicationSys) load(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	for _, bucket := range buckets {
		tgt, err := globalBucketMetadataSys.GetReplicationTargetConfig(bucket.Name)
		if err != nil {
			continue
		}
		if tgt == nil || tgt.Empty() {
			continue
		}
		tgtClient, err := getReplicationTargetClient(tgt)
		if err != nil {
			continue
		}
		sys.Lock()
		sys.targetsMap[bucket.Name] = tgtClient
		sys.targetsARNMap[tgt.URL()] = tgt.Arn
		sys.Unlock()
	}
}

// GetARN returns the ARN associated with replication target URL
func (sys *BucketReplicationSys) getARN(endpoint string) string {
	return sys.targetsARNMap[endpoint]
}

// getReplicationTargetInstanceTransport contains a singleton roundtripper.
var getReplicationTargetInstanceTransport http.RoundTripper
var getReplicationTargetInstanceTransportOnce sync.Once

// Returns a minio-go Client configured to access remote host described in replication target config.
var getReplicationTargetClient = func(tcfg *madmin.BucketReplicationTarget) (*miniogo.Core, error) {
	config := tcfg.Credentials
	// if Signature version '4' use NewV4 directly.
	creds := credentials.NewStaticV4(config.AccessKey, config.SecretKey, "")
	// if Signature version '2' use NewV2 directly.
	if strings.ToUpper(tcfg.API) == "S3V2" {
		creds = credentials.NewStaticV2(config.AccessKey, config.SecretKey, "")
	}

	getReplicationTargetInstanceTransportOnce.Do(func() {
		getReplicationTargetInstanceTransport = NewGatewayHTTPTransport()
	})
	core, err := miniogo.NewCore(tcfg.Endpoint, &miniogo.Options{
		Creds:     creds,
		Secure:    tcfg.IsSSL,
		Transport: getReplicationTargetInstanceTransport,
	})
	return core, err
}

// mustReplicate returns true if object meets replication criteria.
func (sys *BucketReplicationSys) mustReplicate(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string) bool {
	if globalIsGateway {
		return false
	}
	if rs, ok := meta[xhttp.AmzBucketReplicationStatus]; ok {
		replStatus = rs
	}
	if replication.StatusType(replStatus) == replication.Replica {
		return false
	}
	if s3Err := isPutActionAllowed(getRequestAuthType(r), bucket, object, r, iampolicy.GetReplicationConfigurationAction); s3Err != ErrNone {
		return false
	}
	cfg, err := globalBucketReplicationSys.GetConfig(ctx, bucket)
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
		meta[k] = v
	}

	tag, err := tags.ParseObjectTags(objInfo.UserTags)
	if err != nil {
		return
	}
	putOpts = miniogo.PutObjectOptions{
		UserMetadata:         meta,
		UserTags:             tag.ToMap(),
		ContentType:          objInfo.ContentType,
		ContentEncoding:      objInfo.ContentEncoding,
		StorageClass:         dest.StorageClass,
		ReplicationVersionID: objInfo.VersionID,
		ReplicationStatus:    miniogo.ReplicationStatusReplica,
		ReplicationMTime:     objInfo.ModTime,
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
	cfg, err := globalBucketReplicationSys.GetConfig(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}
	tgt := globalBucketReplicationSys.GetTargetClient(ctx, bucket)
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

// getReplicationARN gets existing ARN for an endpoint or generates a new one.
func (sys *BucketReplicationSys) getReplicationARN(endpoint string) string {
	arn, ok := sys.targetsARNMap[endpoint]
	if ok {
		return arn
	}
	return fmt.Sprintf("arn:minio:s3::%s:*", mustGetUUID())
}
