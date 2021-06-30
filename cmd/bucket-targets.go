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
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio-go/v7"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
)

const (
	defaultHealthCheckDuration = 100 * time.Second
)

// BucketTargetSys represents bucket targets subsystem
type BucketTargetSys struct {
	sync.RWMutex
	arnRemotesMap map[string]*TargetClient
	targetsMap    map[string][]madmin.BucketTarget
}

// ListTargets lists bucket targets across tenant or for individual bucket, and returns
// results filtered by arnType
func (sys *BucketTargetSys) ListTargets(ctx context.Context, bucket, arnType string) (targets []madmin.BucketTarget) {
	if bucket != "" {
		if ts, err := sys.ListBucketTargets(ctx, bucket); err == nil {
			for _, t := range ts.Targets {
				if string(t.Type) == arnType || arnType == "" {
					targets = append(targets, t.Clone())
				}
			}
		}
		return targets
	}
	sys.RLock()
	defer sys.RUnlock()
	for _, tgts := range sys.targetsMap {
		for _, t := range tgts {
			if string(t.Type) == arnType || arnType == "" {
				targets = append(targets, t.Clone())
			}
		}
	}
	return
}

// ListBucketTargets - gets list of bucket targets for this bucket.
func (sys *BucketTargetSys) ListBucketTargets(ctx context.Context, bucket string) (*madmin.BucketTargets, error) {
	sys.RLock()
	defer sys.RUnlock()

	tgts, ok := sys.targetsMap[bucket]
	if ok {
		return &madmin.BucketTargets{Targets: tgts}, nil
	}
	return nil, BucketRemoteTargetNotFound{Bucket: bucket}
}

// Delete clears targets present for a bucket
func (sys *BucketTargetSys) Delete(bucket string) {
	sys.Lock()
	defer sys.Unlock()
	tgts, ok := sys.targetsMap[bucket]
	if !ok {
		return
	}
	for _, t := range tgts {
		delete(sys.arnRemotesMap, t.Arn)
	}
	delete(sys.targetsMap, bucket)
}

// SetTarget - sets a new minio-go client target for this bucket.
func (sys *BucketTargetSys) SetTarget(ctx context.Context, bucket string, tgt *madmin.BucketTarget, update bool) error {
	if globalIsGateway {
		return nil
	}
	if !tgt.Type.IsValid() && !update {
		return BucketRemoteArnTypeInvalid{Bucket: bucket}
	}
	clnt, err := sys.getRemoteTargetClient(tgt)
	if err != nil {
		return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket}
	}
	// validate if target credentials are ok
	if _, err = clnt.BucketExists(ctx, tgt.TargetBucket); err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchBucket" {
			return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket}
		}
		return BucketRemoteConnectionErr{Bucket: tgt.TargetBucket, Err: err}
	}
	if tgt.Type == madmin.ReplicationService {
		if !globalIsErasure {
			return NotImplemented{Message: "Replication is not implemented in " + getMinioMode()}
		}
		if !globalBucketVersioningSys.Enabled(bucket) {
			return BucketReplicationSourceNotVersioned{Bucket: bucket}
		}
		vcfg, err := clnt.GetBucketVersioning(ctx, tgt.TargetBucket)
		if err != nil {
			return BucketRemoteConnectionErr{Bucket: tgt.TargetBucket, Err: err}
		}
		if vcfg.Status != string(versioning.Enabled) {
			return BucketRemoteTargetNotVersioned{Bucket: tgt.TargetBucket}
		}
	}
	sys.Lock()
	defer sys.Unlock()

	tgts := sys.targetsMap[bucket]

	newtgts := make([]madmin.BucketTarget, len(tgts))
	found := false
	for idx, t := range tgts {
		if t.Type == tgt.Type {
			if t.Arn == tgt.Arn && !update {
				return BucketRemoteAlreadyExists{Bucket: t.TargetBucket}
			}
			newtgts[idx] = *tgt
			found = true
			continue
		}
		newtgts[idx] = t
	}
	if !found && !update {
		newtgts = append(newtgts, *tgt)
	}

	sys.targetsMap[bucket] = newtgts
	sys.arnRemotesMap[tgt.Arn] = clnt
	sys.updateBandwidthLimit(bucket, tgt.BandwidthLimit)
	return nil
}

func (sys *BucketTargetSys) updateBandwidthLimit(bucket string, limit int64) {
	if globalIsGateway {
		return
	}
	if limit == 0 {
		globalBucketMonitor.DeleteBucket(bucket)
		return
	}
	// Setup bandwidth throttling

	globalBucketMonitor.SetBandwidthLimit(bucket, limit)
}

// RemoveTarget - removes a remote bucket target for this source bucket.
func (sys *BucketTargetSys) RemoveTarget(ctx context.Context, bucket, arnStr string) error {
	if globalIsGateway {
		return nil
	}
	if !globalIsErasure {
		return NotImplemented{Message: "Replication is not implemented in " + getMinioMode()}
	}

	if arnStr == "" {
		return BucketRemoteArnInvalid{Bucket: bucket}
	}

	arn, err := madmin.ParseARN(arnStr)
	if err != nil {
		return BucketRemoteArnInvalid{Bucket: bucket}
	}

	if arn.Type == madmin.ReplicationService {
		// reject removal of remote target if replication configuration is present
		rcfg, err := getReplicationConfig(ctx, bucket)
		if err == nil && rcfg.RoleArn == arnStr {
			sys.RLock()
			_, ok := sys.arnRemotesMap[arnStr]
			sys.RUnlock()
			if ok {
				return BucketRemoteRemoveDisallowed{Bucket: bucket}
			}
		}
	}

	// delete ARN type from list of matching targets
	sys.Lock()
	defer sys.Unlock()
	found := false
	tgts, ok := sys.targetsMap[bucket]
	if !ok {
		return BucketRemoteTargetNotFound{Bucket: bucket}
	}
	targets := make([]madmin.BucketTarget, 0, len(tgts))
	for _, tgt := range tgts {
		if tgt.Arn != arnStr {
			targets = append(targets, tgt)
			continue
		}
		found = true
	}
	if !found {
		return BucketRemoteTargetNotFound{Bucket: bucket}
	}
	sys.targetsMap[bucket] = targets
	delete(sys.arnRemotesMap, arnStr)
	sys.updateBandwidthLimit(bucket, 0)
	return nil
}

// GetRemoteTargetClient returns minio-go client for replication target instance
func (sys *BucketTargetSys) GetRemoteTargetClient(ctx context.Context, arn string) *TargetClient {
	sys.RLock()
	defer sys.RUnlock()
	return sys.arnRemotesMap[arn]
}

// GetRemoteBucketTargetByArn returns BucketTarget for a ARN
func (sys *BucketTargetSys) GetRemoteBucketTargetByArn(ctx context.Context, bucket, arn string) madmin.BucketTarget {
	sys.RLock()
	defer sys.RUnlock()
	var tgt madmin.BucketTarget
	for _, t := range sys.targetsMap[bucket] {
		if t.Arn == arn {
			tgt = t.Clone()
			tgt.Credentials = t.Credentials
			return tgt
		}
	}
	return tgt
}

// NewBucketTargetSys - creates new replication system.
func NewBucketTargetSys() *BucketTargetSys {
	return &BucketTargetSys{
		arnRemotesMap: make(map[string]*TargetClient),
		targetsMap:    make(map[string][]madmin.BucketTarget),
	}
}

// Init initializes the bucket targets subsystem for buckets which have targets configured.
func (sys *BucketTargetSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, bucket targets is not supported.
	if globalIsGateway {
		return nil
	}

	// Load bucket targets once during boot in background.
	go sys.load(ctx, buckets, objAPI)
	return nil
}

// UpdateAllTargets updates target to reflect metadata updates
func (sys *BucketTargetSys) UpdateAllTargets(bucket string, tgts *madmin.BucketTargets) {
	if sys == nil {
		return
	}
	sys.Lock()
	defer sys.Unlock()
	if tgts == nil || tgts.Empty() {
		// remove target and arn association
		if tgts, ok := sys.targetsMap[bucket]; ok {
			for _, t := range tgts {
				delete(sys.arnRemotesMap, t.Arn)
			}
		}
		delete(sys.targetsMap, bucket)
		sys.updateBandwidthLimit(bucket, 0)
		return
	}

	if len(tgts.Targets) > 0 {
		sys.targetsMap[bucket] = tgts.Targets
	}
	for _, tgt := range tgts.Targets {
		tgtClient, err := sys.getRemoteTargetClient(&tgt)
		if err != nil {
			continue
		}
		sys.arnRemotesMap[tgt.Arn] = tgtClient
		sys.updateBandwidthLimit(bucket, tgt.BandwidthLimit)
	}
	sys.targetsMap[bucket] = tgts.Targets
}

// create minio-go clients for buckets having remote targets
func (sys *BucketTargetSys) load(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	for _, bucket := range buckets {
		cfg, err := globalBucketMetadataSys.GetBucketTargetsConfig(bucket.Name)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if cfg == nil || cfg.Empty() {
			continue
		}
		if len(cfg.Targets) > 0 {
			sys.targetsMap[bucket.Name] = cfg.Targets
		}
		for _, tgt := range cfg.Targets {
			tgtClient, err := sys.getRemoteTargetClient(&tgt)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
			sys.arnRemotesMap[tgt.Arn] = tgtClient
			sys.updateBandwidthLimit(bucket.Name, tgt.BandwidthLimit)
		}
		sys.targetsMap[bucket.Name] = cfg.Targets
	}
}

// getRemoteTargetInstanceTransport contains a singleton roundtripper.
var getRemoteTargetInstanceTransport http.RoundTripper
var getRemoteTargetInstanceTransportOnce sync.Once

// Returns a minio-go Client configured to access remote host described in replication target config.
func (sys *BucketTargetSys) getRemoteTargetClient(tcfg *madmin.BucketTarget) (*TargetClient, error) {
	config := tcfg.Credentials
	creds := credentials.NewStaticV4(config.AccessKey, config.SecretKey, "")
	getRemoteTargetInstanceTransportOnce.Do(func() {
		getRemoteTargetInstanceTransport = NewRemoteTargetHTTPTransport()
	})
	api, err := minio.New(tcfg.Endpoint, &miniogo.Options{
		Creds:     creds,
		Secure:    tcfg.Secure,
		Region:    tcfg.Region,
		Transport: getRemoteTargetInstanceTransport,
	})
	if err != nil {
		return nil, err
	}
	hcDuration := defaultHealthCheckDuration
	if tcfg.HealthCheckDuration >= 1 { // require minimum health check duration of 1 sec.
		hcDuration = tcfg.HealthCheckDuration
	}
	tc := &TargetClient{
		Client:              api,
		healthCheckDuration: hcDuration,
		replicateSync:       tcfg.ReplicationSync,
		Bucket:              tcfg.TargetBucket,
		StorageClass:        tcfg.StorageClass,
		disableProxy:        tcfg.DisableProxy,
	}
	return tc, nil
}

// getRemoteARN gets existing ARN for an endpoint or generates a new one.
func (sys *BucketTargetSys) getRemoteARN(bucket string, target *madmin.BucketTarget) string {
	if target == nil {
		return ""
	}
	tgts := sys.targetsMap[bucket]
	for _, tgt := range tgts {
		if tgt.Type == target.Type && tgt.TargetBucket == target.TargetBucket && target.URL().String() == tgt.URL().String() {
			return tgt.Arn
		}
	}
	if !target.Type.IsValid() {
		return ""
	}
	return generateARN(target)
}

// generate ARN that is unique to this target type
func generateARN(t *madmin.BucketTarget) string {
	hash := sha256.New()
	hash.Write([]byte(t.Type))
	hash.Write([]byte(t.Region))
	hash.Write([]byte(t.TargetBucket))
	hashSum := hex.EncodeToString(hash.Sum(nil))
	arn := madmin.ARN{
		Type:   t.Type,
		ID:     hashSum,
		Region: t.Region,
		Bucket: t.TargetBucket,
	}
	return arn.String()
}

// Returns parsed target config. If KMS is configured, remote target is decrypted
func parseBucketTargetConfig(bucket string, cdata, cmetadata []byte) (*madmin.BucketTargets, error) {
	var (
		data []byte
		err  error
		t    madmin.BucketTargets
		meta map[string]string
	)
	if len(cdata) == 0 {
		return nil, nil
	}
	data = cdata
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if len(cmetadata) != 0 {
		if err := json.Unmarshal(cmetadata, &meta); err != nil {
			return nil, err
		}
		if crypto.S3.IsEncrypted(meta) {
			if data, err = decryptBucketMetadata(cdata, bucket, meta, kms.Context{
				bucket:            bucket,
				bucketTargetsFile: bucketTargetsFile,
			}); err != nil {
				return nil, err
			}
		}
	}

	if err = json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// TargetClient is the struct for remote target client.
type TargetClient struct {
	*miniogo.Client
	healthCheckDuration time.Duration
	Bucket              string // remote bucket target
	replicateSync       bool
	StorageClass        string // storage class on remote
	disableProxy        bool
}
