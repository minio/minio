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
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	minio "github.com/minio/minio-go/v7"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/pkg/bucket/versioning"
	"github.com/minio/minio/pkg/madmin"
	sha256 "github.com/minio/sha256-simd"
)

const (
	defaultHealthCheckDuration = 60 * time.Second
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
		return BucketRemoteConnectionErr{Bucket: tgt.TargetBucket}
	}
	if tgt.Type == madmin.ReplicationService {
		if !globalIsErasure {
			return NotImplemented{}
		}
		if !globalBucketVersioningSys.Enabled(bucket) {
			return BucketReplicationSourceNotVersioned{Bucket: bucket}
		}
		vcfg, err := clnt.GetBucketVersioning(ctx, tgt.TargetBucket)
		if err != nil {
			return BucketRemoteConnectionErr{Bucket: tgt.TargetBucket}
		}
		if vcfg.Status != string(versioning.Enabled) {
			return BucketRemoteTargetNotVersioned{Bucket: tgt.TargetBucket}
		}
	}
	if tgt.Type == madmin.ILMService {
		if globalBucketVersioningSys.Enabled(bucket) {
			vcfg, err := clnt.GetBucketVersioning(ctx, tgt.TargetBucket)
			if err != nil {
				if minio.ToErrorResponse(err).Code == "NoSuchBucket" {
					return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket}
				}
				return BucketRemoteConnectionErr{Bucket: tgt.TargetBucket}
			}
			if vcfg.Status != string(versioning.Enabled) {
				return BucketRemoteTargetNotVersioned{Bucket: tgt.TargetBucket}
			}
		}
	}
	sys.Lock()
	defer sys.Unlock()

	tgts := sys.targetsMap[bucket]
	newtgts := make([]madmin.BucketTarget, len(tgts))
	labels := make(map[string]struct{}, len(tgts))
	found := false
	for idx, t := range tgts {
		labels[t.Label] = struct{}{}
		if t.Type == tgt.Type {
			if t.Arn == tgt.Arn && !update {
				return BucketRemoteAlreadyExists{Bucket: t.TargetBucket}
			}
			if t.Label == tgt.Label && !update {
				return BucketRemoteLabelInUse{Bucket: t.TargetBucket}
			}
			newtgts[idx] = *tgt
			found = true
			continue
		}
		newtgts[idx] = t
	}
	if _, ok := labels[tgt.Label]; ok && !update {
		return BucketRemoteLabelInUse{Bucket: tgt.TargetBucket}
	}
	if !found && !update {
		newtgts = append(newtgts, *tgt)
	}

	sys.targetsMap[bucket] = newtgts
	sys.arnRemotesMap[tgt.Arn] = clnt
	return nil
}

// RemoveTarget - removes a remote bucket target for this source bucket.
func (sys *BucketTargetSys) RemoveTarget(ctx context.Context, bucket, arnStr string) error {
	if globalIsGateway {
		return nil
	}
	if arnStr == "" {
		return BucketRemoteArnInvalid{Bucket: bucket}
	}
	arn, err := madmin.ParseARN(arnStr)
	if err != nil {
		return BucketRemoteArnInvalid{Bucket: bucket}
	}
	if arn.Type == madmin.ReplicationService {
		if !globalIsErasure {
			return NotImplemented{}
		}
		// reject removal of remote target if replication configuration is present
		rcfg, err := getReplicationConfig(ctx, bucket)
		if err == nil && rcfg.RoleArn == arnStr {
			if _, ok := sys.arnRemotesMap[arnStr]; ok {
				return BucketRemoteRemoveDisallowed{Bucket: bucket}
			}
		}
	}
	if arn.Type == madmin.ILMService {
		// reject removal of remote target if lifecycle transition uses this arn
		config, err := globalBucketMetadataSys.GetLifecycleConfig(bucket)
		if err == nil && transitionSCInUse(ctx, config, bucket, arnStr) {
			if _, ok := sys.arnRemotesMap[arnStr]; ok {
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
	return nil
}

// GetRemoteTargetClient returns minio-go client for replication target instance
func (sys *BucketTargetSys) GetRemoteTargetClient(ctx context.Context, arn string) *TargetClient {
	sys.RLock()
	defer sys.RUnlock()
	return sys.arnRemotesMap[arn]
}

// GetRemoteTargetWithLabel returns bucket target given a target label
func (sys *BucketTargetSys) GetRemoteTargetWithLabel(ctx context.Context, bucket, targetLabel string) *madmin.BucketTarget {
	sys.RLock()
	defer sys.RUnlock()
	for _, t := range sys.targetsMap[bucket] {
		if strings.ToUpper(t.Label) == strings.ToUpper(targetLabel) {
			tgt := t.Clone()
			return &tgt
		}
	}
	return nil
}

// GetRemoteArnWithLabel returns bucket target's ARN given its target label
func (sys *BucketTargetSys) GetRemoteArnWithLabel(ctx context.Context, bucket, tgtLabel string) *madmin.ARN {
	tgt := sys.GetRemoteTargetWithLabel(ctx, bucket, tgtLabel)
	if tgt == nil {
		return nil
	}
	arn, err := madmin.ParseARN(tgt.Arn)
	if err != nil {
		return nil
	}
	return arn
}

// GetRemoteLabelWithArn returns a bucket target's label given its ARN
func (sys *BucketTargetSys) GetRemoteLabelWithArn(ctx context.Context, bucket, arnStr string) string {
	sys.RLock()
	defer sys.RUnlock()
	for _, t := range sys.targetsMap[bucket] {
		if t.Arn == arnStr {
			return t.Label
		}
	}
	return ""
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
	}
	sys.targetsMap[bucket] = tgts.Targets
}

// create minio-go clients for buckets having remote targets
func (sys *BucketTargetSys) load(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	for _, bucket := range buckets {
		cfg, err := globalBucketMetadataSys.GetBucketTargetsConfig(bucket.Name)
		if err != nil {
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
				continue
			}
			sys.arnRemotesMap[tgt.Arn] = tgtClient
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
		getRemoteTargetInstanceTransport = newGatewayHTTPTransport(10 * time.Minute)
	})
	api, err := minio.New(tcfg.Endpoint, &miniogo.Options{
		Creds:     creds,
		Secure:    tcfg.Secure,
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
		bucket:              tcfg.TargetBucket,
		replicateSync:       tcfg.ReplicationSync,
	}
	go tc.healthCheck()
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
	if !madmin.ServiceType(target.Type).IsValid() {
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
	if len(cmetadata) != 0 {
		if err := json.Unmarshal(cmetadata, &meta); err != nil {
			return nil, err
		}
		if crypto.S3.IsEncrypted(meta) {
			if data, err = decryptBucketMetadata(cdata, bucket, meta, crypto.Context{bucket: bucket, bucketTargetsFile: bucketTargetsFile}); err != nil {
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
	up                  int32
	healthCheckDuration time.Duration
	bucket              string // remote bucket target
	replicateSync       bool
}

func (tc *TargetClient) isOffline() bool {
	return atomic.LoadInt32(&tc.up) == 0
}

func (tc *TargetClient) healthCheck() {
	for {
		_, err := tc.BucketExists(GlobalContext, tc.bucket)
		if err != nil {
			atomic.StoreInt32(&tc.up, 0)
			time.Sleep(tc.healthCheckDuration * time.Second)
			continue
		}
		atomic.StoreInt32(&tc.up, 1)
		time.Sleep(tc.healthCheckDuration * time.Second)
	}
}
