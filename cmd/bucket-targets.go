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
	"errors"
	"maps"
	"net/url"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/kms"
)

const (
	defaultHealthCheckDuration = 5 * time.Second
	// default interval for reload of all remote target endpoints
	defaultHealthCheckReloadDuration = 30 * time.Minute
)

type arnTarget struct {
	Client      *TargetClient
	lastRefresh time.Time
}

// arnErrs represents number of errors seen for a ARN and if update is in progress
// to refresh remote targets from bucket metadata.
type arnErrs struct {
	count            int64
	updateInProgress bool
	bucket           string
}

// BucketTargetSys represents bucket targets subsystem
type BucketTargetSys struct {
	sync.RWMutex
	arnRemotesMap map[string]arnTarget
	targetsMap    map[string][]madmin.BucketTarget
	hMutex        sync.RWMutex
	hc            map[string]epHealth
	hcClient      *madmin.AnonymousClient
	aMutex        sync.RWMutex
	arnErrsMap    map[string]arnErrs // map of ARN to error count of failures to get target
}

type latencyStat struct {
	lastmin lastMinuteLatency
	curr    time.Duration
	avg     time.Duration
	peak    time.Duration
	N       int64
}

func (l *latencyStat) update(d time.Duration) {
	l.lastmin.add(d)
	l.N++
	if d > l.peak {
		l.peak = d
	}
	l.curr = l.lastmin.getTotal().avg()
	l.avg = time.Duration((int64(l.avg)*(l.N-1) + int64(l.curr)) / l.N)
}

// epHealth struct represents health of a replication target endpoint.
type epHealth struct {
	Endpoint        string
	Scheme          string
	Online          bool
	lastOnline      time.Time
	lastHCAt        time.Time
	offlineDuration time.Duration
	latency         latencyStat
}

// isOffline returns current liveness result of remote target. Add endpoint to
// healthCheck map if missing and default to online status
func (sys *BucketTargetSys) isOffline(ep *url.URL) bool {
	sys.hMutex.RLock()
	defer sys.hMutex.RUnlock()
	if h, ok := sys.hc[ep.Host]; ok {
		return !h.Online
	}
	go sys.initHC(ep)
	return false
}

// markOffline sets endpoint to offline if network i/o timeout seen.
func (sys *BucketTargetSys) markOffline(ep *url.URL) {
	sys.hMutex.Lock()
	defer sys.hMutex.Unlock()
	if h, ok := sys.hc[ep.Host]; ok {
		h.Online = false
		sys.hc[ep.Host] = h
	}
}

func (sys *BucketTargetSys) initHC(ep *url.URL) {
	sys.hMutex.Lock()
	sys.hc[ep.Host] = epHealth{
		Endpoint: ep.Host,
		Scheme:   ep.Scheme,
		Online:   true,
	}
	sys.hMutex.Unlock()
}

// newHCClient initializes an anonymous client for performing health check on the remote endpoints
func newHCClient() *madmin.AnonymousClient {
	clnt, e := madmin.NewAnonymousClientNoEndpoint()
	if e != nil {
		bugLogIf(GlobalContext, errors.New("Unable to initialize health check client"))
		return nil
	}
	clnt.SetCustomTransport(globalRemoteTargetTransport)
	return clnt
}

// heartBeat performs liveness check on remote endpoints.
func (sys *BucketTargetSys) heartBeat(ctx context.Context) {
	hcTimer := time.NewTimer(defaultHealthCheckDuration)
	defer hcTimer.Stop()
	for {
		select {
		case <-hcTimer.C:
			sys.hMutex.RLock()
			eps := make([]madmin.ServerProperties, 0, len(sys.hc))
			for _, ep := range sys.hc {
				eps = append(eps, madmin.ServerProperties{Endpoint: ep.Endpoint, Scheme: ep.Scheme})
			}
			sys.hMutex.RUnlock()

			if len(eps) > 0 {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				m := make(map[string]epHealth, len(eps))
				start := time.Now()

				for result := range sys.hcClient.Alive(cctx, madmin.AliveOpts{}, eps...) {
					var lastOnline time.Time
					var offline time.Duration
					//	var deploymentID string
					sys.hMutex.RLock()
					prev, ok := sys.hc[result.Endpoint.Host]
					sys.hMutex.RUnlock()
					if ok {
						if prev.Online != result.Online || !result.Online {
							if !prev.lastHCAt.IsZero() {
								offline = time.Since(prev.lastHCAt) + prev.offlineDuration
							} else {
								offline = prev.offlineDuration
							}
						} else if result.Online {
							offline = prev.offlineDuration
						}
					}
					lastOnline = prev.lastOnline
					if result.Online {
						lastOnline = time.Now()
					}
					l := prev.latency
					l.update(time.Since(start))
					m[result.Endpoint.Host] = epHealth{
						Endpoint:        result.Endpoint.Host,
						Scheme:          result.Endpoint.Scheme,
						Online:          result.Online,
						lastOnline:      lastOnline,
						offlineDuration: offline,
						lastHCAt:        time.Now(),
						latency:         l,
					}
				}
				cancel()
				sys.hMutex.Lock()
				sys.hc = m
				sys.hMutex.Unlock()
			}
			hcTimer.Reset(defaultHealthCheckDuration)
		case <-ctx.Done():
			return
		}
	}
}

// periodically rebuild the healthCheck map from list of targets to clear
// out stale endpoints
func (sys *BucketTargetSys) reloadHealthCheckers(ctx context.Context) {
	m := make(map[string]epHealth)
	tgts := sys.ListTargets(ctx, "", "")
	sys.hMutex.Lock()
	for _, t := range tgts {
		if _, ok := m[t.Endpoint]; !ok {
			scheme := "http"
			if t.Secure {
				scheme = "https"
			}
			epHealth := epHealth{
				Online:   true,
				Endpoint: t.Endpoint,
				Scheme:   scheme,
			}
			if prev, ok := sys.hc[t.Endpoint]; ok {
				epHealth.lastOnline = prev.lastOnline
				epHealth.offlineDuration = prev.offlineDuration
				epHealth.lastHCAt = prev.lastHCAt
				epHealth.latency = prev.latency
			}
			m[t.Endpoint] = epHealth
		}
	}
	// swap out the map
	sys.hc = m
	sys.hMutex.Unlock()
}

func (sys *BucketTargetSys) healthStats() map[string]epHealth {
	sys.hMutex.RLock()
	defer sys.hMutex.RUnlock()
	m := make(map[string]epHealth, len(sys.hc))
	maps.Copy(m, sys.hc)
	return m
}

// ListTargets lists bucket targets across tenant or for individual bucket, and returns
// results filtered by arnType
func (sys *BucketTargetSys) ListTargets(ctx context.Context, bucket, arnType string) (targets []madmin.BucketTarget) {
	h := sys.healthStats()

	if bucket != "" {
		if ts, err := sys.ListBucketTargets(ctx, bucket); err == nil {
			for _, t := range ts.Targets {
				if string(t.Type) == arnType || arnType == "" {
					if hs, ok := h[t.URL().Host]; ok {
						t.TotalDowntime = hs.offlineDuration
						t.Online = hs.Online
						t.LastOnline = hs.lastOnline
						t.Latency = madmin.LatencyStat{
							Curr: hs.latency.curr,
							Avg:  hs.latency.avg,
							Max:  hs.latency.peak,
						}
					}
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
				if hs, ok := h[t.URL().Host]; ok {
					t.TotalDowntime = hs.offlineDuration
					t.Online = hs.Online
					t.LastOnline = hs.lastOnline
					t.Latency = madmin.LatencyStat{
						Curr: hs.latency.curr,
						Avg:  hs.latency.avg,
						Max:  hs.latency.peak,
					}
				}
				targets = append(targets, t.Clone())
			}
		}
	}
	return targets
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
	if !tgt.Type.IsValid() && !update {
		return BucketRemoteArnTypeInvalid{Bucket: bucket}
	}
	clnt, err := sys.getRemoteTargetClient(tgt)
	if err != nil {
		return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket, Err: err}
	}
	// validate if target credentials are ok
	exists, err := clnt.BucketExists(ctx, tgt.TargetBucket)
	if err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "NoSuchBucket":
			return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket, Err: err}
		case "AccessDenied":
			return RemoteTargetConnectionErr{Bucket: tgt.TargetBucket, AccessKey: tgt.Credentials.AccessKey, Err: err}
		}
		return RemoteTargetConnectionErr{Bucket: tgt.TargetBucket, AccessKey: tgt.Credentials.AccessKey, Err: err}
	}
	if !exists {
		return BucketRemoteTargetNotFound{Bucket: tgt.TargetBucket}
	}
	if tgt.Type == madmin.ReplicationService {
		if !globalBucketVersioningSys.Enabled(bucket) {
			return BucketReplicationSourceNotVersioned{Bucket: bucket}
		}
		vcfg, err := clnt.GetBucketVersioning(ctx, tgt.TargetBucket)
		if err != nil {
			return RemoteTargetConnectionErr{Bucket: tgt.TargetBucket, Err: err, AccessKey: tgt.Credentials.AccessKey}
		}
		if !vcfg.Enabled() {
			return BucketRemoteTargetNotVersioned{Bucket: tgt.TargetBucket}
		}
	}

	// Check if target is a MinIO server and alive
	hcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	scheme := "http"
	if tgt.Secure {
		scheme = "https"
	}
	result := <-sys.hcClient.Alive(hcCtx, madmin.AliveOpts{}, madmin.ServerProperties{
		Endpoint: tgt.Endpoint,
		Scheme:   scheme,
	})

	cancel()
	if result.Error != nil {
		return RemoteTargetConnectionErr{Bucket: tgt.TargetBucket, Err: result.Error, AccessKey: tgt.Credentials.AccessKey}
	}
	if !result.Online {
		err := errors.New("Health check timed out after 3 seconds")
		return RemoteTargetConnectionErr{Err: err}
	}

	sys.Lock()
	defer sys.Unlock()

	tgts := sys.targetsMap[bucket]
	newtgts := make([]madmin.BucketTarget, len(tgts))
	found := false
	for idx, t := range tgts {
		if t.Type == tgt.Type {
			if t.Arn == tgt.Arn {
				if !update {
					return BucketRemoteAlreadyExists{Bucket: t.TargetBucket}
				}
				newtgts[idx] = *tgt
				found = true
				continue
			}
			// fail if endpoint is already present in list of targets and not a matching ARN
			if t.Endpoint == tgt.Endpoint {
				return BucketRemoteAlreadyExists{Bucket: t.TargetBucket}
			}
		}
		newtgts[idx] = t
	}
	if !found && !update {
		newtgts = append(newtgts, *tgt)
	}

	sys.targetsMap[bucket] = newtgts
	sys.arnRemotesMap[tgt.Arn] = arnTarget{Client: clnt}
	sys.updateBandwidthLimit(bucket, tgt.Arn, tgt.BandwidthLimit)
	return nil
}

func (sys *BucketTargetSys) updateBandwidthLimit(bucket, arn string, limit int64) {
	if limit == 0 {
		globalBucketMonitor.DeleteBucketThrottle(bucket, arn)
		return
	}
	// Setup bandwidth throttling

	globalBucketMonitor.SetBandwidthLimit(bucket, arn, limit)
}

// RemoveTarget - removes a remote bucket target for this source bucket.
func (sys *BucketTargetSys) RemoveTarget(ctx context.Context, bucket, arnStr string) error {
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
		if err == nil && rcfg != nil {
			for _, tgtArn := range rcfg.FilterTargetArns(replication.ObjectOpts{OpType: replication.AllReplicationType}) {
				if err == nil && (tgtArn == arnStr || rcfg.RoleArn == arnStr) {
					sys.RLock()
					_, ok := sys.arnRemotesMap[arnStr]
					sys.RUnlock()
					if ok {
						return BucketRemoteRemoveDisallowed{Bucket: bucket}
					}
				}
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
	sys.updateBandwidthLimit(bucket, arnStr, 0)
	return nil
}

func (sys *BucketTargetSys) markRefreshInProgress(bucket, arn string) {
	sys.aMutex.Lock()
	defer sys.aMutex.Unlock()
	if v, ok := sys.arnErrsMap[arn]; !ok {
		sys.arnErrsMap[arn] = arnErrs{
			updateInProgress: true,
			count:            v.count + 1,
			bucket:           bucket,
		}
	}
}

func (sys *BucketTargetSys) markRefreshDone(bucket, arn string) {
	sys.aMutex.Lock()
	defer sys.aMutex.Unlock()
	if v, ok := sys.arnErrsMap[arn]; ok {
		sys.arnErrsMap[arn] = arnErrs{
			updateInProgress: false,
			count:            v.count,
			bucket:           bucket,
		}
	}
}

func (sys *BucketTargetSys) isReloadingTarget(bucket, arn string) bool {
	sys.aMutex.RLock()
	defer sys.aMutex.RUnlock()
	if v, ok := sys.arnErrsMap[arn]; ok {
		return v.updateInProgress
	}
	return false
}

func (sys *BucketTargetSys) incTargetErr(bucket, arn string) {
	sys.aMutex.Lock()
	defer sys.aMutex.Unlock()
	if v, ok := sys.arnErrsMap[arn]; ok {
		sys.arnErrsMap[arn] = arnErrs{
			updateInProgress: v.updateInProgress,
			count:            v.count + 1,
		}
	}
}

// GetRemoteTargetClient returns minio-go client for replication target instance
func (sys *BucketTargetSys) GetRemoteTargetClient(bucket, arn string) *TargetClient {
	sys.RLock()
	tgt := sys.arnRemotesMap[arn]
	sys.RUnlock()

	if tgt.Client != nil {
		return tgt.Client
	}
	defer func() { // lazy refresh remote targets
		if tgt.Client == nil && !sys.isReloadingTarget(bucket, arn) && (tgt.lastRefresh.Equal(timeSentinel) || tgt.lastRefresh.Before(UTCNow().Add(-5*time.Minute))) {
			tgts, err := globalBucketMetadataSys.GetBucketTargetsConfig(bucket)
			if err == nil {
				sys.markRefreshInProgress(bucket, arn)
				sys.UpdateAllTargets(bucket, tgts)
				sys.markRefreshDone(bucket, arn)
			}
		}
		sys.incTargetErr(bucket, arn)
	}()
	return nil
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
func NewBucketTargetSys(ctx context.Context) *BucketTargetSys {
	sys := &BucketTargetSys{
		arnRemotesMap: make(map[string]arnTarget),
		targetsMap:    make(map[string][]madmin.BucketTarget),
		arnErrsMap:    make(map[string]arnErrs),
		hc:            make(map[string]epHealth),
		hcClient:      newHCClient(),
	}
	// reload healthCheck endpoints map periodically to remove stale endpoints from the map.
	go func() {
		rTimer := time.NewTimer(defaultHealthCheckReloadDuration)
		defer rTimer.Stop()
		for {
			select {
			case <-rTimer.C:
				sys.reloadHealthCheckers(ctx)
				rTimer.Reset(defaultHealthCheckReloadDuration)
			case <-ctx.Done():
				return
			}
		}
	}()
	go sys.heartBeat(ctx)
	return sys
}

// UpdateAllTargets updates target to reflect metadata updates
func (sys *BucketTargetSys) UpdateAllTargets(bucket string, tgts *madmin.BucketTargets) {
	if sys == nil {
		return
	}
	sys.Lock()
	defer sys.Unlock()

	// Remove existingtarget and arn association
	if stgts, ok := sys.targetsMap[bucket]; ok {
		for _, t := range stgts {
			delete(sys.arnRemotesMap, t.Arn)
		}
		delete(sys.targetsMap, bucket)
	}

	if tgts != nil {
		for _, tgt := range tgts.Targets {
			tgtClient, err := sys.getRemoteTargetClient(&tgt)
			if err != nil {
				continue
			}
			sys.arnRemotesMap[tgt.Arn] = arnTarget{
				Client:      tgtClient,
				lastRefresh: UTCNow(),
			}
			sys.updateBandwidthLimit(bucket, tgt.Arn, tgt.BandwidthLimit)
		}

		if !tgts.Empty() {
			sys.targetsMap[bucket] = tgts.Targets
		}
	}
}

// create minio-go clients for buckets having remote targets
func (sys *BucketTargetSys) set(bucket string, meta BucketMetadata) {
	cfg := meta.bucketTargetConfig
	if cfg == nil || cfg.Empty() {
		return
	}
	sys.Lock()
	defer sys.Unlock()
	for _, tgt := range cfg.Targets {
		tgtClient, err := sys.getRemoteTargetClient(&tgt)
		if err != nil {
			replLogIf(GlobalContext, err)
			continue
		}
		sys.arnRemotesMap[tgt.Arn] = arnTarget{Client: tgtClient}
		sys.updateBandwidthLimit(bucket, tgt.Arn, tgt.BandwidthLimit)
	}
	sys.targetsMap[bucket] = cfg.Targets
}

// Returns a minio-go Client configured to access remote host described in replication target config.
func (sys *BucketTargetSys) getRemoteTargetClient(tcfg *madmin.BucketTarget) (*TargetClient, error) {
	config := tcfg.Credentials
	creds := credentials.NewStaticV4(config.AccessKey, config.SecretKey, "")

	api, err := minio.New(tcfg.Endpoint, &minio.Options{
		Creds:     creds,
		Secure:    tcfg.Secure,
		Region:    tcfg.Region,
		Transport: globalRemoteTargetTransport,
	})
	if err != nil {
		return nil, err
	}
	api.SetAppInfo("minio-replication-target", ReleaseTag+" "+tcfg.Arn)

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
		ARN:                 tcfg.Arn,
		ResetID:             tcfg.ResetID,
		Endpoint:            tcfg.Endpoint,
		Secure:              tcfg.Secure,
	}
	return tc, nil
}

// getRemoteARN gets existing ARN for an endpoint or generates a new one.
func (sys *BucketTargetSys) getRemoteARN(bucket string, target *madmin.BucketTarget, deplID string) (arn string, exists bool) {
	if target == nil {
		return arn, exists
	}
	sys.RLock()
	defer sys.RUnlock()
	tgts := sys.targetsMap[bucket]
	for _, tgt := range tgts {
		if tgt.Type == target.Type &&
			tgt.TargetBucket == target.TargetBucket &&
			target.URL().String() == tgt.URL().String() &&
			tgt.Credentials.AccessKey == target.Credentials.AccessKey {
			return tgt.Arn, true
		}
	}
	if !target.Type.IsValid() {
		return arn, exists
	}
	return generateARN(target, deplID), false
}

// getRemoteARNForPeer returns the remote target for a peer site in site replication
func (sys *BucketTargetSys) getRemoteARNForPeer(bucket string, peer madmin.PeerInfo) string {
	sys.RLock()
	defer sys.RUnlock()
	tgts := sys.targetsMap[bucket]
	for _, target := range tgts {
		ep, _ := url.Parse(peer.Endpoint)
		if target.SourceBucket == bucket &&
			target.TargetBucket == bucket &&
			target.Endpoint == ep.Host &&
			target.Secure == (ep.Scheme == "https") &&
			target.Type == madmin.ReplicationService {
			return target.Arn
		}
	}
	return ""
}

// generate ARN that is unique to this target type
func generateARN(t *madmin.BucketTarget, deplID string) string {
	uuid := deplID
	if uuid == "" {
		uuid = mustGetUUID()
	}
	arn := madmin.ARN{
		Type:   t.Type,
		ID:     uuid,
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
	json := jsoniter.ConfigCompatibleWithStandardLibrary
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
	*minio.Client
	healthCheckDuration time.Duration
	Bucket              string // remote bucket target
	replicateSync       bool
	StorageClass        string // storage class on remote
	disableProxy        bool
	ARN                 string // ARN to uniquely identify remote target
	ResetID             string
	Endpoint            string
	Secure              bool
}
