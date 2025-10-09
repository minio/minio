// Copyright (c) 2015-2024 MinIO, Inc
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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/kms"
	"github.com/prometheus/client_golang/prometheus"
)

//go:generate msgp -file $GOFILE

var (
	errTierMissingCredentials = AdminError{
		Code:       "XMinioAdminTierMissingCredentials",
		Message:    "Specified remote credentials are empty",
		StatusCode: http.StatusForbidden,
	}

	errTierBackendInUse = AdminError{
		Code:       "XMinioAdminTierBackendInUse",
		Message:    "Specified remote tier is already in use",
		StatusCode: http.StatusConflict,
	}

	errTierTypeUnsupported = AdminError{
		Code:       "XMinioAdminTierTypeUnsupported",
		Message:    "Specified tier type is unsupported",
		StatusCode: http.StatusBadRequest,
	}

	errTierBackendNotEmpty = AdminError{
		Code:       "XMinioAdminTierBackendNotEmpty",
		Message:    "Specified remote backend is not empty",
		StatusCode: http.StatusBadRequest,
	}

	errTierInvalidConfig = AdminError{
		Code:       "XMinioAdminTierInvalidConfig",
		Message:    "Unable to setup remote tier, check tier configuration",
		StatusCode: http.StatusBadRequest,
	}
)

const (
	tierConfigFile    = "tier-config.bin"
	tierConfigFormat  = 1
	tierConfigV1      = 1
	tierConfigVersion = 2
)

// tierConfigPath refers to remote tier config object name
var tierConfigPath = path.Join(minioConfigPrefix, tierConfigFile)

const tierCfgRefreshAtHdr = "X-MinIO-TierCfg-RefreshedAt"

// TierConfigMgr holds the collection of remote tiers configured in this deployment.
type TierConfigMgr struct {
	sync.RWMutex `msg:"-"`
	drivercache  map[string]WarmBackend `msg:"-"`

	Tiers           map[string]madmin.TierConfig `json:"tiers"`
	lastRefreshedAt time.Time                    `msg:"-"`
}

type tierMetrics struct {
	sync.RWMutex  // protects requestsCount only
	requestsCount map[string]struct {
		success int64
		failure int64
	}
	histogram *prometheus.HistogramVec
}

var globalTierMetrics = tierMetrics{
	requestsCount: make(map[string]struct {
		success int64
		failure int64
	}),
	histogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tier_ttlb_seconds",
		Help:    "Time taken by requests served by warm tier",
		Buckets: []float64{0.01, 0.1, 1, 2, 5, 10, 60, 5 * 60, 15 * 60, 30 * 60},
	}, []string{"tier"}),
}

func (t *tierMetrics) Observe(tier string, dur time.Duration) {
	t.histogram.With(prometheus.Labels{"tier": tier}).Observe(dur.Seconds())
}

func (t *tierMetrics) logSuccess(tier string) {
	t.Lock()
	defer t.Unlock()

	stat := t.requestsCount[tier]
	stat.success++
	t.requestsCount[tier] = stat
}

func (t *tierMetrics) logFailure(tier string) {
	t.Lock()
	defer t.Unlock()

	stat := t.requestsCount[tier]
	stat.failure++
	t.requestsCount[tier] = stat
}

var (
	// {minio_node}_{tier}_{ttlb_seconds_distribution}
	tierTTLBMD = MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: tierSubsystem,
		Name:      ttlbDistribution,
		Help:      "Distribution of time to last byte for objects downloaded from warm tier",
		Type:      gaugeMetric,
	}

	// {minio_node}_{tier}_{requests_success}
	tierRequestsSuccessMD = MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: tierSubsystem,
		Name:      tierRequestsSuccess,
		Help:      "Number of requests to download object from warm tier that were successful",
		Type:      counterMetric,
	}
	// {minio_node}_{tier}_{requests_failure}
	tierRequestsFailureMD = MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: tierSubsystem,
		Name:      tierRequestsFailure,
		Help:      "Number of requests to download object from warm tier that failed",
		Type:      counterMetric,
	}
)

func (t *tierMetrics) Report() []MetricV2 {
	metrics := getHistogramMetrics(t.histogram, tierTTLBMD, true, true)
	t.RLock()
	defer t.RUnlock()
	for tier, stat := range t.requestsCount {
		metrics = append(metrics, MetricV2{
			Description:    tierRequestsSuccessMD,
			Value:          float64(stat.success),
			VariableLabels: map[string]string{"tier": tier},
		})
		metrics = append(metrics, MetricV2{
			Description:    tierRequestsFailureMD,
			Value:          float64(stat.failure),
			VariableLabels: map[string]string{"tier": tier},
		})
	}
	return metrics
}

func (config *TierConfigMgr) refreshedAt() time.Time {
	config.RLock()
	defer config.RUnlock()
	return config.lastRefreshedAt
}

// IsTierValid returns true if there exists a remote tier by name tierName,
// otherwise returns false.
func (config *TierConfigMgr) IsTierValid(tierName string) bool {
	config.RLock()
	defer config.RUnlock()
	_, valid := config.isTierNameInUse(tierName)
	return valid
}

// isTierNameInUse returns tier type and true if there exists a remote tier by
// name tierName, otherwise returns madmin.Unsupported and false. N B this
// function is meant for internal use, where the caller is expected to take
// appropriate locks.
func (config *TierConfigMgr) isTierNameInUse(tierName string) (madmin.TierType, bool) {
	if t, ok := config.Tiers[tierName]; ok {
		return t.Type, true
	}
	return madmin.Unsupported, false
}

// Add adds tier to config if it passes all validations.
func (config *TierConfigMgr) Add(ctx context.Context, tier madmin.TierConfig, ignoreInUse bool) error {
	config.Lock()
	defer config.Unlock()

	// check if tier name is in all caps
	tierName := tier.Name
	if tierName != strings.ToUpper(tierName) {
		return errTierNameNotUppercase
	}

	// check if tier name already in use
	if _, exists := config.isTierNameInUse(tierName); exists {
		return errTierAlreadyExists
	}

	d, err := newWarmBackend(ctx, tier, true)
	if err != nil {
		return err
	}

	if !ignoreInUse {
		// Check if warmbackend is in use by other MinIO tenants
		inUse, err := d.InUse(ctx)
		if err != nil {
			return err
		}
		if inUse {
			return errTierBackendInUse
		}
	}

	config.Tiers[tierName] = tier
	config.drivercache[tierName] = d

	return nil
}

// Remove removes tier if it is empty.
func (config *TierConfigMgr) Remove(ctx context.Context, tier string, force bool) error {
	d, err := config.getDriver(ctx, tier)
	if err != nil {
		if errors.Is(err, errTierNotFound) {
			return nil
		}
		return err
	}
	if !force {
		if inuse, err := d.InUse(ctx); err != nil {
			return err
		} else if inuse {
			return errTierBackendNotEmpty
		}
	}
	config.Lock()
	delete(config.Tiers, tier)
	delete(config.drivercache, tier)
	config.Unlock()
	return nil
}

// Verify verifies if tier's config is valid by performing all supported
// operations on the corresponding warmbackend.
func (config *TierConfigMgr) Verify(ctx context.Context, tier string) error {
	d, err := config.getDriver(ctx, tier)
	if err != nil {
		return err
	}
	return checkWarmBackend(ctx, d)
}

// Empty returns if tier targets are empty
func (config *TierConfigMgr) Empty() bool {
	if config == nil {
		return true
	}
	return len(config.ListTiers()) == 0
}

// TierType returns the type of tier
func (config *TierConfigMgr) TierType(name string) string {
	config.RLock()
	defer config.RUnlock()

	cfg, ok := config.Tiers[name]
	if !ok {
		return "internal"
	}
	return cfg.Type.String()
}

// ListTiers lists remote tiers configured in this deployment.
func (config *TierConfigMgr) ListTiers() []madmin.TierConfig {
	if config == nil {
		return nil
	}

	config.RLock()
	defer config.RUnlock()

	var tierCfgs []madmin.TierConfig
	for _, tier := range config.Tiers {
		// This makes a local copy of tier config before
		// passing a reference to it.
		tier := tier.Clone()
		tierCfgs = append(tierCfgs, tier)
	}
	return tierCfgs
}

// Edit replaces the credentials of the remote tier specified by tierName with creds.
func (config *TierConfigMgr) Edit(ctx context.Context, tierName string, creds madmin.TierCreds) error {
	config.Lock()
	defer config.Unlock()

	// check if tier by this name exists
	tierType, exists := config.isTierNameInUse(tierName)
	if !exists {
		return errTierNotFound
	}

	cfg := config.Tiers[tierName]
	switch tierType {
	case madmin.S3:
		if creds.AWSRole {
			cfg.S3.AWSRole = true
		}
		if creds.AWSRoleWebIdentityTokenFile != "" && creds.AWSRoleARN != "" {
			cfg.S3.AWSRoleARN = creds.AWSRoleARN
			cfg.S3.AWSRoleWebIdentityTokenFile = creds.AWSRoleWebIdentityTokenFile
		}
		if creds.AccessKey != "" && creds.SecretKey != "" {
			cfg.S3.AccessKey = creds.AccessKey
			cfg.S3.SecretKey = creds.SecretKey
		}
	case madmin.Azure:
		if creds.SecretKey != "" {
			cfg.Azure.AccountKey = creds.SecretKey
		}
		if creds.AzSP.TenantID != "" {
			cfg.Azure.SPAuth.TenantID = creds.AzSP.TenantID
		}
		if creds.AzSP.ClientID != "" {
			cfg.Azure.SPAuth.ClientID = creds.AzSP.ClientID
		}
		if creds.AzSP.ClientSecret != "" {
			cfg.Azure.SPAuth.ClientSecret = creds.AzSP.ClientSecret
		}
	case madmin.GCS:
		if creds.CredsJSON == nil {
			return errTierMissingCredentials
		}
		cfg.GCS.Creds = base64.URLEncoding.EncodeToString(creds.CredsJSON)
	case madmin.MinIO:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierMissingCredentials
		}
		cfg.MinIO.AccessKey = creds.AccessKey
		cfg.MinIO.SecretKey = creds.SecretKey
	}

	d, err := newWarmBackend(ctx, cfg, true)
	if err != nil {
		return err
	}
	config.Tiers[tierName] = cfg
	config.drivercache[tierName] = d
	return nil
}

// Bytes returns msgpack encoded config with format and version headers.
func (config *TierConfigMgr) Bytes() ([]byte, error) {
	config.RLock()
	defer config.RUnlock()
	data := make([]byte, 4, config.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], tierConfigFormat)
	binary.LittleEndian.PutUint16(data[2:4], tierConfigVersion)

	// Marshal the tier config
	return config.MarshalMsg(data)
}

// getDriver returns a warmBackend interface object initialized with remote tier config matching tierName
func (config *TierConfigMgr) getDriver(ctx context.Context, tierName string) (d WarmBackend, err error) {
	config.Lock()
	defer config.Unlock()

	var ok bool
	// Lookup in-memory drivercache
	d, ok = config.drivercache[tierName]
	if ok {
		return d, nil
	}

	// Initialize driver from tier config matching tierName
	t, ok := config.Tiers[tierName]
	if !ok {
		return nil, errTierNotFound
	}
	d, err = newWarmBackend(ctx, t, false)
	if err != nil {
		return nil, err
	}
	config.drivercache[tierName] = d
	return d, nil
}

// configReader returns a PutObjReader and ObjectOptions needed to save config
// using a PutObject API. PutObjReader encrypts json encoded tier configurations
// if KMS is enabled, otherwise simply yields the json encoded bytes as is.
// Similarly, ObjectOptions value depends on KMS' status.
func (config *TierConfigMgr) configReader(ctx context.Context) (*PutObjReader, *ObjectOptions, error) {
	b, err := config.Bytes()
	if err != nil {
		return nil, nil, err
	}

	payloadSize := int64(len(b))
	br := bytes.NewReader(b)
	hr, err := hash.NewReader(ctx, br, payloadSize, "", "", payloadSize)
	if err != nil {
		return nil, nil, err
	}
	if GlobalKMS == nil {
		return NewPutObjReader(hr), &ObjectOptions{MaxParity: true}, nil
	}

	// Note: Local variables with names ek, oek, etc are named inline with
	// acronyms defined here -
	// https://github.com/minio/minio/blob/master/docs/security/README.md#acronyms

	// Encrypt json encoded tier configurations
	metadata := make(map[string]string)
	encBr, oek, err := newEncryptReader(context.Background(), hr, crypto.S3, "", nil, minioMetaBucket, tierConfigPath, metadata, kms.Context{})
	if err != nil {
		return nil, nil, err
	}

	info := ObjectInfo{
		Size: payloadSize,
	}
	encSize := info.EncryptedSize()
	encHr, err := hash.NewReader(ctx, encBr, encSize, "", "", encSize)
	if err != nil {
		return nil, nil, err
	}

	pReader, err := NewPutObjReader(hr).WithEncryption(encHr, &oek)
	if err != nil {
		return nil, nil, err
	}
	opts := &ObjectOptions{
		UserDefined: metadata,
		MTime:       UTCNow(),
		MaxParity:   true,
	}

	return pReader, opts, nil
}

// Reload updates config by reloading remote tier config from config store.
func (config *TierConfigMgr) Reload(ctx context.Context, objAPI ObjectLayer) error {
	newConfig, err := loadTierConfig(ctx, objAPI)

	config.Lock()
	defer config.Unlock()

	switch err {
	case nil:
		break
	case errConfigNotFound: // nothing to reload
		// To maintain the invariance that lastRefreshedAt records the
		// timestamp of last successful refresh
		config.lastRefreshedAt = UTCNow()
		return nil
	default:
		return err
	}

	// Reset drivercache built using current config
	clear(config.drivercache)
	// Remove existing tier configs
	clear(config.Tiers)
	// Copy over the new tier configs
	maps.Copy(config.Tiers, newConfig.Tiers)
	config.lastRefreshedAt = UTCNow()
	return nil
}

// Save saves tier configuration onto objAPI
func (config *TierConfigMgr) Save(ctx context.Context, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	pr, opts, err := globalTierConfigMgr.configReader(ctx)
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(ctx, minioMetaBucket, tierConfigPath, pr, *opts)
	return err
}

// NewTierConfigMgr - creates new tier configuration manager,
func NewTierConfigMgr() *TierConfigMgr {
	return &TierConfigMgr{
		drivercache: make(map[string]WarmBackend),
		Tiers:       make(map[string]madmin.TierConfig),
	}
}

func (config *TierConfigMgr) refreshTierConfig(ctx context.Context, objAPI ObjectLayer) {
	const tierCfgRefresh = 15 * time.Minute
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randInterval := func() time.Duration {
		return time.Duration(r.Float64() * 5 * float64(time.Second))
	}

	// To avoid all MinIO nodes reading the tier config object at the same
	// time.
	t := time.NewTimer(tierCfgRefresh + randInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := config.Reload(ctx, objAPI)
			if err != nil {
				tierLogIf(ctx, err)
			}
		}
		t.Reset(tierCfgRefresh + randInterval())
	}
}

// loadTierConfig loads remote tier configuration from objAPI.
func loadTierConfig(ctx context.Context, objAPI ObjectLayer) (*TierConfigMgr, error) {
	if objAPI == nil {
		return nil, errServerNotInitialized
	}

	data, err := readConfig(ctx, objAPI, tierConfigPath)
	if err != nil {
		return nil, err
	}

	if len(data) <= 4 {
		return nil, errors.New("tierConfigInit: no data")
	}

	// Read header
	switch format := binary.LittleEndian.Uint16(data[0:2]); format {
	case tierConfigFormat:
	default:
		return nil, fmt.Errorf("tierConfigInit: unknown format: %d", format)
	}

	cfg := NewTierConfigMgr()
	switch version := binary.LittleEndian.Uint16(data[2:4]); version {
	case tierConfigV1, tierConfigVersion:
		if _, decErr := cfg.UnmarshalMsg(data[4:]); decErr != nil {
			return nil, decErr
		}
	default:
		return nil, fmt.Errorf("tierConfigInit: unknown version: %d", version)
	}

	return cfg, nil
}

// Init initializes tier configuration reading from objAPI
func (config *TierConfigMgr) Init(ctx context.Context, objAPI ObjectLayer) error {
	err := config.Reload(ctx, objAPI)
	if globalIsDistErasure {
		go config.refreshTierConfig(ctx, objAPI)
	}
	return err
}
