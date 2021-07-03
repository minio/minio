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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/kms"
)

//go:generate msgp -file $GOFILE

var (
	errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")
	errTierBackendInUse      = errors.New("remote tier backend already in use")
	errTierTypeUnsupported   = errors.New("unsupported tier type")
)

const (
	tierConfigFile    = "tier-config.bin"
	tierConfigFormat  = 1
	tierConfigVersion = 1
)

// tierConfigPath refers to remote tier config object name
var tierConfigPath = path.Join(minioConfigPrefix, tierConfigFile)

// TierConfigMgr holds the collection of remote tiers configured in this deployment.
type TierConfigMgr struct {
	sync.RWMutex `msg:"-"`
	drivercache  map[string]WarmBackend `msg:"-"`

	Tiers map[string]madmin.TierConfig `json:"tiers"`
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
func (config *TierConfigMgr) Add(ctx context.Context, tier madmin.TierConfig) error {
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

	d, err := newWarmBackend(ctx, tier)
	if err != nil {
		return err
	}
	// Check if warmbackend is in use by other MinIO tenants
	inUse, err := d.InUse(ctx)
	if err != nil {
		return err
	}
	if inUse {
		return errTierBackendInUse
	}

	config.Tiers[tierName] = tier
	config.drivercache[tierName] = d

	return nil
}

// ListTiers lists remote tiers configured in this deployment.
func (config *TierConfigMgr) ListTiers() []madmin.TierConfig {
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

	newCfg := config.Tiers[tierName]
	switch tierType {
	case madmin.S3:
		if (creds.AccessKey == "" || creds.SecretKey == "") && !creds.AWSRole {
			return errTierInsufficientCreds
		}
		switch {
		case creds.AWSRole:
			newCfg.S3.AWSRole = true
		default:
			newCfg.S3.AccessKey = creds.AccessKey
			newCfg.S3.SecretKey = creds.SecretKey
		}
	case madmin.Azure:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg.Azure.AccountName = creds.AccessKey
		newCfg.Azure.AccountKey = creds.SecretKey

	case madmin.GCS:
		if creds.CredsJSON == nil {
			return errTierInsufficientCreds
		}
		newCfg.GCS.Creds = base64.URLEncoding.EncodeToString(creds.CredsJSON)
	}

	d, err := newWarmBackend(ctx, newCfg)
	if err != nil {
		return err
	}
	config.Tiers[tierName] = newCfg
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
func (config *TierConfigMgr) getDriver(tierName string) (d WarmBackend, err error) {
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
	d, err = newWarmBackend(context.TODO(), t)
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
func (config *TierConfigMgr) configReader() (*PutObjReader, *ObjectOptions, error) {
	b, err := config.Bytes()
	if err != nil {
		return nil, nil, err
	}

	payloadSize := int64(len(b))
	br := bytes.NewReader(b)
	hr, err := hash.NewReader(br, payloadSize, "", "", payloadSize)
	if err != nil {
		return nil, nil, err
	}
	if GlobalKMS == nil {
		return NewPutObjReader(hr), &ObjectOptions{}, nil
	}

	// Note: Local variables with names ek, oek, etc are named inline with
	// acronyms defined here -
	// https://github.com/minio/minio/blob/master/docs/security/README.md#acronyms

	// Encrypt json encoded tier configurations
	metadata := make(map[string]string)
	encBr, oek, err := newEncryptReader(hr, crypto.S3, "", nil, minioMetaBucket, tierConfigPath, metadata, kms.Context{})
	if err != nil {
		return nil, nil, err
	}

	info := ObjectInfo{
		Size: payloadSize,
	}
	encSize := info.EncryptedSize()
	encHr, err := hash.NewReader(encBr, encSize, "", "", encSize)
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
	}

	return pReader, opts, nil
}

// Reload updates config by reloading remote tier config from config store.
func (config *TierConfigMgr) Reload(ctx context.Context, objAPI ObjectLayer) error {
	newConfig, err := loadTierConfig(ctx, objAPI)
	switch err {
	case nil:
		break
	case errConfigNotFound: // nothing to reload
		return nil
	default:
		return err
	}

	config.Lock()
	defer config.Unlock()
	// Reset drivercache built using current config
	for k := range config.drivercache {
		delete(config.drivercache, k)
	}
	// Remove existing tier configs
	for k := range config.Tiers {
		delete(config.Tiers, k)
	}
	// Copy over the new tier configs
	for tier, cfg := range newConfig.Tiers {
		config.Tiers[tier] = cfg
	}

	return nil
}

// Save saves tier configuration onto objAPI
func (config *TierConfigMgr) Save(ctx context.Context, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	pr, opts, err := globalTierConfigMgr.configReader()
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
		return nil, fmt.Errorf("tierConfigInit: no data")
	}

	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case tierConfigFormat:
	default:
		return nil, fmt.Errorf("tierConfigInit: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case tierConfigVersion:
	default:
		return nil, fmt.Errorf("tierConfigInit: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	cfg := NewTierConfigMgr()
	_, decErr := cfg.UnmarshalMsg(data[4:])
	if decErr != nil {
		return nil, decErr
	}
	return cfg, nil

}

// Reset clears remote tier configured and clears tier driver cache.
func (config *TierConfigMgr) Reset() {
	config.Lock()
	for k := range config.drivercache {
		delete(config.drivercache, k)
	}
	for k := range config.Tiers {
		delete(config.Tiers, k)
	}
	config.Unlock()

}

// Init initializes tier configuration reading from objAPI
func (config *TierConfigMgr) Init(ctx context.Context, objAPI ObjectLayer) error {
	// In gateway mode, we don't support ILM tier configuration.
	if globalIsGateway {
		return nil
	}

	return config.Reload(ctx, objAPI)
}
