/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

//go:generate msgp -file $GOFILE

var (
	errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")
	errWarmBackendInUse      = errors.New("Backend warm tier already in use")
	errTierTypeUnsupported   = errors.New("Unsupported tier type")
)

const (
	tierConfigFile    = "tier-config.bin"
	tierConfigFormat  = 1
	tierConfigVersion = 1
)

// tierConfigPath refers to remote tier config object name
var tierConfigPath string = path.Join(minioConfigPrefix, tierConfigFile)

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
func (config *TierConfigMgr) Add(tier madmin.TierConfig) error {
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

	d, err := newWarmBackend(context.TODO(), tier)
	if err != nil {
		return err
	}
	// Check if warmbackend is in use by other MinIO tenants
	inUse, err := d.InUse(context.TODO())
	if err != nil {
		return err
	}
	if inUse {
		return errWarmBackendInUse
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
func (config *TierConfigMgr) Edit(tierName string, creds madmin.TierCreds) error {
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
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg.S3.AccessKey = creds.AccessKey
		newCfg.S3.SecretKey = creds.SecretKey

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

	d, err := newWarmBackend(context.TODO(), newCfg)
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
	sseS3 := true
	var extKey [32]byte
	encBr, oek, err := newEncryptReader(hr, extKey[:], minioMetaBucket, tierConfigPath, metadata, sseS3)
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
func (config *TierConfigMgr) Reload() error {
	newConfig, err := loadTransitionTierConfig()
	if err != nil {
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

func saveGlobalTierConfig() error {
	pr, opts, err := globalTierConfigMgr.configReader()
	if err != nil {
		return err
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	_, err = objAPI.PutObject(context.Background(), minioMetaBucket, tierConfigPath, pr, *opts)
	return err
}

func newTierConfigMgr() *TierConfigMgr {
	return &TierConfigMgr{
		drivercache: make(map[string]WarmBackend),
		Tiers:       make(map[string]madmin.TierConfig),
	}
}

func loadTransitionTierConfig() (*TierConfigMgr, error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return nil, errServerNotInitialized
	}

	data, err := readConfig(context.Background(), objAPI, tierConfigPath)
	switch err {
	case nil:
		break
	case errConfigNotFound:
		return newTierConfigMgr(), nil
	default:
		return nil, err
	}

	if len(data) <= 4 {
		return nil, fmt.Errorf("loadTierConfig: no data")
	}

	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case tierConfigFormat:
	default:
		return nil, fmt.Errorf("loadTierConfig: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case tierConfigVersion:
	default:
		return nil, fmt.Errorf("loadTierConfig: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	config := newTierConfigMgr()
	_, decErr := config.UnmarshalMsg(data[4:])
	if decErr != nil {
		return nil, err
	}

	return config, nil
}
