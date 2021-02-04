/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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
	"encoding/json"
	"errors"
	"path"
	"sync"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

var TierConfigPath string = path.Join(minioConfigPrefix, "tier-config.json")

var errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")

type TierConfigMgr struct {
	sync.RWMutex
	drivercache map[string]warmBackend
	S3          map[string]madmin.TierS3    `json:"s3"`
	Azure       map[string]madmin.TierAzure `json:"azure"`
	GCS         map[string]madmin.TierGCS   `json:"gcs"`
}

var errWarmBackendInUse = errors.New("Backend warm tier already in use")

func (config *TierConfigMgr) isTierNameInUse(tierName string) (madmin.TierType, bool) {
	for name := range config.S3 {
		if tierName == name {
			return madmin.S3, true
		}
	}

	for name := range config.Azure {
		if tierName == name {
			return madmin.Azure, true
		}
	}

	for name := range config.GCS {
		if tierName == name {
			return madmin.GCS, true
		}
	}

	return madmin.Unsupported, false
}

func tierBackendInUse(sc madmin.TierConfig) (bool, error) {
	var d warmBackend
	var err error
	switch sc.Type {
	case madmin.S3:
		d, err = newWarmBackendS3(*sc.S3)
	case madmin.Azure:
		d, err = newWarmBackendAzure(*sc.Azure)
	case madmin.GCS:
		d, err = newWarmBackendGCS(*sc.GCS)
	default:
		return false, errors.New("Unsupported tier type")
	}
	if err != nil {
		return false, err
	}
	return d.InUse(context.Background())
}

func (config *TierConfigMgr) Add(sc madmin.TierConfig) error {
	config.Lock()
	defer config.Unlock()

	tierName := sc.Name()
	// storage-class name already in use
	if _, exists := config.isTierNameInUse(tierName); exists {
		return errTierAlreadyExists
	}

	inuse, err := tierBackendInUse(sc)
	if err != nil {
		return err
	}
	if inuse {
		return errWarmBackendInUse
	}

	switch sc.Type {
	case madmin.S3:
		config.S3[tierName] = *sc.S3

	case madmin.Azure:
		config.Azure[tierName] = *sc.Azure

	case madmin.GCS:
		config.GCS[tierName] = *sc.GCS

	default:
		return errors.New("Unsupported tier type")
	}

	return nil
}

func (config *TierConfigMgr) ListTiers() []madmin.TierConfig {
	config.RLock()
	defer config.RUnlock()

	var configs []madmin.TierConfig
	for _, t := range config.S3 {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type: madmin.S3,
			S3:   &cfg,
		})
	}

	for _, t := range config.Azure {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type:  madmin.Azure,
			Azure: &cfg,
		})
	}

	for _, t := range config.GCS {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type: madmin.GCS,
			GCS:  &cfg,
		})
	}

	return configs
}

func (config *TierConfigMgr) Edit(tierName string, creds madmin.TierCreds) error {
	config.Lock()
	defer config.Unlock()

	// check if tier by this name exists
	var (
		tierType madmin.TierType
		exists   bool
	)
	if tierType, exists = config.isTierNameInUse(tierName); !exists {
		return errTierNotFound
	}

	switch tierType {
	case madmin.S3:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		sc := config.S3[tierName]
		sc.AccessKey = creds.AccessKey
		sc.SecretKey = creds.SecretKey
		config.S3[tierName] = sc

	case madmin.Azure:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		sc := config.Azure[tierName]
		sc.AccountName = creds.AccessKey
		sc.AccountKey = creds.SecretKey
		config.Azure[tierName] = sc
	case madmin.GCS:
		if creds.CredsJSON == nil {
			return errTierInsufficientCreds
		}
		sc := config.GCS[tierName]
		sc.Creds = base64.URLEncoding.EncodeToString(creds.CredsJSON)
		config.GCS[tierName] = sc
	}

	return nil
}

func (config *TierConfigMgr) Bytes() ([]byte, error) {
	config.Lock()
	defer config.Unlock()
	return json.Marshal(config)
}

func (config *TierConfigMgr) GetDriver(tierName string) (d warmBackend, err error) {
	config.Lock()
	defer config.Unlock()

	d = config.drivercache[tierName]
	if d != nil {
		return d, nil
	}
	for k, v := range config.S3 {
		if k == tierName {
			d, err = newWarmBackendS3(v)
			break
		}
	}
	for k, v := range config.Azure {
		if k == tierName {
			d, err = newWarmBackendAzure(v)
			break
		}
	}
	for k, v := range config.GCS {
		if k == tierName {
			d, err = newWarmBackendGCS(v)
			break
		}
	}

	if err != nil {
		return nil, err
	}

	config.drivercache[tierName] = d
	return d, nil
}

func saveGlobalTierConfig() error {
	b, err := globalTierConfigMgr.Bytes()
	if err != nil {
		return err
	}
	r, err := hash.NewReader(bytes.NewReader(b), int64(len(b)), "", "", int64(len(b)), false)
	if err != nil {
		return err
	}
	_, err = globalObjectAPI.PutObject(context.Background(), minioMetaBucket, TierConfigPath, NewPutObjReader(r, nil, nil), ObjectOptions{})
	return err
}

func loadGlobalTransitionTierConfig() error {
	var buf bytes.Buffer
	err := globalObjectAPI.GetObject(context.Background(), minioMetaBucket, TierConfigPath, 0, -1, &buf, "", ObjectOptions{})
	if err != nil {
		if isErrObjectNotFound(err) {
			globalTierConfigMgr = &TierConfigMgr{
				RWMutex:     sync.RWMutex{},
				drivercache: make(map[string]warmBackend),
				S3:          make(map[string]madmin.TierS3),
				Azure:       make(map[string]madmin.TierAzure),
				GCS:         make(map[string]madmin.TierGCS),
			}
			return nil
		}
		return err
	}
	var config TierConfigMgr
	err = json.Unmarshal(buf.Bytes(), &config)
	if err != nil {
		return err
	}
	if config.drivercache == nil {
		config.drivercache = make(map[string]warmBackend)
	}
	if config.S3 == nil {
		config.S3 = make(map[string]madmin.TierS3)
	}
	if config.Azure == nil {
		config.Azure = make(map[string]madmin.TierAzure)
	}
	if config.GCS == nil {
		config.GCS = make(map[string]madmin.TierGCS)
	}

	globalTierConfigMgr = &config
	return nil
}
