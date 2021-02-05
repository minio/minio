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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/sio"
)

var TierConfigPath string = path.Join(minioConfigPrefix, "tier-config.json")

var (
	errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")
	errWarmBackendInUse      = errors.New("Backend warm tier already in use")
	errTierTypeUnsupported   = errors.New("Unsupported tier type")
)

type TierConfigMgr struct {
	sync.RWMutex
	drivercache map[string]warmBackend
	S3          map[string]madmin.TierS3    `json:"s3"`
	Azure       map[string]madmin.TierAzure `json:"azure"`
	GCS         map[string]madmin.TierGCS   `json:"gcs"`
}

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
		return false, errTierTypeUnsupported
	}
	if err != nil {
		return false, err
	}
	return d.InUse(context.Background())
}

func (config *TierConfigMgr) Add(tier madmin.TierConfig) error {
	config.Lock()
	defer config.Unlock()

	// check if tier name is in all caps
	tierName := tier.Name()
	if tierName != strings.ToUpper(tierName) {
		return errTierNameNotUppercase
	}

	// check if tier name already in use
	if _, exists := config.isTierNameInUse(tierName); exists {
		return errTierAlreadyExists
	}

	inuse, err := tierBackendInUse(tier)
	if err != nil {
		return err
	}
	if inuse {
		return errWarmBackendInUse
	}

	switch tier.Type {
	case madmin.S3:
		config.S3[tierName] = *tier.S3

	case madmin.Azure:
		config.Azure[tierName] = *tier.Azure

	case madmin.GCS:
		config.GCS[tierName] = *tier.GCS

	default:
		return errTierTypeUnsupported
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
	if d == nil {
		return nil, errTierNotFound
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
	hr, err := hash.NewReader(br, payloadSize, "", "", payloadSize, false)
	if err != nil {
		return nil, nil, err
	}
	if GlobalKMS == nil {
		return NewPutObjReader(hr, nil, nil), &ObjectOptions{}, nil
	}

	// Note: Local variables with names ek, oek, etc are named inline with
	// acronyms defined here -
	// https://github.com/minio/minio/blob/master/docs/security/README.md#acronyms

	// Encrypt json encoded tier configurations
	metadata := make(map[string]string)
	kmsContext := crypto.Context{minioMetaBucket: path.Join(minioMetaBucket, TierConfigPath)}

	ek, encEK, err := GlobalKMS.GenerateKey(GlobalKMS.DefaultKeyID(), kmsContext)
	if err != nil {
		return nil, nil, err
	}

	oek := crypto.GenerateKey(ek, rand.Reader)
	encOEK := oek.Seal(ek, crypto.GenerateIV(rand.Reader), crypto.S3.String(), minioMetaBucket, TierConfigPath)
	crypto.S3.CreateMetadata(metadata, GlobalKMS.DefaultKeyID(), encEK, encOEK)
	encBr, err := sio.EncryptReader(br, sio.Config{Key: oek[:], MinVersion: sio.Version20})
	if err != nil {
		return nil, nil, err
	}

	info := ObjectInfo{
		Size: payloadSize,
	}
	encSize := info.EncryptedSize()

	encHr, err := hash.NewReader(encBr, encSize, "", "", encSize, false)
	if err != nil {
		return nil, nil, err
	}
	opts := &ObjectOptions{
		UserDefined: metadata,
		MTime:       UTCNow(),
	}
	return NewPutObjReader(hr, encHr, &oek), opts, nil
}

func saveGlobalTierConfig() error {
	pr, opts, err := globalTierConfigMgr.configReader()
	if err != nil {
		return err
	}

	_, err = globalObjectAPI.PutObject(context.Background(), minioMetaBucket, TierConfigPath, pr, *opts)
	return err
}

func loadGlobalTransitionTierConfig() error {
	objReadCloser, err := globalObjectAPI.GetObjectNInfo(context.Background(), minioMetaBucket, TierConfigPath, nil, http.Header{}, readLock, ObjectOptions{})
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

	defer objReadCloser.Close()

	var config TierConfigMgr
	err = json.NewDecoder(objReadCloser).Decode(&config)
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
