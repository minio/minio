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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/sio"
)

const (
	legacyBucketObjectLockEnabledConfigFile = "object-lock-enabled.json"
	legacyBucketObjectLockEnabledConfig     = `{"x-amz-bucket-object-lock-enabled":true}`

	bucketMetadataFile    = ".metadata.bin"
	bucketMetadataFormat  = 1
	bucketMetadataVersion = 1
)

var (
	enabledBucketObjectLockConfig = []byte(`<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`)
	enabledBucketVersioningConfig = []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
)

//go:generate msgp -file $GOFILE

// BucketMetadata contains bucket metadata.
// When adding/removing fields, regenerate the marshal code using the go generate above.
// Only changing meaning of fields requires a version bump.
// bucketMetadataFormat refers to the format.
// bucketMetadataVersion can be used to track a rolling upgrade of a field.
type BucketMetadata struct {
	Name                        string
	Created                     time.Time
	LockEnabled                 bool // legacy not used anymore.
	PolicyConfigJSON            []byte
	NotificationConfigXML       []byte
	LifecycleConfigXML          []byte
	ObjectLockConfigXML         []byte
	VersioningConfigXML         []byte
	EncryptionConfigXML         []byte
	TaggingConfigXML            []byte
	QuotaConfigJSON             []byte
	ReplicationConfigXML        []byte
	BucketTargetsConfigJSON     []byte
	BucketTargetsConfigMetaJSON []byte

	// Unexported fields. Must be updated atomically.
	policyConfig           *policy.Policy
	notificationConfig     *event.Config
	lifecycleConfig        *lifecycle.Lifecycle
	objectLockConfig       *objectlock.Config
	versioningConfig       *versioning.Versioning
	sseConfig              *bucketsse.BucketSSEConfig
	taggingConfig          *tags.Tags
	quotaConfig            *madmin.BucketQuota
	replicationConfig      *replication.Config
	bucketTargetConfig     *madmin.BucketTargets
	bucketTargetConfigMeta map[string]string
}

// newBucketMetadata creates BucketMetadata with the supplied name and Created to Now.
func newBucketMetadata(name string) BucketMetadata {
	return BucketMetadata{
		Name:    name,
		Created: UTCNow(),
		notificationConfig: &event.Config{
			XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		},
		quotaConfig: &madmin.BucketQuota{},
		versioningConfig: &versioning.Versioning{
			XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		},
		bucketTargetConfig:     &madmin.BucketTargets{},
		bucketTargetConfigMeta: make(map[string]string),
	}
}

// Load - loads the metadata of bucket by name from ObjectLayer api.
// If an error is returned the returned metadata will be default initialized.
func (b *BucketMetadata) Load(ctx context.Context, api ObjectLayer, name string) error {
	if name == "" {
		logger.LogIf(ctx, errors.New("bucket name cannot be empty"))
		return errors.New("bucket name cannot be empty")
	}
	configFile := path.Join(bucketConfigPrefix, name, bucketMetadataFile)
	data, err := readConfig(ctx, api, configFile)
	if err != nil {
		return err
	}
	if len(data) <= 4 {
		return fmt.Errorf("loadBucketMetadata: no data")
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case bucketMetadataFormat:
	default:
		return fmt.Errorf("loadBucketMetadata: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case bucketMetadataVersion:
	default:
		return fmt.Errorf("loadBucketMetadata: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}
	// OK, parse data.
	_, err = b.UnmarshalMsg(data[4:])
	b.Name = name // in-case parsing failed for some reason, make sure bucket name is not empty.
	return err
}

// loadBucketMetadata loads and migrates to bucket metadata.
func loadBucketMetadata(ctx context.Context, objectAPI ObjectLayer, bucket string) (BucketMetadata, error) {
	b := newBucketMetadata(bucket)
	err := b.Load(ctx, objectAPI, b.Name)
	if err != nil && !errors.Is(err, errConfigNotFound) {
		return b, err
	}

	// Old bucket without bucket metadata. Hence we migrate existing settings.
	if err := b.convertLegacyConfigs(ctx, objectAPI); err != nil {
		return b, err
	}
	// migrate unencrypted remote targets
	return b, b.migrateTargetConfig(ctx, objectAPI)
}

// parseAllConfigs will parse all configs and populate the private fields.
// The first error encountered is returned.
func (b *BucketMetadata) parseAllConfigs(ctx context.Context, objectAPI ObjectLayer) (err error) {
	if len(b.PolicyConfigJSON) != 0 {
		b.policyConfig, err = policy.ParseConfig(bytes.NewReader(b.PolicyConfigJSON), b.Name)
		if err != nil {
			return err
		}
	} else {
		b.policyConfig = nil
	}

	if len(b.NotificationConfigXML) != 0 {
		if err = xml.Unmarshal(b.NotificationConfigXML, b.notificationConfig); err != nil {
			return err
		}
	}

	if len(b.LifecycleConfigXML) != 0 {
		b.lifecycleConfig, err = lifecycle.ParseLifecycleConfig(bytes.NewReader(b.LifecycleConfigXML))
		if err != nil {
			return err
		}
	} else {
		b.lifecycleConfig = nil
	}

	if len(b.EncryptionConfigXML) != 0 {
		b.sseConfig, err = bucketsse.ParseBucketSSEConfig(bytes.NewReader(b.EncryptionConfigXML))
		if err != nil {
			return err
		}
	} else {
		b.sseConfig = nil
	}

	if len(b.TaggingConfigXML) != 0 {
		b.taggingConfig, err = tags.ParseBucketXML(bytes.NewReader(b.TaggingConfigXML))
		if err != nil {
			return err
		}
	} else {
		b.taggingConfig = nil
	}

	if bytes.Equal(b.ObjectLockConfigXML, enabledBucketObjectLockConfig) {
		b.VersioningConfigXML = enabledBucketVersioningConfig
	}

	if len(b.ObjectLockConfigXML) != 0 {
		b.objectLockConfig, err = objectlock.ParseObjectLockConfig(bytes.NewReader(b.ObjectLockConfigXML))
		if err != nil {
			return err
		}
	} else {
		b.objectLockConfig = nil
	}

	if len(b.VersioningConfigXML) != 0 {
		b.versioningConfig, err = versioning.ParseConfig(bytes.NewReader(b.VersioningConfigXML))
		if err != nil {
			return err
		}
	}

	if len(b.QuotaConfigJSON) != 0 {
		b.quotaConfig, err = parseBucketQuota(b.Name, b.QuotaConfigJSON)
		if err != nil {
			return err
		}
	}

	if len(b.ReplicationConfigXML) != 0 {
		b.replicationConfig, err = replication.ParseConfig(bytes.NewReader(b.ReplicationConfigXML))
		if err != nil {
			return err
		}
	} else {
		b.replicationConfig = nil
	}

	if len(b.BucketTargetsConfigJSON) != 0 {
		b.bucketTargetConfig, err = parseBucketTargetConfig(b.Name, b.BucketTargetsConfigJSON, b.BucketTargetsConfigMetaJSON)
		if err != nil {
			return err
		}
	} else {
		b.bucketTargetConfig = &madmin.BucketTargets{}
	}
	return nil
}

func (b *BucketMetadata) convertLegacyConfigs(ctx context.Context, objectAPI ObjectLayer) error {
	legacyConfigs := []string{
		legacyBucketObjectLockEnabledConfigFile,
		bucketPolicyConfig,
		bucketNotificationConfig,
		bucketLifecycleConfig,
		bucketQuotaConfigFile,
		bucketSSEConfig,
		bucketTaggingConfig,
		bucketReplicationConfig,
		bucketTargetsFile,
		objectLockConfig,
	}

	configs := make(map[string][]byte)

	// Handle migration from lockEnabled to newer format.
	if b.LockEnabled {
		configs[objectLockConfig] = enabledBucketObjectLockConfig
		b.LockEnabled = false // legacy value unset it
		// we are only interested in b.ObjectLockConfigXML or objectLockConfig value
	}

	for _, legacyFile := range legacyConfigs {
		configFile := path.Join(bucketConfigPrefix, b.Name, legacyFile)

		configData, err := readConfig(ctx, objectAPI, configFile)
		if err != nil {
			switch err.(type) {
			case ObjectExistsAsDirectory:
				// in FS mode it possible that we have actual
				// files in this folder with `.minio.sys/buckets/bucket/configFile`
				continue
			}
			if errors.Is(err, errConfigNotFound) {
				// legacy file config not found, proceed to look for new metadata.
				continue
			}

			return err
		}
		configs[legacyFile] = configData
	}

	if len(configs) == 0 {
		// nothing to update, return right away.
		return b.parseAllConfigs(ctx, objectAPI)
	}

	for legacyFile, configData := range configs {
		switch legacyFile {
		case legacyBucketObjectLockEnabledConfigFile:
			if string(configData) == legacyBucketObjectLockEnabledConfig {
				b.ObjectLockConfigXML = enabledBucketObjectLockConfig
				b.VersioningConfigXML = enabledBucketVersioningConfig
				b.LockEnabled = false // legacy value unset it
				// we are only interested in b.ObjectLockConfigXML
			}
		case bucketPolicyConfig:
			b.PolicyConfigJSON = configData
		case bucketNotificationConfig:
			b.NotificationConfigXML = configData
		case bucketLifecycleConfig:
			b.LifecycleConfigXML = configData
		case bucketSSEConfig:
			b.EncryptionConfigXML = configData
		case bucketTaggingConfig:
			b.TaggingConfigXML = configData
		case objectLockConfig:
			b.ObjectLockConfigXML = configData
			b.VersioningConfigXML = enabledBucketVersioningConfig
		case bucketQuotaConfigFile:
			b.QuotaConfigJSON = configData
		case bucketReplicationConfig:
			b.ReplicationConfigXML = configData
		case bucketTargetsFile:
			b.BucketTargetsConfigJSON = configData
		}
	}

	if err := b.Save(ctx, objectAPI); err != nil {
		return err
	}

	for legacyFile := range configs {
		configFile := path.Join(bucketConfigPrefix, b.Name, legacyFile)
		if err := deleteConfig(ctx, objectAPI, configFile); err != nil && !errors.Is(err, errConfigNotFound) {
			logger.LogIf(ctx, err)
		}
	}

	return nil
}

// Save config to supplied ObjectLayer api.
func (b *BucketMetadata) Save(ctx context.Context, api ObjectLayer) error {
	if err := b.parseAllConfigs(ctx, api); err != nil {
		return err
	}

	data := make([]byte, 4, b.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], bucketMetadataFormat)
	binary.LittleEndian.PutUint16(data[2:4], bucketMetadataVersion)

	// Marshal the bucket metadata
	data, err := b.MarshalMsg(data)
	if err != nil {
		return err
	}

	configFile := path.Join(bucketConfigPrefix, b.Name, bucketMetadataFile)
	return saveConfig(ctx, api, configFile, data)
}

// deleteBucketMetadata deletes bucket metadata
// If config does not exist no error is returned.
func deleteBucketMetadata(ctx context.Context, obj objectDeleter, bucket string) error {
	metadataFiles := []string{
		dataUsageCacheName,
		bucketMetadataFile,
	}
	for _, metaFile := range metadataFiles {
		configFile := path.Join(bucketConfigPrefix, bucket, metaFile)
		if err := deleteConfig(ctx, obj, configFile); err != nil && err != errConfigNotFound {
			return err
		}
	}
	return nil
}

// migrate config for remote targets by encrypting data if currently unencrypted and kms is configured.
func (b *BucketMetadata) migrateTargetConfig(ctx context.Context, objectAPI ObjectLayer) error {
	var err error
	// early return if no targets or already encrypted
	if len(b.BucketTargetsConfigJSON) == 0 || GlobalKMS == nil || len(b.BucketTargetsConfigMetaJSON) != 0 {
		return nil
	}

	encBytes, metaBytes, err := encryptBucketMetadata(b.Name, b.BucketTargetsConfigJSON, kms.Context{b.Name: b.Name, bucketTargetsFile: bucketTargetsFile})
	if err != nil {
		return err
	}

	b.BucketTargetsConfigJSON = encBytes
	b.BucketTargetsConfigMetaJSON = metaBytes
	return b.Save(ctx, objectAPI)
}

// encrypt bucket metadata if kms is configured.
func encryptBucketMetadata(bucket string, input []byte, kmsContext kms.Context) (output, metabytes []byte, err error) {
	if GlobalKMS == nil {
		output = input
		return
	}

	metadata := make(map[string]string)
	key, err := GlobalKMS.GenerateKey("", kmsContext)
	if err != nil {
		return
	}

	outbuf := bytes.NewBuffer(nil)
	objectKey := crypto.GenerateKey(key.Plaintext, rand.Reader)
	sealedKey := objectKey.Seal(key.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, "")
	crypto.S3.CreateMetadata(metadata, key.KeyID, key.Ciphertext, sealedKey)
	_, err = sio.Encrypt(outbuf, bytes.NewBuffer(input), sio.Config{Key: objectKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
	if err != nil {
		return output, metabytes, err
	}
	metabytes, err = json.Marshal(metadata)
	if err != nil {
		return
	}
	return outbuf.Bytes(), metabytes, nil
}

// decrypt bucket metadata if kms is configured.
func decryptBucketMetadata(input []byte, bucket string, meta map[string]string, kmsContext kms.Context) ([]byte, error) {
	if GlobalKMS == nil {
		return nil, errKMSNotConfigured
	}
	keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(meta)
	if err != nil {
		return nil, err
	}
	extKey, err := GlobalKMS.DecryptKey(keyID, kmsKey, kmsContext)
	if err != nil {
		return nil, err
	}
	var objectKey crypto.ObjectKey
	if err = objectKey.Unseal(extKey, sealedKey, crypto.S3.String(), bucket, ""); err != nil {
		return nil, err
	}

	outbuf := bytes.NewBuffer(nil)
	_, err = sio.Decrypt(outbuf, bytes.NewBuffer(input), sio.Config{Key: objectKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
	return outbuf.Bytes(), err
}
