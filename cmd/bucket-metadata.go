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

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
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

	PolicyConfigUpdatedAt            time.Time
	ObjectLockConfigUpdatedAt        time.Time
	EncryptionConfigUpdatedAt        time.Time
	TaggingConfigUpdatedAt           time.Time
	QuotaConfigUpdatedAt             time.Time
	ReplicationConfigUpdatedAt       time.Time
	VersioningConfigUpdatedAt        time.Time
	LifecycleConfigUpdatedAt         time.Time
	NotificationConfigUpdatedAt      time.Time
	BucketTargetsConfigUpdatedAt     time.Time
	BucketTargetsConfigMetaUpdatedAt time.Time
	// Add a new UpdatedAt field and update lastUpdate function

	// Unexported fields. Must be updated atomically.
	policyConfig           *policy.BucketPolicy
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
		Name: name,
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

// Return the last update of this bucket metadata, which
// means, the last update of any policy document.
func (b BucketMetadata) lastUpdate() (t time.Time) {
	if b.PolicyConfigUpdatedAt.After(t) {
		t = b.PolicyConfigUpdatedAt
	}
	if b.ObjectLockConfigUpdatedAt.After(t) {
		t = b.ObjectLockConfigUpdatedAt
	}
	if b.EncryptionConfigUpdatedAt.After(t) {
		t = b.EncryptionConfigUpdatedAt
	}
	if b.TaggingConfigUpdatedAt.After(t) {
		t = b.TaggingConfigUpdatedAt
	}
	if b.QuotaConfigUpdatedAt.After(t) {
		t = b.QuotaConfigUpdatedAt
	}
	if b.ReplicationConfigUpdatedAt.After(t) {
		t = b.ReplicationConfigUpdatedAt
	}
	if b.VersioningConfigUpdatedAt.After(t) {
		t = b.VersioningConfigUpdatedAt
	}
	if b.LifecycleConfigUpdatedAt.After(t) {
		t = b.LifecycleConfigUpdatedAt
	}
	if b.NotificationConfigUpdatedAt.After(t) {
		t = b.NotificationConfigUpdatedAt
	}
	if b.BucketTargetsConfigUpdatedAt.After(t) {
		t = b.BucketTargetsConfigUpdatedAt
	}
	if b.BucketTargetsConfigMetaUpdatedAt.After(t) {
		t = b.BucketTargetsConfigMetaUpdatedAt
	}

	return t
}

// Versioning returns true if versioning is enabled
func (b BucketMetadata) Versioning() bool {
	return b.LockEnabled || (b.versioningConfig != nil && b.versioningConfig.Enabled()) || (b.objectLockConfig != nil && b.objectLockConfig.Enabled())
}

// ObjectLocking returns true if object locking is enabled
func (b BucketMetadata) ObjectLocking() bool {
	return b.LockEnabled || (b.objectLockConfig != nil && b.objectLockConfig.Enabled())
}

// SetCreatedAt preserves the CreatedAt time for bucket across sites in site replication. It defaults to
// creation time of bucket on this cluster in all other cases.
func (b *BucketMetadata) SetCreatedAt(createdAt time.Time) {
	if b.Created.IsZero() {
		b.Created = UTCNow()
	}
	if !createdAt.IsZero() {
		b.Created = createdAt.UTC()
	}
}

// Load - loads the metadata of bucket by name from ObjectLayer api.
// If an error is returned the returned metadata will be default initialized.
func readBucketMetadata(ctx context.Context, api ObjectLayer, name string) (BucketMetadata, error) {
	if name == "" {
		internalLogIf(ctx, errors.New("bucket name cannot be empty"), logger.WarningKind)
		return BucketMetadata{}, errInvalidArgument
	}
	b := newBucketMetadata(name)
	configFile := path.Join(bucketMetaPrefix, name, bucketMetadataFile)
	data, err := readConfig(ctx, api, configFile)
	if err != nil {
		return b, err
	}
	if len(data) <= 4 {
		return b, fmt.Errorf("loadBucketMetadata: no data")
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case bucketMetadataFormat:
	default:
		return b, fmt.Errorf("loadBucketMetadata: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case bucketMetadataVersion:
	default:
		return b, fmt.Errorf("loadBucketMetadata: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}
	_, err = b.UnmarshalMsg(data[4:])
	return b, err
}

func loadBucketMetadataParse(ctx context.Context, objectAPI ObjectLayer, bucket string, parse bool) (BucketMetadata, error) {
	b, err := readBucketMetadata(ctx, objectAPI, bucket)
	b.Name = bucket // in-case parsing failed for some reason, make sure bucket name is not empty.
	if err != nil && !errors.Is(err, errConfigNotFound) {
		return b, err
	}
	if err == nil {
		b.defaultTimestamps()
	}

	// If bucket metadata is missing look for legacy files,
	// since we only ever had b.Created as non-zero when
	// migration was complete in 2020-May release. So this
	// a check to avoid migrating for buckets that already
	// have this field set.
	if b.Created.IsZero() {
		configs, err := b.getAllLegacyConfigs(ctx, objectAPI)
		if err != nil {
			return b, err
		}

		if len(configs) > 0 {
			// Old bucket without bucket metadata. Hence we migrate existing settings.
			if err = b.convertLegacyConfigs(ctx, objectAPI, configs); err != nil {
				return b, err
			}
		}
	}

	if parse {
		// nothing to update, parse and proceed.
		if err = b.parseAllConfigs(ctx, objectAPI); err != nil {
			return b, err
		}
	}

	// migrate unencrypted remote targets
	if err = b.migrateTargetConfig(ctx, objectAPI); err != nil {
		return b, err
	}

	return b, nil
}

// loadBucketMetadata loads and migrates to bucket metadata.
func loadBucketMetadata(ctx context.Context, objectAPI ObjectLayer, bucket string) (BucketMetadata, error) {
	return loadBucketMetadataParse(ctx, objectAPI, bucket, true)
}

// parseAllConfigs will parse all configs and populate the private fields.
// The first error encountered is returned.
func (b *BucketMetadata) parseAllConfigs(ctx context.Context, objectAPI ObjectLayer) (err error) {
	if len(b.PolicyConfigJSON) != 0 {
		b.policyConfig, err = policy.ParseBucketPolicyConfig(bytes.NewReader(b.PolicyConfigJSON), b.Name)
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

func (b *BucketMetadata) getAllLegacyConfigs(ctx context.Context, objectAPI ObjectLayer) (map[string][]byte, error) {
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

	configs := make(map[string][]byte, len(legacyConfigs))

	// Handle migration from lockEnabled to newer format.
	if b.LockEnabled {
		configs[objectLockConfig] = enabledBucketObjectLockConfig
		b.LockEnabled = false // legacy value unset it
		// we are only interested in b.ObjectLockConfigXML or objectLockConfig value
	}

	for _, legacyFile := range legacyConfigs {
		configFile := path.Join(bucketMetaPrefix, b.Name, legacyFile)

		configData, info, err := readConfigWithMetadata(ctx, objectAPI, configFile, ObjectOptions{})
		if err != nil {
			if _, ok := err.(ObjectExistsAsDirectory); ok {
				// in FS mode it possible that we have actual
				// files in this folder with `.minio.sys/buckets/bucket/configFile`
				continue
			}
			if errors.Is(err, errConfigNotFound) {
				// legacy file config not found, proceed to look for new metadata.
				continue
			}

			return nil, err
		}
		configs[legacyFile] = configData
		b.Created = info.ModTime
	}

	return configs, nil
}

func (b *BucketMetadata) convertLegacyConfigs(ctx context.Context, objectAPI ObjectLayer, configs map[string][]byte) error {
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
	b.defaultTimestamps()

	if err := b.Save(ctx, objectAPI); err != nil {
		return err
	}

	for legacyFile := range configs {
		configFile := path.Join(bucketMetaPrefix, b.Name, legacyFile)
		if err := deleteConfig(ctx, objectAPI, configFile); err != nil && !errors.Is(err, errConfigNotFound) {
			internalLogIf(ctx, err, logger.WarningKind)
		}
	}

	return nil
}

// default timestamps to metadata Created timestamp if unset.
func (b *BucketMetadata) defaultTimestamps() {
	if b.PolicyConfigUpdatedAt.IsZero() {
		b.PolicyConfigUpdatedAt = b.Created
	}

	if b.EncryptionConfigUpdatedAt.IsZero() {
		b.EncryptionConfigUpdatedAt = b.Created
	}

	if b.TaggingConfigUpdatedAt.IsZero() {
		b.TaggingConfigUpdatedAt = b.Created
	}

	if b.ObjectLockConfigUpdatedAt.IsZero() {
		b.ObjectLockConfigUpdatedAt = b.Created
	}

	if b.QuotaConfigUpdatedAt.IsZero() {
		b.QuotaConfigUpdatedAt = b.Created
	}

	if b.ReplicationConfigUpdatedAt.IsZero() {
		b.ReplicationConfigUpdatedAt = b.Created
	}

	if b.VersioningConfigUpdatedAt.IsZero() {
		b.VersioningConfigUpdatedAt = b.Created
	}

	if b.LifecycleConfigUpdatedAt.IsZero() {
		b.LifecycleConfigUpdatedAt = b.Created
	}

	if b.NotificationConfigUpdatedAt.IsZero() {
		b.NotificationConfigUpdatedAt = b.Created
	}

	if b.BucketTargetsConfigUpdatedAt.IsZero() {
		b.BucketTargetsConfigUpdatedAt = b.Created
	}

	if b.BucketTargetsConfigMetaUpdatedAt.IsZero() {
		b.BucketTargetsConfigMetaUpdatedAt = b.Created
	}
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

	configFile := path.Join(bucketMetaPrefix, b.Name, bucketMetadataFile)
	return saveConfig(ctx, api, configFile, data)
}

// migrate config for remote targets by encrypting data if currently unencrypted and kms is configured.
func (b *BucketMetadata) migrateTargetConfig(ctx context.Context, objectAPI ObjectLayer) error {
	var err error
	// early return if no targets or already encrypted
	if len(b.BucketTargetsConfigJSON) == 0 || GlobalKMS == nil || len(b.BucketTargetsConfigMetaJSON) != 0 {
		return nil
	}

	encBytes, metaBytes, err := encryptBucketMetadata(ctx, b.Name, b.BucketTargetsConfigJSON, kms.Context{b.Name: b.Name, bucketTargetsFile: bucketTargetsFile})
	if err != nil {
		return err
	}

	b.BucketTargetsConfigJSON = encBytes
	b.BucketTargetsConfigMetaJSON = metaBytes
	return b.Save(ctx, objectAPI)
}

// encrypt bucket metadata if kms is configured.
func encryptBucketMetadata(ctx context.Context, bucket string, input []byte, kmsContext kms.Context) (output, metabytes []byte, err error) {
	if GlobalKMS == nil {
		output = input
		return output, metabytes, err
	}

	metadata := make(map[string]string)
	key, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{AssociatedData: kmsContext})
	if err != nil {
		return output, metabytes, err
	}

	outbuf := bytes.NewBuffer(nil)
	objectKey := crypto.GenerateKey(key.Plaintext, rand.Reader)
	sealedKey := objectKey.Seal(key.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, "")
	crypto.S3.CreateMetadata(metadata, key.KeyID, key.Ciphertext, sealedKey)
	_, err = sio.Encrypt(outbuf, bytes.NewBuffer(input), sio.Config{Key: objectKey[:], MinVersion: sio.Version20})
	if err != nil {
		return output, metabytes, err
	}
	metabytes, err = json.Marshal(metadata)
	if err != nil {
		return output, metabytes, err
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
	extKey, err := GlobalKMS.Decrypt(context.TODO(), &kms.DecryptRequest{
		Name:           keyID,
		Ciphertext:     kmsKey,
		AssociatedData: kmsContext,
	})
	if err != nil {
		return nil, err
	}
	var objectKey crypto.ObjectKey
	if err = objectKey.Unseal(extKey, sealedKey, crypto.S3.String(), bucket, ""); err != nil {
		return nil, err
	}

	outbuf := bytes.NewBuffer(nil)
	_, err = sio.Decrypt(outbuf, bytes.NewBuffer(input), sio.Config{Key: objectKey[:], MinVersion: sio.Version20})
	return outbuf.Bytes(), err
}
