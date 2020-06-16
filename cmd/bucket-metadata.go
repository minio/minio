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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/minio/minio-go/v6/pkg/tags"
	"github.com/minio/minio/cmd/logger"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/versioning"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
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
	Name                  string
	Created               time.Time
	LockEnabled           bool // legacy not used anymore.
	PolicyConfigJSON      []byte
	NotificationConfigXML []byte
	LifecycleConfigXML    []byte
	ObjectLockConfigXML   []byte
	VersioningConfigXML   []byte
	EncryptionConfigXML   []byte
	TaggingConfigXML      []byte
	QuotaConfigJSON       []byte

	// Unexported fields. Must be updated atomically.
	policyConfig       *policy.Policy
	notificationConfig *event.Config
	lifecycleConfig    *lifecycle.Lifecycle
	objectLockConfig   *objectlock.Config
	versioningConfig   *versioning.Versioning
	sseConfig          *bucketsse.BucketSSEConfig
	taggingConfig      *tags.Tags
	quotaConfig        *madmin.BucketQuota
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
	}
}

// Load - loads the metadata of bucket by name from ObjectLayer api.
// If an error is returned the returned metadata will be default initialized.
func (b *BucketMetadata) Load(ctx context.Context, api ObjectLayer, name string) error {
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
	return err
}

// loadBucketMetadata loads and migrates to bucket metadata.
func loadBucketMetadata(ctx context.Context, objectAPI ObjectLayer, bucket string) (BucketMetadata, error) {
	b := newBucketMetadata(bucket)
	err := b.Load(ctx, objectAPI, bucket)
	if err == nil {
		return b, b.convertLegacyConfigs(ctx, objectAPI)
	}

	if err != errConfigNotFound {
		return b, err
	}

	// Old bucket without bucket metadata. Hence we migrate existing settings.
	return b, b.convertLegacyConfigs(ctx, objectAPI)
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
func deleteBucketMetadata(ctx context.Context, obj ObjectLayer, bucket string) error {
	configFile := path.Join(bucketConfigPrefix, bucket, bucketMetadataFile)
	if err := deleteConfig(ctx, obj, configFile); err != nil && err != errConfigNotFound {
		return err
	}
	return nil
}
