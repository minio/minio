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
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/minio/minio/cmd/logger"
)

const (
	legacyBucketObjectLockEnabledConfigFile = "object-lock-enabled.json"
	legacyBucketObjectLockEnabledConfig     = `{"x-amz-bucket-object-lock-enabled":true}`

	bucketMetadataFile    = ".metadata.bin"
	bucketMetadataFormat  = 1
	bucketMetadataVersion = 1
)

var (
	defaultBucketObjectLockConfig = []byte(`<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`)
)

//go:generate msgp -file $GOFILE

// BucketMetadata contains bucket metadata.
// When adding/removing fields, regenerate the marshal code using the go generate above.
// Only changing meaning of fields requires a version bump.
// bucketMetadataFormat refers to the format.
// bucketMetadataVersion can be used to track a rolling upgrade of a field.
type BucketMetadata struct {
	Name                       string
	Created                    time.Time
	LockEnabled                bool // legacy not used anymore.
	PolicyConfigJSON           []byte
	NotificationXML            []byte
	LifecycleConfigXML         []byte
	ObjectLockConfigurationXML []byte
	EncryptionConfigXML        []byte
	TaggingConfigXML           []byte
	QuotaConfigJSON            []byte
}

// newBucketMetadata creates BucketMetadata with the supplied name and Created to Now.
func newBucketMetadata(name string) BucketMetadata {
	return BucketMetadata{
		Name:    name,
		Created: UTCNow(),
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

func (b *BucketMetadata) convertLegacyConfigs(ctx context.Context, objectAPI ObjectLayer) error {
	legacyConfigs := []string{
		legacyBucketObjectLockEnabledConfigFile,
		bucketPolicyConfig,
		bucketNotificationConfig,
		bucketLifecycleConfig,
		bucketQuotaConfigFile,
		bucketSSEConfig,
		bucketTaggingConfigFile,
		objectLockConfig,
	}

	configs := make(map[string][]byte)

	// Handle migration from lockEnabled to newer format.
	if b.LockEnabled {
		configs[objectLockConfig] = defaultBucketObjectLockConfig
		b.LockEnabled = false // legacy value unset it
		// we are only interested in b.ObjectLockConfigurationXML or objectLockConfig value
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
		return nil
	}

	for legacyFile, configData := range configs {
		switch legacyFile {
		case legacyBucketObjectLockEnabledConfigFile:
			if string(configData) == legacyBucketObjectLockEnabledConfig {
				b.ObjectLockConfigurationXML = defaultBucketObjectLockConfig
				b.LockEnabled = false // legacy value unset it
				// we are only interested in b.ObjectLockConfigurationXML
			}
		case bucketPolicyConfig:
			b.PolicyConfigJSON = configData
		case bucketNotificationConfig:
			b.NotificationXML = configData
		case bucketLifecycleConfig:
			b.LifecycleConfigXML = configData
		case bucketSSEConfig:
			b.EncryptionConfigXML = configData
		case bucketTaggingConfigFile:
			b.TaggingConfigXML = configData
		case objectLockConfig:
			b.ObjectLockConfigurationXML = configData
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
