package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/minio/minio/cmd/logger"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
)

const (
	getBucketVersioningResponse   = `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
	bucketObjectLockEnabledConfig = `{"x-amz-bucket-object-lock-enabled":true}`
	bucketMetadataFile            = ".metadata"
	bucketMetadataFormat          = 1
	bucketMetadataVersion         = 1

	legacyObjectLockConfig                  = "object-lock.xml"
	legacyBucketObjectLockEnabledConfigFile = "object-lock-enabled.json"
)

//go:generate msgp -file $GOFILE -unexported

// bucketMetadata contains bucket metadata.
// When adding/removing fields, regenerate the marshal code using the go generate above.
// Only changing meaning of fields requires a version bump.
// bucketMetadataFormat refers to the format.
// bucketMetadataVersion can be used to track a rolling upgrade of a field.
type bucketMetadata struct {
	Name       string
	Created    time.Time
	LockConfig []byte
}

// newBucketMetadata creates bucketMetadata with the supplied name and Created to Now.
func newBucketMetadata(name string) bucketMetadata {
	return bucketMetadata{
		Name:    name,
		Created: UTCNow(),
	}
}

var errMetaDataConverted = errors.New("metadata converted")

// loadBucketMetadata loads the metadata of bucket by name from ObjectLayer o.
// If an error is returned the returned metadata will be default initialized.
func loadBucketMetadata(ctx context.Context, o ObjectLayer, name string) (bucketMetadata, error) {
	b := newBucketMetadata(name)
	configFile := path.Join(bucketConfigPrefix, name, bucketMetadataFile)
	data, err := readConfig(ctx, o, configFile)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			// If not found, it may not have been converted.
			// Try to do so.
			b.loadLegacyObjectLock(ctx, o)
			err = b.save(ctx, o)
			if err != nil {
				return b, err
			}
			return b, errMetaDataConverted
		}
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

	// OK, parse data.
	_, err = b.UnmarshalMsg(data[4:])
	return b, err
}

// save config to supplied ObjectLayer o.
func (b *bucketMetadata) save(ctx context.Context, o ObjectLayer) error {
	data := make([]byte, 4, b.Msgsize()+4)
	// Put header
	binary.LittleEndian.PutUint16(data[0:2], bucketMetadataFormat)
	binary.LittleEndian.PutUint16(data[2:4], bucketMetadataVersion)

	// Add data
	data, err := b.MarshalMsg(data)
	if err != nil {
		return err
	}
	configFile := path.Join(bucketConfigPrefix, b.Name, bucketMetadataFile)
	return saveConfig(ctx, o, configFile, data)
}

// delete the config metadata.
// If config does not exist no error is returned.
func (b bucketMetadata) delete(ctx context.Context, o ObjectLayer) error {
	configFile := path.Join(bucketConfigPrefix, b.Name, bucketMetadataFile)
	err := deleteConfig(ctx, o, configFile)
	if err == errConfigNotFound {
		// We don't care
		err = nil
	}
	return err
}

// loadLegacyObjectLock will attempt to load legacy object lock to embed it into bucketMetadata.
func (b *bucketMetadata) loadLegacyObjectLock(ctx context.Context, o ObjectLayer) {
	configFile := path.Join(bucketConfigPrefix, b.Name, legacyBucketObjectLockEnabledConfigFile)
	bucketObjLockData, err := readConfig(ctx, o, configFile)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return
		}
		return
	}

	if string(bucketObjLockData) != bucketObjectLockEnabledConfig {
		// this should never happen
		logger.LogIf(ctx, objectlock.ErrMalformedBucketObjectConfig)
		return
	}

	configFile = path.Join(bucketConfigPrefix, b.Name, legacyObjectLockConfig)
	b.LockConfig, _ = readConfig(ctx, o, configFile)
}
