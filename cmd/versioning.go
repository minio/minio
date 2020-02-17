package cmd

import (
	"bytes"
	"context"
	"encoding/xml"
	"path"
	"sync"

	"github.com/minio/minio/pkg/bucket/versioning"
)

// BucketVersioningSys - Bucket versioning subsystem.
type BucketVersioningSys struct {
	sync.RWMutex
	bucketVersioningMap map[string]versioning.Versioning
}

// Set - sets versioning config to given bucket name.
func (sys *BucketVersioningSys) Set(bucketName string, versioning versioning.Versioning) {
	if globalIsGateway {
		// no-op
		return
	}

	sys.Lock()
	defer sys.Unlock()

	sys.bucketVersioningMap[bucketName] = versioning
}

// Get - gets versioning config associated to a given bucket name.
func (sys *BucketVersioningSys) Get(bucketName string) (lc versioning.Versioning, ok bool) {
	if globalIsGateway {
		// When gateway is enabled, no cached value
		// is used to validate versioning.
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return
		}

		l, err := objAPI.GetBucketVersioning(context.Background(), bucketName)
		if err != nil {
			return
		}
		return *l, true
	}

	sys.RLock()
	defer sys.RUnlock()
	lc, ok = sys.bucketVersioningMap[bucketName]
	return
}

func saveVersioningConfig(ctx context.Context, objAPI ObjectLayer, bucketName string, v *versioning.Versioning) error {
	data, err := xml.Marshal(v)
	if err != nil {
		return err
	}

	// Construct path to versioning.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketVersioningConfig)
	return saveConfig(ctx, objAPI, configFile, data)
}

// getVersioningConfig - get versioning config for given bucket name.
func getVersioningConfig(objAPI ObjectLayer, bucketName string) (*versioning.Versioning, error) {
	// Construct path to versioning.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketVersioningConfig)
	configData, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			return &versioning.Versioning{}, nil
		}
		return nil, err
	}

	return versioning.ParseConfig(bytes.NewReader(configData))
}

// NewBucketVersioningSys - creates new versioning system.
func NewBucketVersioningSys() *BucketVersioningSys {
	return &BucketVersioningSys{
		bucketVersioningMap: make(map[string]versioning.Versioning),
	}
}

// Init - initializes versioning system from versioning.xml of all buckets.
func (sys *BucketVersioningSys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, we always fetch the bucket versioning configuration
	// from the gateway backend. So, this is a no-op for gateway servers.
	if globalIsGateway {
		return nil
	}

	// Load BucketVersioningSys once during boot.
	return sys.load(buckets, objAPI)
}

// Loads lifecycle policies for all buckets into BucketVersioningSys.
func (sys *BucketVersioningSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketVersioning(context.Background(), bucket.Name)
		if err != nil {
			continue
		}

		sys.Set(bucket.Name, *config)
	}

	return nil
}
