package fs

import (
	"os"
	"path/filepath"
)

// IsPrivateBucket - is private bucket
func (fs API) IsPrivateBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPrivate()
}

// IsPublicBucket - is public bucket
func (fs API) IsPublicBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPublicReadWrite()
}

// IsReadOnlyBucket - is read only bucket
func (fs API) IsReadOnlyBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPublicRead()
}
