/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"io/ioutil"
	"os"
	"path"
)

type cacheLayer struct {
	GatewayLayer

	cache *BucketCache
}

type Cache struct {
	CachePath string
}

func (c *Cache) Invalidate(object string) error {
	if _, err := os.Stat(path.Join(c.CachePath, object)); err != nil {
		return err
	} else {
		return os.Remove(path.Join(c.CachePath, object))
	}
}

func (c *Cache) Retrieve(object string) (*Resource, error) {
	if r, err := os.Open(path.Join(c.CachePath, object)); err != nil {
		return nil, err
	} else {
		return &Resource{
			ReadCloser: r,
		}, nil
	}
}

func (c *Cache) Store(object string, r *Resource) error {
	defer r.Close()

	if f, err := os.Create(path.Join(c.CachePath, object)); err != nil {
		return err
	} else if _, err := io.Copy(f, r); err != nil {
		return err
	} else {
		return nil
	}
}

func NewDiskCache(path string) (*Cache, error) {
	if _, err := os.Stat(path); err == nil {
	} else if !os.IsNotExist(err) {
		return nil, err
	} else if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}

	return &Cache{
		CachePath: path,
	}, nil
}

type Resource struct {
	io.ReadCloser
}

type BucketCache struct {
	cache map[string]*Cache

	CachePath string
}

func (bc *BucketCache) Get(bucket string) (*Cache, error) {
	if _, ok := bc.cache[bucket]; !ok {
		if cache, err := NewDiskCache(path.Join(bc.CachePath, bucket)); err != nil {
			return nil, err
		} else {
			bc.cache[bucket] = cache
		}
	}

	return bc.cache[bucket], nil
}

func newCacheLayer(gl GatewayLayer, cachePath string) GatewayLayer {
	return &cacheLayer{
		GatewayLayer: gl,
		cache: &BucketCache{
			cache:     map[string]*Cache{},
			CachePath: cachePath,
		},
	}
}

func (cl *cacheLayer) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	// TODO(nl5887): add pipewriter hereo, make sure it is being written completely
	// use lock files, and meta for cache duration
	// add expiration

	// should we check for existence?
	// should we add an automatic refresher?
	// should we add expiration

	// todo(nl5887): security!!!
	// todo(nl5587): lock!
	if cache, err := cl.cache.Get(bucket); err != nil {
		return err
	} else if resource, err := cache.Retrieve(object); os.IsNotExist(err) {
		pr, pw := io.Pipe()

		go func() {
			defer pw.Close()

			// TODO(nl5887): implement error chan / handling
			if err := cl.GatewayLayer.GetObject(bucket, object, startOffset, length, pw); err != nil {
				//	return err
			}
		}()

		cache.Store(object, &Resource{
			ReadCloser: ioutil.NopCloser(
				io.TeeReader(pr, writer),
			),
		})
	} else {
		defer resource.Close()

		if _, err := io.Copy(writer, resource); err != nil {
			return err
		}
	}

	return nil
}

func (cl *cacheLayer) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return cl.GatewayLayer.GetObjectInfo(bucket, object)
}

func (cl *cacheLayer) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	if cache, err := cl.cache.Get(bucket); err == nil {
		cache.Invalidate(object)
	}
	// 	defer cl.cache.Get(bucket.Prefetch(object))
	// start prefetch

	return cl.GatewayLayer.PutObject(bucket, object, size, data, metadata, sha256sum)
}

func (cl *cacheLayer) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	if cache, err := cl.cache.Get(bucket); err == nil {
		cache.Invalidate(object)
	}

	// 	defer cl.cache.Get(bucket.Prefetch(object))
	// start prefetch

	return cl.GatewayLayer.CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
}
