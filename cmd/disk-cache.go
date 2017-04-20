package cmd

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"encoding/json"
)

type diskCacheObjectMeta struct {
	Version    string            `json:"version"`
	BucketName string            `json:"bucketName"`
	ObjectName string            `json:"objectName"`
	ModTime    time.Time         `json:"modTime"`
	Size       int64             `json:"size"`
	Anonymous  bool              `json:"anonymous"`
	HTTPMeta   map[string]string `json:"httpMeta"`
}

func (objectMeta diskCacheObjectMeta) toObjectInfo() ObjectInfo {
	objInfo := ObjectInfo{}
	objInfo.UserDefined = make(map[string]string)
	objInfo.Bucket = objectMeta.BucketName
	objInfo.Name = objectMeta.ObjectName

	objInfo.ModTime = objectMeta.ModTime
	objInfo.Size = objectMeta.Size
	objInfo.MD5Sum = objectMeta.HTTPMeta["md5Sum"]
	objInfo.ContentType = objectMeta.HTTPMeta["content-type"]
	objInfo.ContentEncoding = objectMeta.HTTPMeta["content-encoding"]

	for key, val := range objectMeta.HTTPMeta {
		if key == "md5Sum" {
			continue
		}
		objInfo.UserDefined[key] = val
	}

	return objInfo
}

func newDiskCacheObjectMeta(objInfo ObjectInfo, anon bool) diskCacheObjectMeta {
	objMeta := diskCacheObjectMeta{}
	objMeta.HTTPMeta = make(map[string]string)
	objMeta.BucketName = objInfo.Bucket
	objMeta.ObjectName = objInfo.Name
	objMeta.ModTime = objInfo.ModTime
	objMeta.Size = objInfo.Size

	objMeta.HTTPMeta["md5Sum"] = objInfo.MD5Sum
	objMeta.HTTPMeta["content-type"] = objInfo.ContentType
	objMeta.HTTPMeta["content-encoding"] = objInfo.ContentEncoding

	for key, val := range objInfo.UserDefined {
		objMeta.HTTPMeta[key] = val
	}

	objMeta.Anonymous = anon

	return objMeta
}

type diskCacheObject struct {
	*os.File
	cache diskCache
}

func (o diskCacheObject) Commit(objInfo ObjectInfo, anon bool) error {
	encPath := o.cache.encodedPath(objInfo.Bucket, objInfo.Name)
	if err := os.Rename(o.Name(), encPath); err != nil {
		return err
	}
	if err := o.Close(); err != nil {
		return err
	}
	metaPath := o.cache.encodedMetaPath(objInfo.Bucket, objInfo.Name)
	objMeta := newDiskCacheObjectMeta(objInfo, anon)
	metaBytes, err := json.Marshal(objMeta)
	if err != nil {
		return err
	}
	// FIXME: take care of locking.
	if err = ioutil.WriteFile(metaPath, metaBytes, 0644); err != nil {
		return err
	}
	return nil
}

func (o diskCacheObject) NoCommit() error {
	tmpName := o.Name()
	if err := o.Close(); err != nil {
		return err
	}

	return os.Remove(tmpName)
}

type diskCache struct {
	dir      string
	maxUsage int
	expiry   int
	tmpDir   string
	dataDir  string
}

func (c diskCache) encodedPath(bucket, object string) string {
	return path.Join(c.dataDir, fmt.Sprintf("%x", sha256.Sum256([]byte(path.Join(bucket, object)))))
}

func (c diskCache) encodedMetaPath(bucket, object string) string {
	return path.Join(c.dataDir, fmt.Sprintf("%x.json", sha256.Sum256([]byte(path.Join(bucket, object)))))
}

func (c diskCache) Put() (*diskCacheObject, error) {
	f, err := ioutil.TempFile(c.tmpDir, "")
	if err != nil {
		return nil, err
	}
	return &diskCacheObject{f, c}, nil
}

func (c diskCache) Get(bucket, object string) (*diskCacheObject, ObjectInfo, bool, error) {
	metaPath := c.encodedMetaPath(bucket, object)
	objMeta := diskCacheObjectMeta{}
	metaBytes, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return nil, ObjectInfo{}, false, err
	}
	if err := json.Unmarshal(metaBytes, &objMeta); err != nil {
		return nil, ObjectInfo{}, false, err
	}

	file, err := os.Open(c.encodedPath(bucket, object))
	if err != nil {
		return nil, ObjectInfo{}, false, err
	}
	return &diskCacheObject{file, c}, objMeta.toObjectInfo(), objMeta.Anonymous, nil
}

func (c diskCache) Delete(bucket, object string) error {
	if err := os.Remove(c.encodedPath(bucket, object)); err != nil {
		return err
	}
	if err := os.Remove(c.encodedMetaPath(bucket, object)); err != nil {
		return err
	}
	return nil
}

func NewDiskCache(dir string, maxUsage, expiry int) (*diskCache, error) {
	tmpDir := path.Join(dir, "tmp")
	dataDir := path.Join(dir, "data")
	if err := os.MkdirAll(tmpDir, 0766); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0766); err != nil {
		return nil, err
	}
	return &diskCache{dir, maxUsage, expiry, tmpDir, dataDir}, nil
}

type CacheObjects struct {
	dcache *diskCache

	GetObjectFn         func(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	GetObjectInfoFn     func(bucket, object string) (objInfo ObjectInfo, err error)
	AnonGetObjectFn     func(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	AnonGetObjectInfoFn func(bucket, object string) (objInfo ObjectInfo, err error)
}

func (c CacheObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	objInfo, err := c.GetObjectInfo(bucket, object)
	_, backendDown := errorCause(err).(BackendDown)
	if err != nil && !backendDown {
		c.dcache.Delete(bucket, object)
		return err
	}
	r, cachedObjInfo, _, err := c.dcache.Get(bucket, object)
	if err == nil {
		defer r.Close()
		if backendDown {
			_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
			return err
		} else {
			if cachedObjInfo.MD5Sum == objInfo.MD5Sum {
				_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
				return err
			} else {
				c.dcache.Delete(bucket, object)
			}
		}
	}
	if startOffset != 0 || length != objInfo.Size {
		return c.GetObjectFn(bucket, object, startOffset, length, writer)
	}
	cachedObj, err := c.dcache.Put()
	if err != nil {
		return err
	}
	err = c.GetObjectFn(bucket, object, 0, objInfo.Size, io.MultiWriter(writer, cachedObj))
	if err != nil {
		cachedObj.NoCommit()
		return err
	}
	objInfo, err = c.GetObjectInfoFn(bucket, object)
	if err != nil {
		cachedObj.NoCommit()
		return err
	}

	// FIXME: race-condition: what if the server object got replaced.
	return cachedObj.Commit(objInfo, false)
}

func (c CacheObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	objInfo, err := c.GetObjectInfoFn(bucket, object)
	if err != nil {
		if _, ok := errorCause(err).(BackendDown); !ok {
			c.dcache.Delete(bucket, object)
			return ObjectInfo{}, err
		}
		r, cachedObjInfo, _, err := c.dcache.Get(bucket, object)
		if err == nil {
			defer r.Close()
			return cachedObjInfo, nil
		}
		return ObjectInfo{}, BackendDown{}
	}
	r, cachedObjInfo, _, err := c.dcache.Get(bucket, object)
	if err != nil {
		return objInfo, nil
	}
	defer r.Close()
	if cachedObjInfo.MD5Sum != objInfo.MD5Sum {
		c.dcache.Delete(bucket, object)
	}
	return objInfo, nil
}

func (c CacheObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	objInfo, err := c.AnonGetObjectInfo(bucket, object)
	_, backendDown := errorCause(err).(BackendDown)
	if err != nil && !backendDown {
		// Do not delete cache entry
		return err
	}
	r, cachedObjInfo, anon, err := c.dcache.Get(bucket, object)
	if err == nil {
		defer r.Close()
		if backendDown {
			if anon {
				_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
				return err
			} else {
				return BackendDown{}
			}
		} else {
			if cachedObjInfo.MD5Sum == objInfo.MD5Sum {
				_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
				return err
			} else {
				c.dcache.Delete(bucket, object)
			}
		}
	}
	if startOffset != 0 || length != cachedObjInfo.Size {
		return c.AnonGetObjectFn(bucket, object, startOffset, length, writer)
	}
	cachedObj, err := c.dcache.Put()
	if err != nil {
		return err
	}
	err = c.AnonGetObjectFn(bucket, object, 0, cachedObjInfo.Size, io.MultiWriter(writer, cachedObj))
	if err != nil {
		cachedObj.NoCommit()
		return err
	}
	objInfo, err = c.AnonGetObjectInfoFn(bucket, object)
	if err != nil {
		cachedObj.NoCommit()
		return err
	}
	// FIXME: race-condition: what if the server object got replaced.
	return cachedObj.Commit(objInfo, true)
}

func (c CacheObjects) AnonGetObjectInfo(bucket, object string) (ObjectInfo, error) {
	objInfo, err := c.AnonGetObjectInfoFn(bucket, object)
	if err != nil {
		if _, ok := errorCause(err).(BackendDown); !ok {
			return ObjectInfo{}, err
		}
		r, cachedObjInfo, anon, err := c.dcache.Get(bucket, object)
		if err == nil && anon {
			defer r.Close()
			return cachedObjInfo, nil
		}
		return ObjectInfo{}, BackendDown{}
	}
	r, cachedObjInfo, _, err := c.dcache.Get(bucket, object)
	if err != nil {
		return objInfo, nil
	}
	defer r.Close()
	if cachedObjInfo.MD5Sum != objInfo.MD5Sum {
		c.dcache.Delete(bucket, object)
	}
	return objInfo, nil
}

func NewServerCacheObjects(l ObjectLayer, dir string, maxUsage, expiry int) (*CacheObjects, error) {
	dcache, err := NewDiskCache(dir, maxUsage, expiry)
	if err != nil {
		return nil, err
	}

	return &CacheObjects{
		dcache:          dcache,
		GetObjectFn:     l.GetObject,
		GetObjectInfoFn: l.GetObjectInfo,
		AnonGetObjectFn: func(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
			return NotImplemented{}
		},
		AnonGetObjectInfoFn: func(bucket, object string) (ObjectInfo, error) {
			return ObjectInfo{}, NotImplemented{}
		},
	}, nil
}

func NewGatewayCacheObjects(l GatewayLayer, dir string, maxUsage, expiry int) (*CacheObjects, error) {
	dcache, err := NewDiskCache(dir, maxUsage, expiry)
	if err != nil {
		return nil, err
	}

	return &CacheObjects{
		dcache:              dcache,
		GetObjectFn:         l.GetObject,
		GetObjectInfoFn:     l.GetObjectInfo,
		AnonGetObjectFn:     l.AnonGetObject,
		AnonGetObjectInfoFn: l.AnonGetObjectInfo,
	}, nil
}
