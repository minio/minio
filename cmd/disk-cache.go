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

func (c diskCache) Commit(f *os.File, objInfo ObjectInfo, anon bool) error {
	encPath := c.encodedPath(objInfo.Bucket, objInfo.Name)
	if err := os.Rename(f.Name(), encPath); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	metaPath := c.encodedMetaPath(objInfo.Bucket, objInfo.Name)
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

func (o diskCache) NoCommit(f *os.File) error {
	tmpName := f.Name()
	if err := f.Close(); err != nil {
		return err
	}

	return os.Remove(tmpName)
}

func (c diskCache) Put() (*os.File, error) {
	return ioutil.TempFile(c.tmpDir, "")
}

func (c diskCache) Get(bucket, object string) (*os.File, ObjectInfo, bool, error) {
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
	return file, objMeta.toObjectInfo(), objMeta.Anonymous, nil
}

func (c diskCache) GetObjectInfo(bucket, object string) (ObjectInfo, bool, error) {
	metaPath := c.encodedMetaPath(bucket, object)
	objMeta := diskCacheObjectMeta{}
	metaBytes, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := json.Unmarshal(metaBytes, &objMeta); err != nil {
		return ObjectInfo{}, false, err
	}

	return objMeta.toObjectInfo(), objMeta.Anonymous, nil
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
		c.dcache.NoCommit(cachedObj)
		return err
	}
	objInfo, err = c.GetObjectInfoFn(bucket, object)
	if err != nil {
		c.dcache.NoCommit(cachedObj)
		return err
	}

	// FIXME: race-condition: what if the server object got replaced.
	return c.dcache.Commit(cachedObj, objInfo, false)
}

func (c CacheObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	objInfo, err := c.GetObjectInfoFn(bucket, object)
	if err != nil {
		if _, ok := errorCause(err).(BackendDown); !ok {
			c.dcache.Delete(bucket, object)
			return ObjectInfo{}, err
		}
		cachedObjInfo, _, err := c.dcache.GetObjectInfo(bucket, object)
		if err == nil {
			return cachedObjInfo, nil
		}
		return ObjectInfo{}, BackendDown{}
	}
	cachedObjInfo, _, err := c.dcache.GetObjectInfo(bucket, object)
	if err != nil {
		return objInfo, nil
	}

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
	if startOffset != 0 || length != objInfo.Size {
		return c.AnonGetObjectFn(bucket, object, startOffset, length, writer)
	}
	cachedObj, err := c.dcache.Put()
	if err != nil {
		return err
	}
	err = c.AnonGetObjectFn(bucket, object, 0, objInfo.Size, io.MultiWriter(writer, cachedObj))
	if err != nil {
		c.dcache.NoCommit(cachedObj)
		return err
	}
	objInfo, err = c.AnonGetObjectInfoFn(bucket, object)
	if err != nil {
		c.dcache.NoCommit(cachedObj)
		return err
	}
	// FIXME: race-condition: what if the server object got replaced.
	return c.dcache.Commit(cachedObj, objInfo, true)
}

func (c CacheObjects) AnonGetObjectInfo(bucket, object string) (ObjectInfo, error) {
	objInfo, err := c.AnonGetObjectInfoFn(bucket, object)
	if err != nil {
		if _, ok := errorCause(err).(BackendDown); !ok {
			return ObjectInfo{}, err
		}
		cachedObjInfo, anon, err := c.dcache.GetObjectInfo(bucket, object)
		if err == nil && anon {
			return cachedObjInfo, nil
		}
		return ObjectInfo{}, BackendDown{}
	}
	cachedObjInfo, _, err := c.dcache.GetObjectInfo(bucket, object)
	if err != nil {
		return objInfo, nil
	}
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
