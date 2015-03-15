package donutmem

import (
	"bytes"
	"errors"
	"github.com/minio-io/minio/pkg/donutbox"
	"io"
	"strconv"
	"strings"
	"sync"
)

type bucket struct {
	name     string
	metadata map[string]string
	objects  map[string]*object
	lock     *sync.RWMutex
}

type object struct {
	name     string
	data     []byte
	metadata map[string]string
	lock     *sync.RWMutex
}

type donutMem struct {
	buckets map[string]*bucket
	lock    *sync.RWMutex
}

// NewDonutMem creates a new in memory donut
func NewDonutMem() donutbox.DonutBox {
	return donutMem{
		buckets: make(map[string]*bucket),
		lock:    new(sync.RWMutex),
	}
}

// system operations
func (donutMem donutMem) ListBuckets() ([]string, error) {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	buckets := make([]string, 0)
	for k, _ := range donutMem.buckets {
		buckets = append(buckets, k)
	}
	return buckets, nil
}

// bucket operations
func (donutMem donutMem) CreateBucket(b string) error {
	donutMem.lock.Lock()
	defer donutMem.lock.Unlock()
	b = strings.ToLower(b)
	if _, ok := donutMem.buckets[b]; ok {
		return errors.New("Bucket Exists")
	}
	metadata := make(map[string]string)
	metadata["name"] = b
	newBucket := bucket{
		name:     b,
		metadata: metadata,
		objects:  make(map[string]*object),
		lock:     new(sync.RWMutex),
	}
	donutMem.buckets[b] = &newBucket
	return nil
}

func (donutMem donutMem) ListObjectsInBucket(bucketKey, prefixKey string) ([]string, error) {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		objects := make([]string, 0)
		for objectKey, _ := range curBucket.objects {
			if strings.HasPrefix(objectKey, prefixKey) {
				objects = append(objects, objectKey)
			}
		}
		return objects, nil
	}
	return nil, errors.New("Bucket does not exist")
}

func (donutMem donutMem) GetBucketMetadata(bucketKey string) (map[string]string, error) {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()

	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		result := make(map[string]string)
		for k, v := range curBucket.metadata {
			result[k] = v
		}
		return result, nil
	}
	return nil, errors.New("Bucket not found")
}

func (donutMem donutMem) SetBucketMetadata(bucketKey string, metadata map[string]string) error {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.Lock()
		defer curBucket.lock.Unlock()
		newMetadata := make(map[string]string)
		for k, v := range metadata {
			newMetadata[k] = v
		}
		curBucket.metadata = newMetadata
		return nil
	}
	return errors.New("Bucket not found")
}

// object operations
func (donutMem donutMem) GetObjectWriter(bucket, key string, column uint, blockSize uint) *io.PipeWriter {
	reader, writer := io.Pipe()
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucket]; ok {
		curBucket.lock.Lock()
		defer curBucket.lock.Unlock()
		if _, ok := curBucket.objects[key]; !ok {
			// create object
			metadata := make(map[string]string)
			metadata["key"] = key
			metadata["blockSize"] = strconv.FormatInt(int64(blockSize), 10)

			newObject := object{
				name:     key,
				data:     make([]byte, 0),
				metadata: metadata,
				lock:     new(sync.RWMutex),
			}

			newObject.lock.Lock()
			curBucket.objects[key] = &newObject
			go func() {
				defer newObject.lock.Unlock()
				var objBuffer bytes.Buffer
				_, err := io.Copy(&objBuffer, reader)
				if err == nil {
					newObject.data = objBuffer.Bytes()
					writer.Close()
				} else {
					donutMem.lock.RLock()
					defer donutMem.lock.RUnlock()
					bucket, _ := donutMem.buckets[bucket]
					bucket.lock.Lock()
					defer bucket.lock.Unlock()
					delete(bucket.objects, key)
					writer.CloseWithError(err)
				}
			}()
			return writer
		}
		writer.CloseWithError(errors.New("Object exists"))
		return writer
	}
	writer.CloseWithError(errors.New("Bucket does not exist"))
	return writer
}

func (donutMem donutMem) GetObjectReader(bucket, key string, column int) (io.Reader, error) {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucket]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		if curObject, ok := curBucket.objects[key]; ok {
			curObject.lock.RLock()
			defer curObject.lock.RUnlock()
			return bytes.NewBuffer(curObject.data), nil
		}
		return nil, errors.New("Object not found")
	}
	return nil, errors.New("Bucket not found")
}

func (donutMem donutMem) SetObjectMetadata(bucketKey, objectKey string, metadata map[string]string) error {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		if curObject, ok := curBucket.objects[objectKey]; ok {
			curObject.lock.Lock()
			defer curObject.lock.Unlock()
			newMetadata := make(map[string]string)
			for k, v := range metadata {
				newMetadata[k] = v
			}
			curObject.metadata = newMetadata
			return nil
		}
		return errors.New("Object not found")
	}
	return errors.New("Bucket not found")
}

func (donutMem donutMem) GetObjectMetadata(bucketKey, objectKey string) (map[string]string, error) {
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()

	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		if curObject, ok := curBucket.objects[objectKey]; ok {
			curObject.lock.RLock()
			defer curObject.lock.RUnlock()
			result := make(map[string]string)
			for k, v := range curObject.metadata {
				result[k] = v
			}
			return result, nil
		}
		return nil, errors.New("Object not found")
	}
	return nil, errors.New("Bucket not found")
}
