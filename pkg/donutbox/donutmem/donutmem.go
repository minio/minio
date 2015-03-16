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
	var buckets []string
	for k := range donutMem.buckets {
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
		objectMap := make(map[string]string)
		for objectKey := range curBucket.objects {
			objectName := strings.Split(objectKey, "#")[0]
			if strings.HasPrefix(objectName, prefixKey) {
				objectMap[objectName] = objectName
			}
		}
		var objects []string
		for k, _ := range objectMap {
			objects = append(objects, k)
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
func (donutMem donutMem) GetObjectWriter(bucketKey, objectKey string, column uint, blockSize uint) (*donutbox.NewObject, error) {
	key := getKey(bucketKey, objectKey, column)
	reader, writer := io.Pipe()
	returnObject := donutbox.CreateNewObject(writer)
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.Lock()
		defer curBucket.lock.Unlock()
		if _, ok := curBucket.objects[key]; !ok {
			newObject := object{
				name: key,
				data: make([]byte, 0),
				lock: new(sync.RWMutex),
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

					metadata := returnObject.GetMetadata()
					for k, v := range metadata {
						metadata[k] = v
					}
					metadata["key"] = objectKey
					metadata["column"] = strconv.FormatUint(uint64(column), 10)
					newObject.metadata = metadata

					return
				}

				donutMem.lock.RLock()
				defer donutMem.lock.RUnlock()
				bucket, _ := donutMem.buckets[bucketKey]
				bucket.lock.Lock()
				defer bucket.lock.Unlock()
				delete(bucket.objects, key)
				writer.CloseWithError(err)
			}()
			return returnObject, nil
		}
		writer.CloseWithError(errors.New("Object exists"))
		return nil, errors.New("Object exists")
	}
	writer.CloseWithError(errors.New("Bucket does not exist"))
	return nil, errors.New("Bucket does not exist")
}

func (donutMem donutMem) GetObjectReader(bucketKey, objectKey string, column uint) (io.Reader, error) {
	key := getKey(bucketKey, objectKey, column)
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()
	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
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

//func (donutMem donutMem) SetObjectMetadata(bucketKey, objectKey string, column uint, metadata map[string]string) error {
//	key := getKey(bucketKey, objectKey, column)
//	donutMem.lock.RLock()
//	defer donutMem.lock.RUnlock()
//	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
//		curBucket.lock.RLock()
//		defer curBucket.lock.RUnlock()
//		if curObject, ok := curBucket.objects[key]; ok {
//			curObject.lock.Lock()
//			defer curObject.lock.Unlock()
//			newMetadata := make(map[string]string)
//			for k, v := range metadata {
//				newMetadata[k] = v
//			}
//			curObject.metadata = newMetadata
//			return nil
//		}
//		return errors.New("Object not found")
//	}
//	return errors.New("Bucket not found")
//}

func (donutMem donutMem) GetObjectMetadata(bucketKey, objectKey string, column uint) (map[string]string, error) {
	key := getKey(bucketKey, objectKey, column)
	donutMem.lock.RLock()
	defer donutMem.lock.RUnlock()

	if curBucket, ok := donutMem.buckets[bucketKey]; ok {
		curBucket.lock.RLock()
		defer curBucket.lock.RUnlock()
		if curObject, ok := curBucket.objects[key]; ok {
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

func getKey(bucketKey, objectKey string, column uint) string {
	return objectKey + "#" + strconv.FormatUint(uint64(column), 10)
}
