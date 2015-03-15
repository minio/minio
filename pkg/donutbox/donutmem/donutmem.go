package donutmem

import (
	"bytes"
	"errors"
	"github.com/minio-io/minio/pkg/donutbox"
	"io"
	"strconv"
	"strings"
)

type bucket struct {
	name     string
	metadata map[string]string
	objects  map[string]object
}

type object struct {
	name     string
	data     []byte
	metadata map[string]string
}

type donutMem struct {
	buckets map[string]bucket
}

// NewDonutMem creates a new in memory donut
func NewDonutMem() donutbox.DonutBox {
	return donutMem{
		buckets: make(map[string]bucket),
	}
}

// system operations
func (donutMem donutMem) ListBuckets() ([]string, error) {
	return nil, errors.New("Not Implemented")
}

// bucket operations
func (donutMem donutMem) CreateBucket(b string) error {
	b = strings.ToLower(b)
	if _, ok := donutMem.buckets[b]; ok {
		return errors.New("Bucket Exists")
	}
	metadata := make(map[string]string)
	metadata["name"] = b
	newBucket := bucket{
		name:     b,
		metadata: metadata,
		objects:  make(map[string]object),
	}
	donutMem.buckets[b] = newBucket
	return nil
}
func (donutMem donutMem) ListObjects(bucket, prefix string) ([]string, error) {
	return nil, errors.New("Not Implemented")
}
func (donutMem donutMem) GetBucketMetadata(bucket, name string) (io.Reader, error) {
	return nil, errors.New("Not Implemented")
}
func (donutMem donutMem) SetBucketMetadata(bucket, name string, metadata io.Reader) error {
	return errors.New("Not Implemented")
}

// object operations
func (donutMem donutMem) GetObjectWriter(bucket, key string, column uint, blockSize uint) (io.WriteCloser, <-chan donutbox.Result, error) {
	if _, ok := donutMem.buckets[bucket]; ok {
		if _, ok := donutMem.buckets[bucket].objects[key]; !ok {
			reader, writer := io.Pipe()
			ch := make(chan donutbox.Result)
			go func() {
				var objBuffer bytes.Buffer
				_, err := io.Copy(&objBuffer, reader)
				metadata := make(map[string]string)
				metadata["key"] = key
				metadata["blockSize"] = strconv.FormatInt(int64(blockSize), 10)
				if err == nil {
					newObject := object{
						name:     key,
						data:     objBuffer.Bytes(),
						metadata: metadata,
					}
					donutMem.buckets[bucket].objects[key] = newObject
				}
				ch <- donutbox.Result{Err: err}
			}()
			return writer, ch, nil
		}
		return nil, nil, errors.New("Object exists")
	}
	return nil, nil, errors.New("Bucket not found")
}
func (donutMem donutMem) GetObjectReader(bucket, key string, column int) (io.Reader, error) {
	if b, ok := donutMem.buckets[bucket]; ok {
		if obj, ok := b.objects[key]; ok {
			return bytes.NewBuffer(obj.data), nil
		}
		return nil, errors.New("Object not found")
	}
	return nil, errors.New("Bucket not found")
}
func (donutMem donutMem) StoreObjectMetadata(bucket, object, name string, reader io.Reader) error {
	return errors.New("Not Implemented")
}
func (donutMem donutMem) GetObjectMetadata(bucket, object, name string) (io.Reader, error) {
	return nil, errors.New("Not Implemented")
}
