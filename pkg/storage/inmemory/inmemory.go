package inmemory

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

type Storage struct {
	bucketdata map[string]storedBucket
	objectdata map[string]storedObject
}

type storedBucket struct {
	metadata mstorage.BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type storedObject struct {
	metadata mstorage.ObjectMetadata
	data     []byte
}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.objectdata[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, mstorage.ObjectNotFound{Bucket: bucket, Path: object}
	}
}

func (storage *Storage) StoreObject(bucket string, key string, data io.Reader) error {
	objectKey := bucket + ":" + key
	if _, ok := storage.objectdata[objectKey]; ok == true {
		return mstorage.ObjectExists{Bucket: bucket, Key: key}
	}
	var bytesBuffer bytes.Buffer
	newObject := storedObject{}
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		size := bytesBuffer.Len()
		etag := fmt.Sprintf("%x", sha256.Sum256(bytesBuffer.Bytes()))
		newObject.metadata = mstorage.ObjectMetadata{
			Key:        key,
			SecCreated: time.Now().Unix(),
			Size:       size,
			ETag:       etag,
		}
		newObject.data = bytesBuffer.Bytes()
	}
	storage.objectdata[objectKey] = newObject
	return nil
}

func (storage *Storage) StoreBucket(bucketName string) error {
	if !isValidBucket(bucketName) {
		return mstorage.BucketNameInvalid{Bucket: bucketName}
	}

	if _, ok := storage.bucketdata[bucketName]; ok == true {
		return mstorage.BucketExists{Bucket: bucketName}
	}
	newBucket := storedBucket{}
	newBucket.metadata = mstorage.BucketMetadata{
		Name:    bucketName,
		Created: time.Now().Unix(),
	}
	log.Println(bucketName)
	storage.bucketdata[bucketName] = newBucket
	return nil
}

func (storage *Storage) ListObjects(bucket, prefix string, count int) []mstorage.ObjectMetadata {
	// TODO prefix and count handling
	var results []mstorage.ObjectMetadata
	for key, object := range storage.objectdata {
		if strings.HasPrefix(key, bucket+":") {
			results = append(results, object.metadata)
		}
	}
	return results
}

func (storage *Storage) ListBuckets(prefix string) []mstorage.BucketMetadata {
	// TODO prefix handling
	var results []mstorage.BucketMetadata
	for _, bucket := range storage.bucketdata {
		results = append(results, bucket.metadata)
	}
	return results
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		bucketdata: make(map[string]storedBucket),
		objectdata: make(map[string]storedObject),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

func isValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", bucket); match == true {
		return false
	}
	match, _ := regexp.MatchString("[a-zA-Z0-9\\.\\-]", bucket)
	return match
}

func (storage *Storage) GetObjectMetadata(bucket, key string) mstorage.ObjectMetadata {
	objectKey := bucket + ":" + key

	return storage.objectdata[objectKey].metadata
}
