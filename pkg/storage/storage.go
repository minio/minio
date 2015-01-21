package storage

import (
	"bytes"
	"io"
	"log"
	"strings"
	"time"
)

type Storage struct {
	data map[string]storedObject
}

type storedObject struct {
	metadata ObjectMetadata
	data     []byte
}

type ObjectMetadata struct {
	Key        string
	SecCreated int64
	Size       int
}

type GenericError struct {
	bucket string
	path   string
}

type ObjectNotFound GenericError

func (self ObjectNotFound) Error() string {
	return "Not Found: " + self.bucket + "#" + self.path
}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.data[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, ObjectNotFound{bucket: bucket, path: object}
	}
}

func (storage *Storage) StoreObject(bucket string, key string, data io.Reader) {
	objectKey := bucket + ":" + key
	var bytesBuffer bytes.Buffer
	newObject := storedObject{}
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		newObject.metadata = ObjectMetadata{
			Key:        key,
			SecCreated: time.Now().Unix(),
			Size:       len(bytesBuffer.Bytes()),
		}
		newObject.data = bytesBuffer.Bytes()
	}
	storage.data[objectKey] = newObject
}

func (storage *Storage) ListObjects(bucket, prefix string, count int) []ObjectMetadata {
	var results []ObjectMetadata
	for key, object := range storage.data {
		log.Println(key)
		if strings.HasPrefix(key, bucket+":") {
			results = append(results, object.metadata)
		}
	}
	return results
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		data: make(map[string]storedObject),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}
