/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/policy"
)

// Storage - fs local variables
type Storage struct {
	root string
	lock *sync.Mutex
}

// Metadata - carries metadata about object
type Metadata struct {
	Md5sum      []byte
	ContentType string
}

// Start filesystem channel
func Start(root string) (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	s := Storage{}
	s.root = root
	s.lock = new(sync.Mutex)
	go start(ctrlChannel, errorChannel, &s)
	return ctrlChannel, errorChannel, &s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *Storage) {
	err := os.MkdirAll(s.root, 0700)
	errorChannel <- err
	close(errorChannel)
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

/// Bucket Operations

// ListBuckets - Get service
func (storage *Storage) ListBuckets() ([]mstorage.BucketMetadata, error) {
	files, err := ioutil.ReadDir(storage.root)
	if err != nil {
		return []mstorage.BucketMetadata{}, mstorage.EmbedError("bucket", "", err)
	}

	var metadataList []mstorage.BucketMetadata
	for _, file := range files {
		// Skip policy files
		if strings.HasSuffix(file.Name(), "_policy.json") {
			continue
		}
		if !file.IsDir() {
			return []mstorage.BucketMetadata{}, mstorage.BackendCorrupted{Path: storage.root}
		}
		metadata := mstorage.BucketMetadata{
			Name:    file.Name(),
			Created: file.ModTime(), // TODO - provide real created time
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// StoreBucket - PUT Bucket
func (storage *Storage) StoreBucket(bucket string) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return mstorage.BucketExists{
			Bucket: bucket,
		}
	}

	// make bucket
	err := os.Mkdir(bucketDir, 0700)
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	return nil
}

// GetBucketPolicy - GET bucket policy
func (storage *Storage) GetBucketPolicy(bucket string) (interface{}, error) {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	var p policy.BucketPolicy
	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return policy.BucketPolicy{}, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return policy.BucketPolicy{}, mstorage.BucketNotFound{Bucket: bucket}
	}

	// get policy path
	bucketPolicy := path.Join(storage.root, bucket+"_policy.json")
	filestat, err := os.Stat(bucketPolicy)

	if os.IsNotExist(err) {
		return policy.BucketPolicy{}, mstorage.BucketPolicyNotFound{Bucket: bucket}
	}

	if filestat.IsDir() {
		return policy.BucketPolicy{}, mstorage.BackendCorrupted{Path: bucketPolicy}
	}

	file, err := os.OpenFile(bucketPolicy, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return policy.BucketPolicy{}, mstorage.EmbedError(bucket, "", err)
	}
	encoder := json.NewDecoder(file)
	err = encoder.Decode(&p)
	if err != nil {
		return policy.BucketPolicy{}, mstorage.EmbedError(bucket, "", err)
	}

	return p, nil

}

// StoreBucketPolicy - PUT bucket policy
func (storage *Storage) StoreBucketPolicy(bucket string, policy interface{}) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return mstorage.BucketNotFound{
			Bucket: bucket,
		}
	}

	// get policy path
	bucketPolicy := path.Join(storage.root, bucket+"_policy.json")
	filestat, ret := os.Stat(bucketPolicy)
	if !os.IsNotExist(ret) {
		if filestat.IsDir() {
			return mstorage.BackendCorrupted{Path: bucketPolicy}
		}
	}

	file, err := os.OpenFile(bucketPolicy, os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	encoder := json.NewEncoder(file)
	err = encoder.Encode(policy)
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	return nil
}

/// Object Operations

// CopyObjectToWriter - GET object
func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// validate bucket
	if mstorage.IsValidBucket(bucket) == false {
		return 0, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// validate object
	if mstorage.IsValidObject(object) == false {
		return 0, mstorage.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	objectPath := path.Join(storage.root, bucket, object)

	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
			return 0, mstorage.EmbedError(bucket, object, err)
		}
	}
	file, err := os.Open(objectPath)
	defer file.Close()
	if err != nil {
		return 0, mstorage.EmbedError(bucket, object, err)
	}

	count, err := io.Copy(w, file)
	if err != nil {
		return count, mstorage.EmbedError(bucket, object, err)
	}
	return count, nil
}

// GetObjectMetadata - HEAD object
func (storage *Storage) GetObjectMetadata(bucket, object string) (mstorage.ObjectMetadata, error) {
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.ObjectMetadata{}, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	if mstorage.IsValidObject(object) == false {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNameInvalid{Bucket: bucket, Object: bucket}
	}

	objectPath := path.Join(storage.root, bucket, object)

	stat, err := os.Stat(objectPath)
	if os.IsNotExist(err) {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
	}

	_, err = os.Stat(objectPath + "$metadata")
	if os.IsNotExist(err) {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
	}

	file, err := os.Open(objectPath + "$metadata")
	defer file.Close()
	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
	}

	var deserializedMetadata Metadata
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&deserializedMetadata)

	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
	}

	contentType := "application/octet-stream"
	if deserializedMetadata.ContentType != "" {
		contentType = deserializedMetadata.ContentType
	}
	contentType = strings.TrimSpace(contentType)

	etag := bucket + "#" + path.Base(object)
	if len(deserializedMetadata.Md5sum) != 0 {
		etag = hex.EncodeToString(deserializedMetadata.Md5sum)
	}
	metadata := mstorage.ObjectMetadata{
		Bucket:      bucket,
		Key:         path.Base(object),
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		ETag:        etag,
		ContentType: contentType,
	}

	return metadata, nil
}

type bucketDir struct {
	files map[string]os.FileInfo
	root  string
}

func (p *bucketDir) getAllFiles(object string, fl os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if fl.Mode().IsRegular() {
		if strings.HasSuffix(object, "$metadata") {
			return nil
		}
		_p := strings.Split(object, p.root+"/")
		if len(_p) > 1 {
			p.files[_p[1]] = fl
		}
	}
	return nil
}

func delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

type byObjectKey []mstorage.ObjectMetadata

// Len
func (b byObjectKey) Len() int { return len(b) }

// Swap
func (b byObjectKey) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less
func (b byObjectKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

// ListObjects - GET bucket (list objects)
func (storage *Storage) ListObjects(bucket string, resources mstorage.BucketResourcesMetadata) ([]mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	p := bucketDir{}
	p.files = make(map[string]os.FileInfo)

	if mstorage.IsValidBucket(bucket) == false {
		return []mstorage.ObjectMetadata{}, resources, mstorage.BucketNameInvalid{Bucket: bucket}
	}
	if resources.Prefix != "" && mstorage.IsValidObject(resources.Prefix) == false {
		return []mstorage.ObjectMetadata{}, resources, mstorage.ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix}
	}

	rootPrefix := path.Join(storage.root, bucket)
	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return []mstorage.ObjectMetadata{}, resources, mstorage.BucketNotFound{Bucket: bucket}
	}

	p.root = rootPrefix
	err := filepath.Walk(rootPrefix, p.getAllFiles)
	if err != nil {
		return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
	}

	var metadataList []mstorage.ObjectMetadata
	for name, file := range p.files {
		if len(metadataList) >= resources.Maxkeys {
			resources.IsTruncated = true
			goto ret
		}
		// TODO handle resources.Marker
		switch true {
		case resources.Delimiter != "" && resources.Prefix == "":
			delimitedName := delimiter(name, resources.Delimiter)
			switch true {
			case delimitedName == "":
				metadata, err := storage.GetObjectMetadata(bucket, name)
				if err != nil {
					return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
				metadataList = append(metadataList, metadata)
			case delimitedName == file.Name():
				metadata, err := storage.GetObjectMetadata(bucket, name)
				if err != nil {
					return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
				metadataList = append(metadataList, metadata)
			case delimitedName != "":
				resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
			}
		case resources.Delimiter != "" && strings.HasPrefix(name, resources.Prefix):
			trimmedName := strings.TrimPrefix(name, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			switch true {
			case name == resources.Prefix:
				metadata, err := storage.GetObjectMetadata(bucket, name)
				if err != nil {
					return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
				metadataList = append(metadataList, metadata)
			case delimitedName == file.Name():
				metadata, err := storage.GetObjectMetadata(bucket, name)
				if err != nil {
					return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
				metadataList = append(metadataList, metadata)
			case delimitedName != "":
				if delimitedName == resources.Delimiter {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
				} else {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
				}
			}
		case strings.HasPrefix(name, resources.Prefix):
			metadata, err := storage.GetObjectMetadata(bucket, name)
			if err != nil {
				return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
			}
			metadataList = append(metadataList, metadata)
		}
	}

ret:
	sort.Sort(byObjectKey(metadataList))
	return metadataList, resources, nil
}

// StoreObject - PUT object
func (storage *Storage) StoreObject(bucket, key, contentType string, data io.Reader) error {
	// TODO Commits should stage then move instead of writing directly
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// check bucket name valid
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// check bucket exists
	if _, err := os.Stat(path.Join(storage.root, bucket)); os.IsNotExist(err) {
		return mstorage.BucketNotFound{Bucket: bucket}
	}

	// verify object path legal
	if mstorage.IsValidObject(key) == false {
		return mstorage.ObjectNameInvalid{Bucket: bucket, Object: key}
	}

	// verify content type
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)

	// get object path
	objectPath := path.Join(storage.root, bucket, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return mstorage.EmbedError(bucket, key, err)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return mstorage.ObjectExists{
			Bucket: bucket,
			Object: key,
		}
	}

	// write object
	file, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	h := md5.New()
	mw := io.MultiWriter(file, h)

	_, err = io.Copy(mw, data)
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	//
	file, err = os.OpenFile(objectPath+"$metadata", os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	// serialize metadata to gob
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(&Metadata{
		ContentType: contentType,
		Md5sum:      h.Sum(nil),
	})
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	return nil
}
