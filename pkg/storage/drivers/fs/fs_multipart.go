package filesystem

import (
	"bytes"
	"crypto/md5"
	"crypto/sha512"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

// MultipartSession holds active session information
type MultipartSession struct {
	TotalParts int
	UploadID   string
	Initiated  time.Time
	Parts      []*drivers.PartMetadata
}

// Multiparts collection of many parts
type Multiparts struct {
	ActiveSession map[string]*MultipartSession
}

func (fs *fsDriver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	return drivers.BucketMultipartResourcesMetadata{}, iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) NewMultipartUpload(bucket, key, contentType string) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if !drivers.IsValidBucket(bucket) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}

	bucketPath := path.Join(fs.root, bucket)
	_, err := os.Stat(bucketPath)

	// check bucket exists
	if os.IsNotExist(err) {
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if err != nil {
		return "", iodine.New(drivers.InternalError{}, nil)
	}

	objectPath := path.Join(bucketPath, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return "", iodine.New(drivers.ObjectExists{
			Bucket: bucket,
			Object: key,
		}, nil)
	}

	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucket + key + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	file, err := os.OpenFile(objectPath+"$multiparts", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	defer file.Close()

	mpartSession := new(MultipartSession)
	mpartSession.TotalParts = 0
	mpartSession.UploadID = uploadID
	mpartSession.Initiated = time.Now()
	var parts []*drivers.PartMetadata
	mpartSession.Parts = parts
	fs.multiparts.ActiveSession[uploadID] = mpartSession

	// serialize metadata to gob
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(mpartSession)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	return uploadID, nil
}

func (fs *fsDriver) isValidUploadID(uploadID string) bool {
	_, ok := fs.multiparts.ActiveSession[uploadID]
	return ok
}

func (fs *fsDriver) writePart(objectPath string, partID int, size int64, data io.Reader) (drivers.PartMetadata, error) {
	partPath := objectPath + fmt.Sprintf("$%d", partID)
	// write part
	partFile, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return drivers.PartMetadata{}, iodine.New(err, nil)
	}
	defer partFile.Close()

	h := md5.New()
	mw := io.MultiWriter(partFile, h)

	_, err = io.CopyN(mw, data, size)
	if err != nil {
		return drivers.PartMetadata{}, iodine.New(err, nil)
	}

	fi, err := os.Stat(partPath)
	if err != nil {
		return drivers.PartMetadata{}, iodine.New(err, nil)
	}
	partMetadata := drivers.PartMetadata{}
	partMetadata.ETag = hex.EncodeToString(h.Sum(nil))
	partMetadata.PartNumber = partID
	partMetadata.Size = fi.Size()
	partMetadata.LastModified = fi.ModTime()
	return partMetadata, nil
}

// partNumber is a sortable interface for Part slice
type partNumber []*drivers.PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

func (fs *fsDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if partID <= 0 {
		return "", iodine.New(errors.New("invalid part id, cannot be zero or less than zero"), nil)
	}
	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// verify object path legal
	if drivers.IsValidObjectName(key) == false {
		return "", iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: key}, nil)
	}

	if !fs.isValidUploadID(uploadID) {
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}

	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return "", iodine.New(drivers.InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	bucketPath := path.Join(fs.root, bucket)
	_, err := os.Stat(bucketPath)

	// check bucket exists
	if os.IsNotExist(err) {
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if err != nil {
		return "", iodine.New(drivers.InternalError{}, nil)
	}

	objectPath := path.Join(bucketPath, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return "", iodine.New(drivers.ObjectExists{
			Bucket: bucket,
			Object: key,
		}, nil)
	}
	partMetadata, err := fs.writePart(objectPath, partID, size, data)
	if err != nil {
		return "", iodine.New(err, nil)
	}

	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), partMetadata.ETag); err != nil {
			return "", iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Key: key}, nil)
		}
	}

	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDWR, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	defer multiPartfile.Close()

	var deserializedMultipartSession MultipartSession
	decoder := gob.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	deserializedMultipartSession.Parts = append(deserializedMultipartSession.Parts, &partMetadata)
	deserializedMultipartSession.TotalParts++
	fs.multiparts.ActiveSession[uploadID] = &deserializedMultipartSession

	sort.Sort(partNumber(deserializedMultipartSession.Parts))
	encoder := gob.NewEncoder(multiPartfile)
	err = encoder.Encode(&deserializedMultipartSession)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	return partMetadata.ETag, nil
}

func (fs *fsDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// verify object path legal
	if drivers.IsValidObjectName(key) == false {
		return "", iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: key}, nil)
	}

	if !fs.isValidUploadID(uploadID) {
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}

	bucketPath := path.Join(fs.root, bucket)
	_, err := os.Stat(bucketPath)
	// check bucket exists
	if os.IsNotExist(err) {
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if err != nil {
		return "", iodine.New(drivers.InternalError{}, nil)
	}

	objectPath := path.Join(bucketPath, key)
	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return "", iodine.New(drivers.ObjectExists{
			Bucket: bucket,
			Object: key,
		}, nil)
	}

	file, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	defer file.Close()
	h := md5.New()
	mw := io.MultiWriter(file, h)

	for i := 1; i <= len(parts); i++ {
		recvMD5 := parts[i]
		partFile, err := os.OpenFile(objectPath+fmt.Sprintf("$%d", i), os.O_RDONLY, 0600)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		obj, err := ioutil.ReadAll(partFile)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		calcMD5Bytes := md5.Sum(obj)
		// complete multi part request header md5sum per part is hex encoded
		recvMD5Bytes, err := hex.DecodeString(strings.Trim(recvMD5, "\""))
		if err != nil {
			return "", iodine.New(drivers.InvalidDigest{Md5: recvMD5}, nil)
		}
		if !bytes.Equal(recvMD5Bytes, calcMD5Bytes[:]) {
			return "", iodine.New(drivers.BadDigest{Md5: recvMD5, Bucket: bucket, Key: key}, nil)
		}
		_, err = io.Copy(mw, bytes.NewBuffer(obj))
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}
	md5sum := hex.EncodeToString(h.Sum(nil))

	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDWR, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	var deserializedMultipartSession MultipartSession
	decoder := gob.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	multiPartfile.Close() // close it right here, since we will delete it subsequently

	delete(fs.multiparts.ActiveSession, uploadID)
	for _, part := range deserializedMultipartSession.Parts {
		err = os.RemoveAll(objectPath + fmt.Sprintf("$%d", part.PartNumber))
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}
	err = os.RemoveAll(objectPath + "$multiparts")
	if err != nil {
		return "", iodine.New(err, nil)
	}
	return md5sum, nil
}

func (fs *fsDriver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// verify object path legal
	if drivers.IsValidObjectName(key) == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: key}, nil)
	}

	if !fs.isValidUploadID(resources.UploadID) {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.InvalidUploadID{UploadID: resources.UploadID}, nil)
	}

	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucket
	objectResourcesMetadata.Key = key
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}

	bucketPath := path.Join(fs.root, bucket)
	_, err := os.Stat(bucketPath)
	// check bucket exists
	if os.IsNotExist(err) {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if err != nil {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.InternalError{}, nil)
	}

	objectPath := path.Join(bucketPath, key)
	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDWR, 0600)
	if err != nil {
		return drivers.ObjectResourcesMetadata{}, iodine.New(err, nil)
	}
	defer multiPartfile.Close()

	var deserializedMultipartSession MultipartSession
	decoder := gob.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return drivers.ObjectResourcesMetadata{}, iodine.New(err, nil)
	}
	var parts []*drivers.PartMetadata
	for i := startPartNumber; i <= deserializedMultipartSession.TotalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		parts = append(parts, deserializedMultipartSession.Parts[i-1])
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

func (fs *fsDriver) AbortMultipartUpload(bucket, key, uploadID string) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// verify object path legal
	if drivers.IsValidObjectName(key) == false {
		return iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: key}, nil)
	}

	if !fs.isValidUploadID(uploadID) {
		return iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}

	bucketPath := path.Join(fs.root, bucket)
	_, err := os.Stat(bucketPath)
	// check bucket exists
	if os.IsNotExist(err) {
		return iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if err != nil {
		return iodine.New(drivers.InternalError{}, nil)
	}

	objectPath := path.Join(bucketPath, key)
	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDWR, 0600)
	if err != nil {
		return iodine.New(err, nil)
	}

	var deserializedMultipartSession MultipartSession
	decoder := gob.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return iodine.New(err, nil)
	}
	multiPartfile.Close() // close it right here, since we will delete it subsequently

	delete(fs.multiparts.ActiveSession, uploadID)
	for _, part := range deserializedMultipartSession.Parts {
		err = os.RemoveAll(objectPath + fmt.Sprintf("$%d", part.PartNumber))
		if err != nil {
			return iodine.New(err, nil)
		}
	}
	err = os.RemoveAll(objectPath + "$multiparts")
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}
