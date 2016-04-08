package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio/pkg/probe"
)

type objectLayer struct {
	storage StorageAPI
}

func newObjectLayer(storage StorageAPI) *objectLayer {
	return &objectLayer{storage}
}

// Bucket operations

func (o objectLayer) MakeBucket(bucket string) *probe.Error {
	return probe.NewError(o.storage.MakeVol(bucket))
}

func (o objectLayer) GetBucketInfo(bucket string) (BucketInfo, *probe.Error) {
	vi, e := o.storage.StatVol(bucket)
	if e != nil {
		return BucketInfo{}, probe.NewError(e)
	}
	return BucketInfo{vi.Name, vi.Created}, nil
}

func (o objectLayer) ListBuckets() ([]BucketInfo, *probe.Error) {
	var bucketInfos []BucketInfo
	vols, e := o.storage.ListVols()
	if e != nil {
		return nil, probe.NewError(e)
	}
	for _, vol := range vols {
		bucketInfos = append(bucketInfos, BucketInfo{vol.Name, vol.Created})
	}
	return bucketInfos, nil
}

func (o objectLayer) DeleteBucket(bucket string) *probe.Error {
	return probe.NewError(o.storage.DeleteVol(bucket))
}

// Object Operations

func (o objectLayer) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, *probe.Error) {
	r, e := o.storage.ReadFile(bucket, object, startOffset)
	if e != nil {
		return nil, probe.NewError(e)
	}
	return r, nil
}

func (o objectLayer) GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error) {
	fi, e := o.storage.StatFile(bucket, object)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	fmt.Println(fi)
	return ObjectInfo{
		Bucket:       fi.Volume,
		Name:         fi.Name,
		ModifiedTime: fi.ModifiedTime,
		Size:         fi.Size,
		IsDir:        fi.IsDir}, nil
}

func (o objectLayer) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (ObjectInfo, *probe.Error) {
	w, e := o.storage.CreateFile(bucket, object)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	_, e = io.Copy(w, data)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	e = w.Close()
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	fi, e := o.storage.StatFile(bucket, object)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	return ObjectInfo{Bucket: fi.Volume, Name: fi.Name, ModifiedTime: fi.ModifiedTime, Size: fi.Size}, nil
}

func (o objectLayer) DeleteObject(bucket, object string) *probe.Error {
	return probe.NewError(o.storage.DeleteFile(bucket, object))
}

func (o objectLayer) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error) {
	recursive := true
	if delimiter == "/" {
		recursive = false
	}
	fileInfos, eof, e := o.storage.ListFiles(bucket, prefix, marker, recursive, maxKeys)
	if e != nil {
		return ListObjectsInfo{}, probe.NewError(e)
	}
	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		result.NextMarker = fileInfo.Name
		if fileInfo.IsDir {
			result.Prefixes = append(result.Prefixes, fileInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:         fileInfo.Name,
			ModifiedTime: fileInfo.ModifiedTime,
			Size:         fileInfo.Size,
			IsDir:        fileInfo.IsDir,
		})
	}
	return result, nil
}

func (o objectLayer) ListMultipartUploads(bucket, objectPrefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error) {
	return ListMultipartsInfo{}, probe.NewError(errors.New("Not implemented"))
}

func (o objectLayer) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	return "", probe.NewError(errors.New("Not implemented"))
}

func (o objectLayer) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
	return "", probe.NewError(errors.New("Not implemented"))
}

func (o objectLayer) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error) {
	return ListPartsInfo{}, probe.NewError(errors.New("Not implemented"))
}

func (o objectLayer) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (ObjectInfo, *probe.Error) {
	return ObjectInfo{}, probe.NewError(errors.New("Not implemented"))
}

func (o objectLayer) AbortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	return probe.NewError(errors.New("Not implemented"))
}
