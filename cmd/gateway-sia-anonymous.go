package cmd

import (
	"io"
)

// AnonGetBucketInfo - Get bucket metadata from azure anonymously.
func (l *siaObjects) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	return bucketInfo, nil
}

// AnonPutObject - SendPUT request without authentication.
// This is needed when clients send PUT requests on objects that can be uploaded without auth.
func (l *siaObjects) AnonPutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	return ObjectInfo{}, nil
}

// AnonGetObject - SendGET request without authentication.
// This is needed when clients send GET requests on objects that can be downloaded without auth.
func (l *siaObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return nil
}

// AnonGetObjectInfo - Send HEAD request without authentication and convert the
// result to ObjectInfo.
func (l *siaObjects) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return ObjectInfo{}, nil
}

// AnonListObjects - Use Azure equivalent ListBlobs.
func (l *siaObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return result, nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (l *siaObjects) AnonListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (result ListObjectsV2Info, err error) {
	return result, nil
}
