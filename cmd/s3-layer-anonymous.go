package cmd

import "io"

// AnonGetObject - Get object anonymously
func (l *S3Layer) AnonGetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) (err error) {
	return l.GetObject(bucket, key, startOffset, length, writer)
}

// AnonGetObjectInfo - Get object info anonymously
func (l *S3Layer) AnonGetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	return l.GetObjectInfo(bucket, object)
}

// AnonListObjects - List objects anonymously
func (l *S3Layer) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return l.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *S3Layer) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	return l.GetBucketInfo(bucket)
}
