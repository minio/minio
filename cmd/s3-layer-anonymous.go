package cmd

import "io"

// AnonGetObject - Get object anonymously
func (l *S3Layer) AnonGetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) (err error) {
	// already an s3 object error
	return l.GetObject(bucket, key, startOffset, length, writer)
}

// AnonGetObjectInfo - Get object info anonymously
func (l *S3Layer) AnonGetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	// already an s3 object error
	return l.GetObjectInfo(bucket, object)
}

// AnonListObjects - List objects anonymously
func (l *S3Layer) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	result, err = l.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return ListObjectsInfo{}, s3ToObjectError(traceError(err), bucket)
	}
	return
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *S3Layer) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	bucketInfo, err = l.GetBucketInfo(bucket)
	if err != nil {
		return bucketInfo, s3ToObjectError(traceError(err), bucket)
	}

	return
}
