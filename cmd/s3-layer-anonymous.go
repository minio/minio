package cmd

import "io"

// AnonGetObject - Get object anonymously
func (l *s3Gateway) AnonGetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	object, err := l.anonClient.GetObject(bucket, key)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	object.Seek(startOffset, io.SeekStart)
	if _, err := io.CopyN(writer, object, length); err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	object.Close()
	return nil
}

// AnonGetObjectInfo - Get object info anonymously
func (l *s3Gateway) AnonGetObjectInfo(bucket string, object string) (ObjectInfo, error) {
	oi, err := l.anonClient.StatObject(bucket, object)
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// AnonListObjects - List objects anonymously
func (l *s3Gateway) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	loi := ListObjectsInfo{}

	loi.Objects = make([]ObjectInfo, maxKeys)

	result, err := l.anonClient.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return ListObjectsInfo{}, err
	}

	return fromMinioClientListBucketResult(bucket, result), nil
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *s3Gateway) AnonGetBucketInfo(bucket string) (BucketInfo, error) {
	buckets, err := l.anonClient.ListBuckets()
	if err != nil {
		return BucketInfo{}, s3ToObjectError(traceError(err), bucket)
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return BucketInfo{}, traceError(BucketNotFound{Bucket: bucket})
}
