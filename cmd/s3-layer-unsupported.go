package cmd

// HealBucket - Not relevant.
func (l *S3Layer) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (l *S3Layer) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return []BucketInfo{}, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (l *S3Layer) HealObject(bucket string, object string) error {
	return traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (l *S3Layer) ListObjectsHeal(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, traceError(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (l *S3Layer) ListUploadsHeal(bucket string, prefix string, marker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, traceError(NotImplemented{})
}
