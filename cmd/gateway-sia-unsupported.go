package cmd

// HealBucket - Not relevant.
func (l *siaObjects) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (l *siaObjects) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return nil, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (l *siaObjects) HealObject(bucket, object string) (int, int, error) {
	return 0, 0, traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (l *siaObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, traceError(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (l *siaObjects) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	return lmi, traceError(NotImplemented{})
}
