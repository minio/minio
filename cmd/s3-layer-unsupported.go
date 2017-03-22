package cmd

// HealBucket - Not relevant.
func (l *S3Layer) HealBucket(bucket string) error {
	panic("HealBucket: not implemented")
}

// ListBucketsHeal - Not relevant.
func (l *S3Layer) ListBucketsHeal() (buckets []BucketInfo, err error) {
	panic("ListBucketsHeal: not implemented")
}

// HealObject - Not relevant.
func (l *S3Layer) HealObject(bucket string, object string) error {
	panic("HealObject: not implemented")
}

// ListObjectsHeal - Not relevant.
func (l *S3Layer) ListObjectsHeal(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	panic("ListObjectsHeal: not implemented")
}

// ListUploadsHeal - Not relevant.
func (l *S3Layer) ListUploadsHeal(bucket string, prefix string, marker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	panic("ListUploadsHeal: not implemented")
}
