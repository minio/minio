package storage

type GenericError struct {
	Bucket string
	Path   string
}

type ObjectExists struct {
	Bucket string
	Key    string
}

type ObjectNotFound GenericError

type GenericBucketError struct {
	Bucket string
}

type BucketNameInvalid GenericBucketError
type BucketExists GenericBucketError
type BucketNotFound GenericBucketError

func (self ObjectNotFound) Error() string {
	return "Object not Found: " + self.Bucket + "#" + self.Path
}

func (self ObjectExists) Error() string {
	return "Object exists: " + self.Bucket + "#" + self.Key
}

func (self BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + self.Bucket
}

func (self BucketExists) Error() string {
	return "Bucket exists: " + self.Bucket
}

func (self BucketNotFound) Error() string {
	return "Bucket not Found: " + self.Bucket
}
