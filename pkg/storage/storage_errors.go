package storage

type GenericError struct {
	bucket string
	path   string
}

type ObjectExists struct {
	bucket string
	key    string
}

type BucketNameInvalid struct {
	bucket string
}

type BucketExists struct {
	bucket string
}

type ObjectNotFound GenericError

func (self ObjectNotFound) Error() string {
	return "Object not Found: " + self.bucket + "#" + self.path
}

func (self ObjectExists) Error() string {
	return "Object exists: " + self.bucket + "#" + self.key
}

func (self BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + self.bucket
}

func (self BucketExists) Error() string {
	return "Bucket exists: " + self.bucket
}
