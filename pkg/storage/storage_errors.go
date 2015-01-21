package storage

type GenericError struct {
	Bucket string
	Path   string
}

type ObjectExists struct {
	Bucket string
	Key    string
}

type BucketNameInvalid struct {
	Bucket string
}

type BucketExists struct {
	Bucket string
}

type ObjectNotFound GenericError

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
