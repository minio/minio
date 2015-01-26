package storage

type GenericError struct {
	Bucket string
	Path   string
}

type ObjectExists struct {
	Bucket string
	Key    string
}

type ObjectNotFound GenericObjectError

type GenericBucketError struct {
	Bucket string
}

type GenericObjectError struct {
	Bucket string
	Object string
}

type ImplementationError struct {
	Bucket string
	Object string
	Err    error
}

func (self ImplementationError) Error() string {
	error := ""
	if self.Bucket != "" {
		error = error + "Bucket: " + self.Bucket + " "
	}
	if self.Object != "" {
		error = error + "Object: " + self.Object + " "
	}
	error = error + "Error: " + self.Err.Error()
	return error
}

func EmbedError(bucket, object string, err error) ImplementationError {
	return ImplementationError{
		Bucket: bucket,
		Object: object,
		Err:    err,
	}
}

type BucketNameInvalid GenericBucketError
type BucketExists GenericBucketError
type BucketNotFound GenericBucketError
type ObjectNameInvalid GenericObjectError

func (self ObjectNotFound) Error() string {
	return "Object not Found: " + self.Bucket + "#" + self.Object
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

func (self ObjectNameInvalid) Error() string {
	return "Object name invalid: " + self.Bucket + "#" + self.Object
}
