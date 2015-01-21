package storage

type GenericError struct {
	bucket string
	path   string
}

type ObjectExists struct {
	bucket string
	key    string
}

type ObjectNotFound GenericError

func (self ObjectNotFound) Error() string {
	return "Not Found: " + self.bucket + "#" + self.path
}

func (self ObjectExists) Error() string {
	return "Object exists: " + self.bucket + "#" + self.key
}
