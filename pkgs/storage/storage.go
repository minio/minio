package storage

type ObjectStorage interface {
	Get(path string) ([]byte, error)
	Put(path string, object []byte) error
}
