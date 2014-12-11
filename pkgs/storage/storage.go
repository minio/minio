package storage

type ObjectStorage interface {
	List(path string) ([]ObjectDescription, error)
	Get(path string) ([]byte, error)
	Put(path string, object []byte) error
}

type ObjectDescription struct {
	Path  string
	IsDir bool
	Hash  string
}
