package storage

type ObjectStorage interface {
	GetList() ([]byte, error)
	Get(path string) ([]byte, error)
	Put(path string, object []byte) error
}
