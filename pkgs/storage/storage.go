package storage

import "io"

type ObjectStorage interface {
	List() ([]ObjectDescription, error)
	Get(path string) (io.Reader, error)
	Put(path string, object io.Reader) error
}

type ObjectDescription struct {
	Name    string
	Md5sum  string
	Murmur3 string
}
