package storage

import "io"

type ObjectStorage interface {
	List(path string) ([]ObjectDescription, error)
	Get(path string) (io.Reader, error)
	Put(path string, object io.Reader) error
}

type ObjectDescription struct {
	Path  string
	IsDir bool
	Hash  string
}
