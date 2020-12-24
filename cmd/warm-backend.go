package cmd

import "io"

type warmBackend interface {
	Put(bucket, object string, r io.Reader, length int64) error
	Get(bucket, object string) (io.ReadCloser, int64, error)
	Remove(bucket, object string) error
}
