package cmd

import (
	"context"
	"io"
)

type warmBackendGetOpts struct {
	startOffset int64
	length      int64
}

type warmBackend interface {
	Put(ctx context.Context, object string, r io.Reader, length int64) error
	Get(ctx context.Context, object string, opts warmBackendGetOpts) (io.ReadCloser, error)
	Remove(ctx context.Context, object string) error
	InUse(ctx context.Context) (bool, error)
	// GetTarget() (string, string)
}
