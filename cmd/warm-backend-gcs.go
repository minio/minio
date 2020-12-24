package cmd

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type warmBackendGCS struct {
	client *storage.Client
}

func (gcs *warmBackendGCS) Put(bucket, key string, data io.Reader, length int64) error {
	object := gcs.client.Bucket(bucket).Object(key)

	w := object.NewWriter(context.Background())

	if _, err := io.Copy(w, data); err != nil {
		return err
	}

	return w.Close()
}

func (gcs *warmBackendGCS) Get(bucket, key string) (r io.ReadCloser, err error) {
	// GCS storage decompresses a gzipped object by default and returns the data.
	// Refer to https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding
	// Need to set `Accept-Encoding` header to `gzip` when issuing a GetObject call, to be able
	// to download the object in compressed state.
	// Calling ReadCompressed with true accomplishes that.
	object := gcs.client.Bucket(bucket).Object(key).ReadCompressed(true)

	r, err = object.NewRangeReader(context.Background(), 0, 0)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (gcs *warmBackendGCS) Remove(bucket, object string) error {
	return gcs.client.Bucket(bucket).Object(object).Delete(context.Background())
}

func newWarmBackendGCS() (*warmBackendGCS, error) {
	return nil, nil
}
