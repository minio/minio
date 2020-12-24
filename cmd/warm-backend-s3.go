package cmd

import (
	"io"
	"net/url"

	minio "github.com/minio/minio-go"
)

type warmBackendS3 struct {
	client *minio.Client
}

func (s3 *warmBackendS3) Put(bucket, object string, r io.Reader, length int64) error {
	_, err := s3.client.PutObject(bucket, object, r, length, minio.PutObjectOptions{})
	return err
}

func (s3 *warmBackendS3) Get(bucket, object string) (r io.ReadCloser, err error) {
	return s3.client.GetObject(bucket, object, minio.GetObjectOptions{})
}

func (s3 *warmBackendS3) Remove(bucket, object string) error {
	return s3.client.RemoveObject(bucket, object)
}

func newWarmBackendS3(endpoint, accessKey, secretKey string) (*warmBackendS3, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(u.Host, accessKey, secretKey, u.Scheme == "https")
	if err != nil {
		return nil, err
	}
	return &warmBackendS3{client}, nil
}
