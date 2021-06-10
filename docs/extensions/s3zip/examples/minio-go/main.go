package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3ExtensionTransport struct {
	tr http.RoundTripper
}

func (t *s3ExtensionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("x-minio-extract", "true")
	return t.tr.RoundTrip(req)
}

func main() {
	tr, _ := minio.DefaultTransport(false)

	s3Client, err := minio.New("minio-server-address:9000", &minio.Options{
		Creds:     credentials.NewStaticV4("access-key", "secret-key", ""),
		Transport: &s3ExtensionTransport{tr},
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Download API.md from the archive
	rd, err := s3Client.GetObject(context.Background(), "your-bucket", "path/to/file.zip/data.csv", minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	_, err = io.Copy(os.Stdout, rd)
	if err != nil {
		log.Fatalln(err)
	}

	return
}
