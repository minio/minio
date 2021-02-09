package main

import (
	"context"
	"log"

	"github.com/minio/minio/pkg/madmin"
)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// API requests are secure (HTTPS) if secure=true and insecure (HTTP) otherwise.
	// New returns an MinIO Admin client object.
	minioAdminClient, err := madmin.New("your-minio.example.com:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
	if err != nil {
		log.Fatalln(err)
	}
	// Create a test bucket and upload a test object and run this.
	bucket := "bucket"
	object := "/path/to/file"
	info, e := minioAdminClient.GetObjectDebugInfo(context.Background(), bucket, object)
	if e != nil {
		log.Fatalln(e)
	}
	log.Printf("info: %#v\n", info)
}
