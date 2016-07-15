## Using aws-sdk-go with Minio

aws-sdk-go is the official AWS SDK for the Go programming language. This document covers
how to use aws-sdk-go with Minio server.

### Install AWS SDK S3 service

```sh
$ go get github.com/aws/aws-sdk-go/service/s3
```

### List all buckets on Minio

```go
package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	newSession := session.New()
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("<YOUR-ACCESS-KEY-ID>", "<YOUR-SECRET-ACCESS-KEY", ""),
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	// Create an S3 service object in the default region.
	s3Client := s3.New(newSession, s3Config)

	cparams := &s3.CreateBucketInput{
		Bucket: aws.String("newbucket"), // Required
	}
	_, err := s3Client.CreateBucket(cparams)
	if err != nil {
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	var lparams *s3.ListBucketsInput
	// Call the ListBuckets() Operation
	resp, err := s3Client.ListBuckets(lparams)
	if err != nil {
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Println(resp)
}
```

Populate your AccessKeyId and SecretAccessKey credentials and run the program as shown below.

```sh
$ go run aws-sdk-minio.go
{
  Buckets: [{
      CreationDate: 2015-10-22 01:46:04 +0000 UTC,
      Name: "newbucket"
    }],
  Owner: {
    DisplayName: "minio",
    ID: "minio"
  }
}
```