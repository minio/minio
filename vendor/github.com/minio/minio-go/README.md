# Minio Go Library for Amazon S3 Compatible Cloud Storage [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Description

Minio Go library is a simple client library for S3 compatible cloud storage servers. Supports AWS Signature Version 4 and 2. AWS Signature Version 4 is chosen as default.

List of supported cloud storage providers.

 - AWS Signature Version 4
   - Amazon S3
   - Minio

 - AWS Signature Version 2
   - Google Cloud Storage (Compatibility Mode)
   - Openstack Swift + Swift3 middleware
   - Ceph Object Gateway
   - Riak CS

## Install

If you do not have a working Golang environment, please follow [Install Golang](./INSTALLGO.md).

```sh
$ go get github.com/minio/minio-go
```

## Example

### ListBuckets()

This example shows how to List your buckets.

```go
package main

import (
	"log"

	"github.com/minio/minio-go"
)

func main() {
	// Requests are always secure (HTTPS) by default. Set insecure=true to enable insecure (HTTP) access.
	// This boolean value is the last argument for New().

	// New returns an Amazon S3 compatible client object. API copatibality (v2 or v4) is automatically
	// determined based on the Endpoint value.
	s3Client, err := minio.New("s3.amazonaws.com", "YOUR-ACCESS-KEY-HERE", "YOUR-SECRET-KEY-HERE", false)
	if err != nil {
	    log.Fatalln(err)
	}
	buckets, err := s3Client.ListBuckets()
	if err != nil {
		log.Fatalln(err)
	}
	for _, bucket := range buckets {
		log.Println(bucket)
	}
}
```

## Documentation

### Bucket Operations.
* [MakeBucket(bucketName, BucketACL, location) error](examples/s3/makebucket.go)
* [BucketExists(bucketName) error](examples/s3/bucketexists.go)
* [RemoveBucket(bucketName) error](examples/s3/removebucket.go)
* [GetBucketACL(bucketName) (BucketACL, error)](examples/s3/getbucketacl.go)
* [SetBucketACL(bucketName, BucketACL) error)](examples/s3/setbucketacl.go)
* [ListBuckets() []BucketInfo](examples/s3/listbuckets.go)
* [ListObjects(bucketName, objectPrefix, recursive, chan<- struct{}) <-chan ObjectInfo](examples/s3/listobjects.go)
* [ListIncompleteUploads(bucketName, prefix, recursive, chan<- struct{}) <-chan ObjectMultipartInfo](examples/s3/listincompleteuploads.go)

### Object Operations.
* [PutObject(bucketName, objectName, io.Reader, contentType) error](examples/s3/putobject.go)
* [GetObject(bucketName, objectName) (*Object, error)](examples/s3/getobject.go)
* [StatObject(bucketName, objectName) (ObjectInfo, error)](examples/s3/statobject.go)
* [RemoveObject(bucketName, objectName) error](examples/s3/removeobject.go)
* [RemoveIncompleteUpload(bucketName, objectName) <-chan error](examples/s3/removeincompleteupload.go)

### File Object Operations.
* [FPutObject(bucketName, objectName, filePath, contentType) (size, error)](examples/s3/fputobject.go)
* [FGetObject(bucketName, objectName, filePath) error](examples/s3/fgetobject.go)

### Presigned Operations.
* [PresignedGetObject(bucketName, objectName, time.Duration, url.Values) (string, error)](examples/s3/presignedgetobject.go)
* [PresignedPutObject(bucketName, objectName, time.Duration) (string, error)](examples/s3/presignedputobject.go)
* [PresignedPostPolicy(NewPostPolicy()) (map[string]string, error)](examples/s3/presignedpostpolicy.go)

### API Reference

[![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/minio/minio-go)

## Contribute

[Contributors Guide](./CONTRIBUTING.md)

[![Build Status](https://travis-ci.org/minio/minio-go.svg)](https://travis-ci.org/minio/minio-go) [![Build status](https://ci.appveyor.com/api/projects/status/1ep7n2resn6fk1w6?svg=true)](https://ci.appveyor.com/project/harshavardhana/minio-go)
