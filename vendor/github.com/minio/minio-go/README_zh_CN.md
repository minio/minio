# 适用于与Amazon S3兼容云存储的Minio Go SDK [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Sourcegraph](https://sourcegraph.com/github.com/minio/minio-go/-/badge.svg)](https://sourcegraph.com/github.com/minio/minio-go?badge)

Minio Go Client SDK提供了简单的API来访问任何与Amazon S3兼容的对象存储服务。

**支持的云存储:** 

- AWS Signature Version 4
   - Amazon S3
   - Minio

- AWS Signature Version 2
   - Google Cloud Storage (兼容模式)
   - Openstack Swift + Swift3 middleware
   - Ceph Object Gateway
   - Riak CS

本文我们将学习如何安装Minio client SDK，连接到Minio，并提供一下文件上传的示例。对于完整的API以及示例，请参考[Go Client API Reference](https://docs.minio.io/docs/golang-client-api-reference)。

本文假设你已经有 [Go开发环境](https://docs.minio.io/docs/how-to-install-golang)。

## 从Github下载
```sh
go get -u github.com/minio/minio-go
```

## 初始化Minio Client
Minio client需要以下4个参数来连接与Amazon S3兼容的对象存储。

| 参数  | 描述| 
| :---         |     :---     |
| endpoint   | 对象存储服务的URL   | 
| accessKeyID | Access key是唯一标识你的账户的用户ID。 |   
| secretAccessKey | Secret key是你账户的密码。 |
| secure | true代表使用HTTPS |


```go
package main

import (
	"github.com/minio/minio-go"
	"log"
)

func main() {
	endpoint := "play.minio.io:9000"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true

	// 初使化 minio client对象。
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient初使化成功
}
```

## 示例-文件上传
本示例连接到一个对象存储服务，创建一个存储桶并上传一个文件到存储桶中。

我们在本示例中使用运行在 [https://play.minio.io:9000](https://play.minio.io:9000) 上的Minio服务，你可以用这个服务来开发和测试。示例中的访问凭据是公开的。

### FileUploader.go
```go
package main

import (
	"github.com/minio/minio-go"
	"log"
)

func main() {
	endpoint := "play.minio.io:9000"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true

	// 初使化minio client对象。
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	// 创建一个叫mymusic的存储桶。
	bucketName := "mymusic"
	location := "us-east-1"

	err = minioClient.MakeBucket(bucketName, location)
	if err != nil {
		// 检查存储桶是否已经存在。
		exists, err := minioClient.BucketExists(bucketName)
		if err == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)
		}
	}
	log.Printf("Successfully created %s\n", bucketName)

	// 上传一个zip文件。
	objectName := "golden-oldies.zip"
	filePath := "/tmp/golden-oldies.zip"
	contentType := "application/zip"

	// 使用FPutObject上传一个zip文件。
	n, err := minioClient.FPutObject(bucketName, objectName, filePath, minio.PutObjectOptions{ContentType:contentType})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)
}
```

### 运行FileUploader
```sh
go run file-uploader.go
2016/08/13 17:03:28 Successfully created mymusic 
2016/08/13 17:03:40 Successfully uploaded golden-oldies.zip of size 16253413

mc ls play/mymusic/
[2016-05-27 16:02:16 PDT]  17MiB golden-oldies.zip
```

## API文档
完整的API文档在这里。
* [完整API文档](https://docs.minio.io/docs/golang-client-api-reference)

### API文档 : 操作存储桶
* [`MakeBucket`](https://docs.minio.io/docs/golang-client-api-reference#MakeBucket)
* [`ListBuckets`](https://docs.minio.io/docs/golang-client-api-reference#ListBuckets)
* [`BucketExists`](https://docs.minio.io/docs/golang-client-api-reference#BucketExists)
* [`RemoveBucket`](https://docs.minio.io/docs/golang-client-api-reference#RemoveBucket)
* [`ListObjects`](https://docs.minio.io/docs/golang-client-api-reference#ListObjects)
* [`ListObjectsV2`](https://docs.minio.io/docs/golang-client-api-reference#ListObjectsV2)
* [`ListIncompleteUploads`](https://docs.minio.io/docs/golang-client-api-reference#ListIncompleteUploads)

### API文档 : 存储桶策略
* [`SetBucketPolicy`](https://docs.minio.io/docs/golang-client-api-reference#SetBucketPolicy)
* [`GetBucketPolicy`](https://docs.minio.io/docs/golang-client-api-reference#GetBucketPolicy)

### API文档 : 存储桶通知
* [`SetBucketNotification`](https://docs.minio.io/docs/golang-client-api-reference#SetBucketNotification)
* [`GetBucketNotification`](https://docs.minio.io/docs/golang-client-api-reference#GetBucketNotification)
* [`RemoveAllBucketNotification`](https://docs.minio.io/docs/golang-client-api-reference#RemoveAllBucketNotification)
* [`ListenBucketNotification`](https://docs.minio.io/docs/golang-client-api-reference#ListenBucketNotification) (Minio Extension)

### API文档 : 操作文件对象
* [`FPutObject`](https://docs.minio.io/docs/golang-client-api-reference#FPutObject)
* [`FGetObject`](https://docs.minio.io/docs/golang-client-api-reference#FPutObject)
* [`FPutObjectWithContext`](https://docs.minio.io/docs/golang-client-api-reference#FPutObjectWithContext)
* [`FGetObjectWithContext`](https://docs.minio.io/docs/golang-client-api-reference#FGetObjectWithContext)

### API文档 : 操作对象
* [`GetObject`](https://docs.minio.io/docs/golang-client-api-reference#GetObject)
* [`PutObject`](https://docs.minio.io/docs/golang-client-api-reference#PutObject)
* [`GetObjectWithContext`](https://docs.minio.io/docs/golang-client-api-reference#GetObjectWithContext)
* [`PutObjectWithContext`](https://docs.minio.io/docs/golang-client-api-reference#PutObjectWithContext)
* [`PutObjectStreaming`](https://docs.minio.io/docs/golang-client-api-reference#PutObjectStreaming)
* [`StatObject`](https://docs.minio.io/docs/golang-client-api-reference#StatObject)
* [`CopyObject`](https://docs.minio.io/docs/golang-client-api-reference#CopyObject)
* [`RemoveObject`](https://docs.minio.io/docs/golang-client-api-reference#RemoveObject)
* [`RemoveObjects`](https://docs.minio.io/docs/golang-client-api-reference#RemoveObjects)
* [`RemoveIncompleteUpload`](https://docs.minio.io/docs/golang-client-api-reference#RemoveIncompleteUpload)

### API文档: 操作加密对象
* [`GetEncryptedObject`](https://docs.minio.io/docs/golang-client-api-reference#GetEncryptedObject)
* [`PutEncryptedObject`](https://docs.minio.io/docs/golang-client-api-reference#PutEncryptedObject)

### API文档 : Presigned操作
* [`PresignedGetObject`](https://docs.minio.io/docs/golang-client-api-reference#PresignedGetObject)
* [`PresignedPutObject`](https://docs.minio.io/docs/golang-client-api-reference#PresignedPutObject)
* [`PresignedHeadObject`](https://docs.minio.io/docs/golang-client-api-reference#PresignedHeadObject)
* [`PresignedPostPolicy`](https://docs.minio.io/docs/golang-client-api-reference#PresignedPostPolicy)

### API文档 : 客户端自定义设置
* [`SetAppInfo`](http://docs.minio.io/docs/golang-client-api-reference#SetAppInfo)
* [`SetCustomTransport`](http://docs.minio.io/docs/golang-client-api-reference#SetCustomTransport)
* [`TraceOn`](http://docs.minio.io/docs/golang-client-api-reference#TraceOn)
* [`TraceOff`](http://docs.minio.io/docs/golang-client-api-reference#TraceOff)

## 完整示例

### 完整示例 : 操作存储桶
* [makebucket.go](https://github.com/minio/minio-go/blob/master/examples/s3/makebucket.go)
* [listbuckets.go](https://github.com/minio/minio-go/blob/master/examples/s3/listbuckets.go)
* [bucketexists.go](https://github.com/minio/minio-go/blob/master/examples/s3/bucketexists.go)
* [removebucket.go](https://github.com/minio/minio-go/blob/master/examples/s3/removebucket.go)
* [listobjects.go](https://github.com/minio/minio-go/blob/master/examples/s3/listobjects.go)
* [listobjectsV2.go](https://github.com/minio/minio-go/blob/master/examples/s3/listobjectsV2.go)
* [listincompleteuploads.go](https://github.com/minio/minio-go/blob/master/examples/s3/listincompleteuploads.go)

### 完整示例 : 存储桶策略
* [setbucketpolicy.go](https://github.com/minio/minio-go/blob/master/examples/s3/setbucketpolicy.go)
* [getbucketpolicy.go](https://github.com/minio/minio-go/blob/master/examples/s3/getbucketpolicy.go)
* [listbucketpolicies.go](https://github.com/minio/minio-go/blob/master/examples/s3/listbucketpolicies.go)
 
### 完整示例 : 存储桶通知
* [setbucketnotification.go](https://github.com/minio/minio-go/blob/master/examples/s3/setbucketnotification.go)
* [getbucketnotification.go](https://github.com/minio/minio-go/blob/master/examples/s3/getbucketnotification.go)
* [removeallbucketnotification.go](https://github.com/minio/minio-go/blob/master/examples/s3/removeallbucketnotification.go)
* [listenbucketnotification.go](https://github.com/minio/minio-go/blob/master/examples/minio/listenbucketnotification.go) (Minio扩展)

### 完整示例 : 操作文件对象
* [fputobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/fputobject.go)
* [fgetobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/fgetobject.go)
* [fputobject-context.go](https://github.com/minio/minio-go/blob/master/examples/s3/fputobject-context.go)
* [fgetobject-context.go](https://github.com/minio/minio-go/blob/master/examples/s3/fgetobject-context.go)

### 完整示例 : 操作对象
* [putobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/putobject.go)
* [getobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/getobject.go)
* [putobject-context.go](https://github.com/minio/minio-go/blob/master/examples/s3/putobject-context.go)
* [getobject-context.go](https://github.com/minio/minio-go/blob/master/examples/s3/getobject-context.go)
* [statobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/statobject.go)
* [copyobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/copyobject.go)
* [removeobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/removeobject.go)
* [removeincompleteupload.go](https://github.com/minio/minio-go/blob/master/examples/s3/removeincompleteupload.go)
* [removeobjects.go](https://github.com/minio/minio-go/blob/master/examples/s3/removeobjects.go)

### 完整示例 : 操作加密对象
* [put-encrypted-object.go](https://github.com/minio/minio-go/blob/master/examples/s3/put-encrypted-object.go)
* [get-encrypted-object.go](https://github.com/minio/minio-go/blob/master/examples/s3/get-encrypted-object.go)
* [fput-encrypted-object.go](https://github.com/minio/minio-go/blob/master/examples/s3/fputencrypted-object.go)

### 完整示例 : Presigned操作
* [presignedgetobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/presignedgetobject.go)
* [presignedputobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/presignedputobject.go)
* [presignedheadobject.go](https://github.com/minio/minio-go/blob/master/examples/s3/presignedheadobject.go)
* [presignedpostpolicy.go](https://github.com/minio/minio-go/blob/master/examples/s3/presignedpostpolicy.go)

## 了解更多
* [完整文档](https://docs.minio.io)
* [Minio Go Client SDK API文档](https://docs.minio.io/docs/golang-client-api-reference) 
* [Go 音乐播放器完整示例](https://docs.minio.io/docs/go-music-player-app)

## 贡献
[贡献指南](https://github.com/minio/minio-go/blob/master/docs/zh_CN/CONTRIBUTING.md)

[![Build Status](https://travis-ci.org/minio/minio-go.svg)](https://travis-ci.org/minio/minio-go)
[![Build status](https://ci.appveyor.com/api/projects/status/1d05e6nvxcelmrak?svg=true)](https://ci.appveyor.com/project/harshavardhana/minio-go)

