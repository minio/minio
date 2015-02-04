# Minio API

## General Overview

Minio stores and retrieves data in a logical format based upon REST
based URLs.

### Note about examples:

XML and JSON results have been prettified for readability. As a result, the Content-Length may not match exactly.
```
Form:
http://minio.example.com/{bucket}/{path:.*}

Examples:
http://minio.example.com/bucket/object
http://minio.example.com/bucket/path/to/object
http://minio.example.com/bucket2/path/to/object
```

## GET /

List buckets accessible by the user.

The default output is XML. JSON output may also be requested by adding the following header:

```
Accept: "application/json"
```

Example:
```
GET / HTTP/1.1
```
```
HTTP/1.1 200 OK
Connection: close
Content-Type: application/xml
Server: Minio
Date: Mon, 02 Feb 2015 22:09:00 GMT
Content-Length: 306

<ListAllMyBucketsResult>
  <Owner>
    <ID>minio</ID>
    <DisplayName>minio</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>bucket</Name>
      <CreationDate>2015-01-30T15:20:09.013Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>minio</Name>
      <CreationDate>2015-01-27T17:46:28.264Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
```

```
GET / HTTP/1.1
Accept: application/json
```

```
HTTP/1.1 200 OK
Connection: close
Content-Type: application/json
Server: Minio
Date: Wed, 04 Feb 2015 21:59:10 GMT
Content-Length: 223

{
   "Owner" : {
      "ID" : "minio"
      "DisplayName" : "minio",
   },
   "Buckets" : {
      "Bucket" : [
         {
            "Name" : "bucket",
            "CreationDate" : "2015-01-30T15:20:09.013Z"
         },
         {
            "Name" : "minio",
            "CreationDate" : "2015-02-02T14:52:34.914Z"
         }
      ]
   }
}
```

## GET /{bucket}/

Lists objects in a bucket.


Example:
```
GET /minio/ HTTP/1.1
```
```
HTTP/1.1 200 OK
Connection: close
Content-Type: application/xml
Server: Minio
Date: Tue, 03 Feb 2015 00:57:59 GMT
Content-Length: 579

<?xml version="1.0"?>
<ListBucketResult>
  <Name>minio</Name>
  <Marker/>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>hello</Key>
    <LastModified>2015-02-02T14:52:34.914Z</LastModified>
    <ETag>minio#hello</ETag>
    <Size>75</Size>
    <StorageClass>STANDARD</StorageClass>
    <Owner>
      <ID>minio</ID>
      <DisplayName>minio</DisplayName>
    </Owner>
  </Contents>
  <Contents>
    <Key>one</Key>
    <LastModified>2015-01-27T17:46:28.264Z</LastModified>
    <ETag>minio#one</ETag>
    <Size>4096</Size>
    <StorageClass>STANDARD</StorageClass>
    <Owner>
      <ID>minio</ID>
      <DisplayName>minio</DisplayName>
    </Owner>
  </Contents>
</ListBucketResult>
```

## PUT /{bucket}/

Example:
```
PUT /books/ HTTP/1.1
```
```
HTTP/1.1 200 OK
Connection: close
Server: Minio
Date: Mon, 02 Feb 2015 22:05:43 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

EXAMPLE
## GET /{bucket}/{object}

```
GET /minio/hello HTTP/1.1
```
```
HTTP/1.1 200 OK
Connection: close
Content-Length: 75
Content-Type: text/plain
Etag: minio#hello
Last-Modified: Mon, 02 Feb 2015 14:52:34 PST
Server: Minio
Date: Mon, 02 Feb 2015 22:59:51 GMT

<?xml version="1.0"?>
<html>
  <head/>
  <body>Hello World!</body>
</html>
```

Retrieves an object from a bucket

## HEAD /{bucket}/{object}
```
HEAD /minio/hello HTTP/1.1
```
```
HTTP/1.1 200 OK
Connection: close
Content-Length: 75
Content-Type: text/plain
Etag: minio#hello
Last-Modified: Mon, 02 Feb 2015 14:52:34 PST
Server: Minio
Date: Mon, 02 Feb 2015 23:02:30 GMT
```

Retrieves meta-data about an object

## PUT /{bucket}/{object}

Stores an object

```
PUT /minio/hello HTTP/1.1
Content-Length: 75

<?xml version="1.0"?>
<html>
  <head/>
  <body>Hello World!</body>
</html>
```
```
HTTP/1.1 200 OK
```
