## API Documentation

### Minio client object creation
Minio client object is created using minio-go:
```go
package main

import (
    "fmt"

    "github.com/minio/minio-go"
)

func main() {
    s3Client, err := minio.New("s3.amazonaws.com", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", false)
    if err !!= nil {
        fmt.Println(err)
        return
    }
}
```

s3Client can be used to perform operations on S3 storage. APIs are described below.

### Bucket operations

* [`MakeBucket`](#MakeBucket)
* [`ListBuckets`](#ListBuckets)
* [`BucketExists`](#BucketExists)
* [`RemoveBucket`](#RemoveBucket)
* [`ListObjects`](#ListObjects)
* [`ListIncompleteUploads`](#ListIncompleteUploads)

### Object operations

* [`GetObject`](#GetObject)
* [`PutObject`](#PutObject)
* [`StatObject`](#StatObject)
* [`RemoveObject`](#RemoveObject)
* [`RemoveIncompleteUpload`](#RemoveIncompleteUpload)

### File operations.

* [`FPutObject`](#FPutObject)
* [`FGetObject`](#FPutObject)

### Bucket policy operations.

* [`SetBucketPolicy`](#SetBucketPolicy)
* [`GetBucketPolicy`](#GetBucketPolicy)
* [`RemoveBucketPolicy`](#RemoveBucketPolicy)

### Presigned operations

* [`PresignedGetObject`](#PresignedGetObject)
* [`PresignedPutObject`](#PresignedPutObject)
* [`PresignedPostPolicy`](#PresignedPostPolicy)

### Bucket operations
---------------------------------------
<a name="MakeBucket">
#### MakeBucket(bucketName, location)
Create a new bucket.

__Arguments__
* `bucketName` _string_ - Name of the bucket.
* `location` _string_ - region valid values are _us-west-1_, _us-west-2_,  _eu-west-1_, _eu-central-1_, _ap-southeast-1_, _ap-northeast-1_, _ap-southeast-2_, _sa-east-1_

__Example__
```go
err := s3Client.MakeBucket("mybucket", "us-west-1")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println("Successfully created mybucket.")
```
---------------------------------------
<a name="ListBuckets">
#### ListBuckets()
List all buckets.

`bucketList` emits bucket with the format:
* `bucket.Name` _string_: bucket name
* `bucket.CreationDate` time.Time : date when bucket was created

__Example__
```go
buckets, err := s3Client.ListBuckets()
if err != nil {
    fmt.Println(err)
    return
}
for _, bucket := range buckets {
    fmt.Println(bucket)
}
```
---------------------------------------
<a name="BucketExists">
#### BucketExists(bucketName)
Check if bucket exists.

__Arguments__
* `bucketName` _string_ : name of the bucket

__Example__
```go
err := s3Client.BucketExists("mybucket")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="RemoveBucket">
#### RemoveBucket(bucketName)
Remove a bucket.

__Arguments__
* `bucketName` _string_ : name of the bucket

__Example__
```go
err := s3Client.RemoveBucket("mybucket")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="GetBucketPolicy">
#### GetBucketPolicy(bucketName, objectPrefix)
Get access permissions on a bucket or a prefix.

__Arguments__
* `bucketName` _string_ : name of the bucket
* `objectPrefix` _string_ : name of the object prefix

__Example__
```go
bucketPolicy, err := s3Client.GetBucketPolicy("mybucket")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println("Access permissions for mybucket is", bucketPolicy)
```
---------------------------------------
<a name="SetBucketPolicy">
#### SetBucketPolicy(bucketname, objectPrefix, policy)
Set access permissions on bucket or an object prefix.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectPrefix` _string_ : name of the object prefix
* `policy` _BucketPolicy_: policy can be _non_, _readonly_, _readwrite_, _writeonly_

__Example__
```go
err := s3Client.SetBucketPolicy("mybucket", "myprefix", "readwrite")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="RemoveBucketPolicy">
#### RemoveBucketPolicy(bucketname, objectPrefix)
Remove existing permissions on bucket or an object prefix.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectPrefix` _string_ : name of the object prefix

__Example__
```go
err := s3Client.RemoveBucketPolicy("mybucket", "myprefix")
if err != nil {
    fmt.Println(err)
    return
}
```

---------------------------------------
<a name="ListObjects">
#### ListObjects(bucketName, prefix, recursive, doneCh)
List objects in a bucket.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectPrefix` _string_: the prefix of the objects that should be listed
* `recursive` _bool_: `true` indicates recursive style listing and `false` indicates directory style listing delimited by '/'
* `doneCh`   chan struct{} : channel for pro-actively closing the internal go routine

__Return Value__
* `<-chan ObjectInfo` _chan ObjectInfo_: Read channel for all the objects in the bucket, the object is of the format:
  * `objectInfo.Key` _string_: name of the object
  * `objectInfo.Size` _int64_: size of the object
  * `objectInfo.ETag` _string_: etag of the object
  * `objectInfo.LastModified` _time.Time_: modified time stamp

__Example__
```go
// Create a done channel to control 'ListObjects' go routine.
doneCh := make(chan struct{})

// Indicate to our routine to exit cleanly upon return.
defer close(doneCh)

isRecursive := true
objectCh := s3Client.ListObjects("mybucket", "myprefix", isRecursive, doneCh)
for object := range objectCh {
    if object.Err != nil {
        fmt.Println(object.Err)
        return
    }
    fmt.Println(object)
}

```

---------------------------------------
<a name="ListIncompleteUploads">
#### ListIncompleteUploads(bucketName, prefix, recursive)
List partially uploaded objects in a bucket.

__Arguments__
* `bucketname` _string_: name of the bucket
* `prefix` _string_: prefix of the object names that are partially uploaded
* `recursive` bool: directory style listing when false, recursive listing when true
* `doneCh`   chan struct{} : channel for pro-actively closing the internal go routine

__Return Value__
* `<-chan ObjectMultipartInfo` _chan ObjectMultipartInfo_ : emits multipart objects of the format:
  * `multiPartObjInfo.Key` _string_: name of the incomplete object
  * `multiPartObjInfo.UploadID` _string_: upload ID of the incomplete object
  * `multiPartObjInfo.Size` _int64_: size of the incompletely uploaded object

__Example__
```go
// Create a done channel to control 'ListObjects' go routine.
doneCh := make(chan struct{})

// Indicate to our routine to exit cleanly upon return.
defer close(doneCh)

isRecursive := true
multiPartObjectCh := s3Client.ListIncompleteUploads("mybucket", "myprefix", isRecursive, doneCh)
for multiPartObject := range multiPartObjectCh {
    if multiPartObject.Err != nil {
        fmt.Println(multiPartObject.Err)
        return
    }
    fmt.Println(multiPartObject)
}
```

---------------------------------------
### Object operations
<a name="GetObject">
#### GetObject(bucketName, objectName)
Download an object.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object

__Return Value__
* `object` _*minio.Object_ : _minio.Object_ represents object reader.

__Example__
```go
object, err := s3Client.GetObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
localFile _ := os.Open("/tmp/local-file")
if _, err := io.Copy(localFile, object); err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
---------------------------------------
<a name="FGetObject">
#### FGetObject(bucketName, objectName, filePath)
Callback is called with `error` in case of error or `null` in case of success

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object
* `filePath` _string_: path to which the object data will be written to

__Example__
```go
err := s3Client.FGetObject("mybucket", "photo.jpg", "/tmp/photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="PutObject">
#### PutObject(bucketName, objectName, reader, contentType)
Upload an object.

Uploading a stream
__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object
* `reader` _io.Reader_: Any golang object implementing io.Reader
* `contentType` _string_: content type of the object.

__Example__
```go
file, err := os.Open("my-testfile")
if err != nil {
	fmt.Println(err)
    return
}
defer file.Close()

n, err := s3Client.PutObject("my-bucketname", "my-objectname", object, "application/octet-stream")
if err != nil {
    fmt.Println(err)
    return
}
```

---------------------------------------
<a name="FPutObject">
#### FPutObject(bucketName, objectName, filePath, contentType)
Uploads the object using contents from a file

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object
* `filePath` _string_: file path of the file to be uploaded
* `contentType` _string_: content type of the object

__Example__
```go
n, err := s3Client.FPutObject("my-bucketname", "my-objectname", "/tmp/my-filename.csv", "application/csv")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="StatObject">
#### StatObject(bucketName, objectName)
Get metadata of an object.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object

__Return Value__
   `objInfo`   _ObjectInfo_ : object stat info for following format:
  * `objInfo.Size` _int64_: size of the object
  * `objInfo.ETag` _string_: etag of the object
  * `objInfo.ContentType` _string_: Content-Type of the object
  * `objInfo.LastModified` _string_: modified time stamp

__Example__
```go
objInfo, err := s3Client.StatObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(objInfo)
```
---------------------------------------
<a name="RemoveObject">
#### RemoveObject(bucketName, objectName)
Remove an object.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object

__Example__
```go
err := s3Client.RemoveObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```
---------------------------------------
<a name="RemoveIncompleteUpload">
#### RemoveIncompleteUpload(bucketName, objectName)
Remove an partially uploaded object.

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object

__Example__
```go
err := s3Client.RemoveIncompleteUpload("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```

### Presigned operations
---------------------------------------
<a name="PresignedGetObject">
#### PresignedGetObject(bucketName, objectName, expiry)
Generate a presigned URL for GET.

__Arguments__
* `bucketName` _string_: name of the bucket.
* `objectName` _string_: name of the object.
* `expiry` _time.Duration_: expiry in seconds.
  `reqParams` _url.Values_ : additional response header overrides supports _response-expires_, _response-content-type_, _response-cache-control_, _response-content-disposition_

__Example__
```go
// Set request parameters for content-disposition.
reqParams := make(url.Values)
reqParams.Set("response-content-disposition", "attachment; filename=\"your-filename.txt\"")

// Generates a presigned url which expires in a day.
presignedURL, err := s3Client.PresignedGetObject("mybucket", "photo.jpg", time.Second * 24 * 60 * 60, reqParams)
if err != nil {
    fmt.Println(err)
    return
}
```

---------------------------------------
<a name="PresignedPutObject">
#### PresignedPutObject(bucketName, objectName, expiry)
Generate a presigned URL for PUT.
<blockquote>
NOTE: you can upload to S3 only with specified object name.
</blockquote>

__Arguments__
* `bucketName` _string_: name of the bucket
* `objectName` _string_: name of the object
* `expiry` _time.Duration_: expiry in seconds

__Example__
```go
// Generates a url which expires in a day.
presignedURL, err := s3Client.PresignedPutObject("mybucket", "photo.jpg", time.Second * 24 * 60 * 60)
if err != nil {
    fmt.Println(err)
    return
}
```

---------------------------------------
<a name="PresignedPostPolicy">
#### PresignedPostPolicy
PresignedPostPolicy we can provide policies specifying conditions restricting
what you want to allow in a POST request, such as bucket name where objects can be
uploaded, key name prefixes that you want to allow for the object being created and more.

We need to create our policy first:
```go
policy := minio.NewPostPolicy()
```
Apply upload policy restrictions:
```go
policy.SetBucket("my-bucketname")
policy.SetKey("my-objectname")
policy.SetExpires(time.Now().UTC().AddDate(0, 0, 10)) // expires in 10 days

// Only allow 'png' images.
policy.SetContentType("image/png")

// Only allow content size in range 1KB to 1MB.
policy.SetContentLengthRange(1024, 1024*1024)
```
Get the POST form key/value object:
```go
formData, err := s3Client.PresignedPostPolicy(policy)
if err != nil {
    fmt.Println(err)
    return
}
```

POST your content from the command line using `curl`:
```go
fmt.Printf("curl ")
for k, v := range m {
    fmt.Printf("-F %s=%s ", k, v)
}
fmt.Printf("-F file=@/etc/bash.bashrc ")
fmt.Printf("https://my-bucketname.s3.amazonaws.com\n")
```
