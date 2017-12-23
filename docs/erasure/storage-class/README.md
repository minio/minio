# Minio Storage Class Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio server supports storage class in erasure coding mode. This allows configurable data and parity disks per object.

## Overview

Minio supports two storage classes, Reduced Redundancy class and Standard class. These classes can be defined using environment variables
set before starting Minio server. After the data and parity disks for each storage class are defined using environment variables,
you can set the storage class of an object via request metadata field `x-amz-storage-class`. Minio server then honors the storage class by
saving the object in specific number of data and parity disks.

### Values for standard storage class (STANDARD)

`STANDARD` storage class implies more parity than `REDUCED_REDUNDANCY` class. So, `STANDARD` parity disks should be

- Greater than or equal to 2, if `REDUCED_REDUNDANCY` parity is not set.
- Greater than `REDUCED_REDUNDANCY` parity, if it is set.

Parity blocks can not be higher than data blocks, so `STANDARD` storage class parity can not be higher than N/2. (N being total number of disks)

Default value for `STANDARD` storage class is `N/2` (N is the total number of drives).

### Reduced redundancy storage class (REDUCED_REDUNDANCY)

`REDUCED_REDUNDANCY` implies lesser parity than `STANDARD` class. So,`REDUCED_REDUNDANCY` parity disks should be

- Less than N/2, if `STANDARD` parity is not set.
- Less than `STANDARD` Parity, if it is set.

As parity below 2 is not recommended, `REDUCED_REDUNDANCY` storage class is not supported for 4 disks erasure coding setup.

Default value for `REDUCED_REDUNDANCY` storage class is `2`.

## Get started with Storage Class

### Set storage class

The format to set storage class environment variables is as follows

`MINIO_STORAGE_CLASS_STANDARD=EC:parity`
`MINIO_STORAGE_CLASS_RRS=EC:parity`

For example, set `MINIO_STORAGE_CLASS_RRS` parity 2 and `MINIO_STORAGE_CLASS_STANDARD` parity 3

```sh
export MINIO_STORAGE_CLASS_STANDARD=EC:3
export MINIO_STORAGE_CLASS_RRS=EC:2
```

If storage class is not defined before starting Minio server, and subsequent PutObject metadata field has `x-amz-storage-class` present
with values `REDUCED_REDUNDANCY` or `STANDARD`, Minio server uses default parity values.

### Set metadata

In below example `minio-go` is used to set the storage class to `REDUCED_REDUNDANCY`. This means this object will be split across 6 data disks and 2 parity disks (as per the storage class set in previous step).

```go
s3Client, err := minio.New("localhost:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
if err != nil {
	log.Fatalln(err)
}

object, err := os.Open("my-testfile")
if err != nil {
	log.Fatalln(err)
}
defer object.Close()
objectStat, err := object.Stat()
if err != nil {
	log.Fatalln(err)
}

n, err := s3Client.PutObject("my-bucketname", "my-objectname", object, objectStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream", StorageClass: "REDUCED_REDUNDANCY"})
if err != nil {
	log.Fatalln(err)
}
log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
```