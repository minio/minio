# Minio Storage Class Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio server supports storage class in erasure coding mode. This allows configurable data and parity disks per object.

## Overview

Minio supports two storage classes, Reduced Redundancy class and Standard class. These classes can be defined using environment variables
set before starting Minio server. After the data and parity disks for each storage class are defined using environment variables,
you can set the storage class of an object via request metadata field `x-amz-storage-class`. Minio server then honors the storage class by
saving the object in specific number of data and parity disks.

### Values for standard storage class (SS)

Standard storage class implies more parity than RRS class. So, SS parity disks should be

- Greater than or equal to 2, if RRS parity is not set.
- Greater than RRS parity, if it is set.

As parity blocks can not be higher than data blocks, Standard storage class can not be higher than N/2. (N being total number of disks)

Default value for standard storage class is `N/2` (N is the total number of drives).

### Reduced redundancy storage class (RRS)

Reduced redundancy implies lesser parity than SS class. So, RRS parity disks should be

- Less than N/2, if SS parity is not set.
- Less than SS Parity, if it is set.

As parity below 2 is not recommended, RR storage class is not supported for 4 disks erasure coding setup.

Default value for reduced redundancy storage class is `2`.

## Get started with Storage Class

### Set storage class

The format to set storage class environment variables is as follows

`MINIO_STORAGE_CLASS_RRS=EC:parity`
`MINIO_STORAGE_CLASS_STANDARD=EC:parity`

For example, set RRS parity 2 and SS parity 3, in 8 disk erasure code setup.

```sh
export MINIO_STORAGE_CLASS_RRS=EC:2
export MINIO_STORAGE_CLASS_STANDARD=EC:3
```

If storage class is not defined before starting Minio server, and subsequent PutObject metadata field has `x-amz-storage-class` present
with values `MINIO_STORAGE_CLASS_RRS` or `MINIO_STORAGE_CLASS_STANDARD`, Minio server uses default parity values.

### Set metadata

In below example `minio-go` is used to set the storage class to `MINIO_STORAGE_CLASS_RRS`. This means this object will be split across 6 data disks and 2 parity disks (as per the storage class set in previous step).

```go
s3Client, err := minio.New("s3.amazonaws.com", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
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

n, err := s3Client.PutObject("my-bucketname", "my-objectname", object, objectStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream", StorageClass: "MINIO_STORAGE_CLASS_RRS"})
if err != nil {
	log.Fatalln(err)
}
log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
```