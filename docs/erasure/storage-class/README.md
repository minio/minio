# MinIO Storage Class Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server supports storage class in erasure coding mode. This allows configurable data and parity drives per object.

This page is intended as a summary of MinIO Erasure Coding. For a more complete explanation, see <https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html>.

## Overview

MinIO supports two storage classes, Reduced Redundancy class and Standard class. These classes can be defined using environment variables
set before starting MinIO server. After the data and parity drives for each storage class are defined using environment variables,
you can set the storage class of an object via request metadata field `x-amz-storage-class`. MinIO server then honors the storage class by
saving the object in specific number of data and parity drives.

## Storage usage

The selection of varying data and parity drives has a direct impact on the drive space usage. With storage class, you can optimize for high
redundancy or better drive space utilization.

To get an idea of how various combinations of data and parity drives affect the storage usage, let’s take an example of a 100 MiB file stored
on 16 drive MinIO deployment. If you use eight data and eight parity drives, the file space usage will be approximately twice, i.e. 100 MiB
file will take 200 MiB space. But, if you use ten data and six parity drives, same 100 MiB file takes around 160 MiB. If you use 14 data and
two parity drives, 100 MiB file takes only approximately 114 MiB.

Below is a list of data/parity drives and corresponding _approximate_ storage space usage on a 16 drive MinIO deployment. The field _storage
usage ratio_ is simply the drive space used by the file after erasure-encoding, divided by actual file size.

| Total Drives (N) | Data Drives (D) | Parity Drives (P) | Storage Usage Ratio |
|------------------|-----------------|-------------------|---------------------|
|               16 |               8 |                 8 |                2.00 |
|               16 |               9 |                 7 |                1.79 |
|               16 |              10 |                 6 |                1.60 |
|               16 |              11 |                 5 |                1.45 |
|               16 |              12 |                 4 |                1.34 |
|               16 |              13 |                 3 |                1.23 |
|               16 |              14 |                 2 |                1.14 |

You can calculate _approximate_ storage usage ratio using the formula - total drives (N) / data drives (D).

### Allowed values for STANDARD storage class

`STANDARD` storage class implies more parity than `REDUCED_REDUNDANCY` class. So, `STANDARD` parity drives should be

- Greater than or equal to 2, if `REDUCED_REDUNDANCY` parity is not set.
- Greater than `REDUCED_REDUNDANCY` parity, if it is set.

Parity blocks can not be higher than data blocks, so `STANDARD` storage class parity can not be higher than N/2. (N being total number of drives)

The default value for the `STANDARD` storage class depends on the number of volumes in the erasure set:

| Erasure Set Size | Default Parity (EC:N) |
|------------------|-----------------------|
| 5 or fewer       |                 EC:2  |
| 6-7              |                 EC:3  |
| 8 or more        |                 EC:4  |

For more complete documentation on Erasure Set sizing, see the [MinIO Documentation on Erasure Sets](https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html#erasure-sets).

### Allowed values for REDUCED_REDUNDANCY storage class

`REDUCED_REDUNDANCY` implies lesser parity than `STANDARD` class. So,`REDUCED_REDUNDANCY` parity drives should be

- Less than N/2, if `STANDARD` parity is not set.
- Less than `STANDARD` Parity, if it is set.

Default value for `REDUCED_REDUNDANCY` storage class is `1`.

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

Storage class can also be set via `mc admin config` get/set commands to update the configuration. Refer [storage class](https://github.com/minio/minio/tree/master/docs/config#storage-class) for
more details.

#### Note

- If `STANDARD` storage class is set via environment variables or `mc admin config` get/set commands, and `x-amz-storage-class` is not present in request metadata, MinIO server will
apply `STANDARD` storage class to the object. This means the data and parity drives will be used as set in `STANDARD` storage class.

- If storage class is not defined before starting MinIO server, and subsequent PutObject metadata field has `x-amz-storage-class` present
with values `REDUCED_REDUNDANCY` or `STANDARD`, MinIO server uses default parity values.

### Set metadata

In below example `minio-go` is used to set the storage class to `REDUCED_REDUNDANCY`. This means this object will be split across 6 data drives and 2 parity drives (as per the storage class set in previous step).

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
