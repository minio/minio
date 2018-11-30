# Minio Storage Class Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio Server supports two storage classes in erasure coding mode: `Reduced Redundancy` class and `Standard` class. These classes enable Minio Server to save an object across a configurable number of data and parity drives.

This guide describes how to prepare for and use a storage class.

- [Calculate Drive Requirements and Usage](#calculate-drive-requirements-and-usage)
- [Get Started with Storage Class](#get-started-with-storage-class)

## <a name="calculate-drive-requirements-and-usage"></a>1. Calculate Drive Requirements and Usage

The number of data and parity drives has a direct impact on the drive space usage. A storage class can be used to optimize for high redundancy or better drive space utilization.

The following lists how different combinations of data and parity drives, affect storage usage for a 100 MB file stored on a 16-drive Minio deployment:
* **8 data and 8 parity drives**: the file space usage is approximately twice the size of the file (i.e. the 100 MB file will consume 200 MB of space).
* **8 data and 6 parity drives**: the file space usage for the 100 MB file is around 160 MB.
* **14 data and 2 parity drives**: the file space usage for the 100 MB file is approximately 114 MB.

Below is a list of data/parity drive configurations and the *approximate* storage space consumption on a 16-drive Minio deployment:

| **Total Drives (N)** | **Data Drives (D)** | **Parity Drives (P)** | **Storage Usage Ratio** |
|------------------|-----------------|-------------------|---------------------|
|               16 |               8 |                 8 |                2.00 |
|               16 |               9 |                 7 |                1.79 |
|               16 |              10 |                 6 |                1.60 |
|               16 |              11 |                 5 |                1.45 |
|               16 |              12 |                 4 |                1.34 |
|               16 |              13 |                 3 |                1.23 |
|               16 |              14 |                 2 |                1.14 |


The *storage usage* ratio is calculated as follows:
* **Storage Usage = Drive Space Used by the File After Erasure-Encoding / Actual File Size**

The *approximate storage usage* ratio is calculated as follows:
* **Approximate Storage Usage = Total Drives (N) / Data Drives (D)**

### 1.1 Values for the STANDARD Storage Class

The usage of the `STANDARD` storage class implies more parity than the `REDUCED_REDUNDANCY` class. When using the `STANDARD` storage class, parity drives should be:
* greater than or equal to 2, if `REDUCED_REDUNDANCY` parity is not set.
* greater than `REDUCED_REDUNDANCY` parity, if `REDUCED_REDUNDANCY` parity is set.

The number of parity blocks cannot be higher than data blocks, so the `STANDARD` storage class parity cannot be higher than N/2, where N is the total number of drives. The default value for `STANDARD` is N/2.

### 1.2 Values for the REDUCED_REDUNDANCY Storage Class

`REDUCED_REDUNDANCY` implies less parity than the `STANDARD` class. When using the `REDUCED_REDUNDANCY` storage class, parity drives should be:
* less than N/2, if `STANDARD` parity is not set.
* less than `STANDARD` parity, if `STANDARD` parity is set.

Since a parity of less than 2 is not recommended, the `REDUCED_REDUNDANCY` storage class does not support a 4-drive erasure coding configuration. The default value for the `REDUCED_REDUNDANCY` storage class is 2.

## <a name="get-started-with-storage-class"></a>2. Get Started with Storage Class

### 2.1 Set the Storage Class Using Environment Variables

Use the following format to set the storage class using environment variables:

`MINIO_STORAGE_CLASS_STANDARD=EC:parity`
`MINIO_STORAGE_CLASS_RRS=EC:parity`

The following example sets `MINIO_STORAGE_CLASS_RRS` to parity 2 and `MINIO_STORAGE_CLASS_STANDARD` to parity 3:

```sh
export MINIO_STORAGE_CLASS_STANDARD=EC:3
export MINIO_STORAGE_CLASS_RRS=EC:2
```

### 2.2 Set the Storage Class Using Minio Client

The storage class can also be set via `mc admin config` get/set commands to update the configuration. For more information see the [storage class](https://github.com/minio/minio/tree/master/docs/config#storage-class) documentation.

**Note:**
* If the `STANDARD` storage class is set via environment variables or `mc admin config` get/set commands, and `x-amz-storage-class` is not present in request metadata, Minio Server will apply the `STANDARD` storage class to the object. This means the data and parity drives will be used as set in the `STANDARD` storage class.
* If the storage class is not defined before starting Minio Server, and a subsequent `PutObject` metadata field has `x-amz-storage-class` present with the values `REDUCED_REDUNDANCY` or `STANDARD`, Minio Server will use the default parity values.

### 2.2 Example in Go Using `REDUCED_REDUNDANCY`

In the example below, `minio-go` is used to set the storage class to `REDUCED_REDUNDANCY` and split the object across 6 data drives and 2 parity drives:

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
