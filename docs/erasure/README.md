# Minio Erasure Code Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio protects data against hardware failures and silent data corruption using erasure code and checksums. With the highest level of redundancy, you may lose up to half (N/2) of the total drives and still be able to recover the data.

## What is Erasure Code?

Erasure code is a mathematical algorithm to reconstruct missing or corrupted data. Minio uses Reed-Solomon code to shard objects into variable data and parity blocks. For example, in a 12 drive setup, an object can be sharded to a variable number of data and parity blocks across all the drives - ranging from six data and six parity blocks to ten data and two parity blocks.

By default, Minio shards the objects across N/2 data and N/2 parity drives. Though, you can use [storage classes](https://github.com/minio/minio/tree/master/docs/erasure/storage-class) to use a custom configuration. We recommend N/2 data and parity blocks, as it ensures the best protection from drive failures.

In 12 drive example above, with Minio server running in the default configuration, you can lose any of the six drives and still reconstruct the data reliably from the remaining drives.

## Why is Erasure Code useful?

Erasure code protects data from multiple drives failure, unlike RAID or replication. For example, RAID6 can protect against two drive failure whereas in Minio erasure code you can lose as many as half of drives and still the data remains safe. Further, Minio's erasure code is at the object level and can heal one object at a time. For RAID, healing can be done only at the volume level which translates into high downtime. As Minio encodes each object individually, it can heal objects incrementally. Storage servers once deployed should not require drive replacement or healing for the lifetime of the server. Minio's erasure coded backend is designed for operational efficiency and takes full advantage of hardware acceleration whenever available.

![Erasure](https://github.com/minio/minio/blob/master/docs/screenshots/erasure-code.jpg?raw=true)

## What is Bit Rot protection?

Bit Rot, also known as data rot or silent data corruption is a data loss issue faced by disk drives today. Data on the drive may silently get corrupted without signalling an error has occurred, making bit rot more dangerous than a permanent hard drive failure.

Minio's erasure coded backend uses high speed [HighwayHash](https://blog.minio.io/highwayhash-fast-hashing-at-over-10-gb-s-per-core-in-golang-fee938b5218a) checksums to protect against Bit Rot.

## Get Started with Minio in Erasure Code

### 1. Prerequisites

Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)

### 2. Run Minio Server with Erasure Code

Example: Start Minio server in a 12 drives setup, using Minio binary.

```sh
minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8 /data9 /data10 /data11 /data12
```

Example: Start Minio server in a 8 drives setup, using Minio Docker image. 

```sh
docker run -p 9000:9000 --name minio \
  -v /mnt/data1:/data1 \
  -v /mnt/data2:/data2 \
  -v /mnt/data3:/data3 \
  -v /mnt/data4:/data4 \
  -v /mnt/data5:/data5 \
  -v /mnt/data6:/data6 \
  -v /mnt/data7:/data7 \
  -v /mnt/data8:/data8 \
  minio/minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8
```

### 3. Test your setup

You may unplug drives randomly and continue to perform I/O on the system.
