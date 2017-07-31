# Minio Erasure Code Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio protects data against hardware failures and silent data corruption using erasure code and checksums. You may lose  half the number (N/2) of drives and still be able to recover the data.

## What is Erasure Code?

Erasure code is a mathematical algorithm to reconstruct missing or corrupted data. Minio uses Reed-Solomon code to shard objects into N/2 data and N/2 parity blocks. This means that in a 12 drive setup, an object is sharded across as 6 data and 6 parity blocks. You can lose as many as 6 drives (be it parity or data) and still reconstruct the data reliably from the remaining drives.

## Why is Erasure Code useful?

Erasure code protects data from multiple drives failure unlike RAID or replication. For eg RAID6 can protect against 2 drive failure whereas in Minio erasure code you can lose as many as half number of drives and still the data remains safe. Further Minio's erasure code is at object level and can heal one object at a time. For RAID, healing can only be performed at volume level which translates into huge down time. As Minio encodes each object individually with a high parity count. Storage servers once deployed should not require drive replacement or healing for the lifetime of the server. Minio's erasure coded backend is designed for operational efficiency and takes full advantage of hardware acceleration whenever available.

![Erasure](https://github.com/minio/minio/blob/master/docs/screenshots/erasure-code.jpg?raw=true)

## What is Bit Rot protection?

Bit Rot also known as Data Rot or Silent Data Corruption is a serious data loss issue faced by disk drives today. Data on the drive may silently get corrupted without signalling an error has occurred. This makes Bit Rot more dangerous than permanent hard drive failure.

Minio's erasure coded backend uses high speed [BLAKE2](https://blog.minio.io/accelerating-blake2b-by-4x-using-simd-in-go-assembly-33ef16c8a56b#.jrp1fdwer) hash based checksums to protect against Bit Rot.  

## Get Started with Minio in Erasure Code 

### 1. Prerequisites:

Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)

### 2. Run Minio Server with Erasure Code.

Example: Start Minio server in a 12 drives setup, using Minio binary.

```sh
minio server /mnt/export1/backend /mnt/export2/backend /mnt/export3/backend /mnt/export4/backend /mnt/export5/backend /mnt/export6/backend /mnt/export7/backend /mnt/export8/backend /mnt/export9/backend /mnt/export10/backend /mnt/export11/backend /mnt/export12/backend
```

Example: Start Minio server in a 8 drives setup, using Minio Docker image. 

```sh
docker run -p 9000:9000 --name minio \
  -v /mnt/export1/backend:/export1 \
  -v /mnt/export2/backend:/export2 \
  -v /mnt/export3/backend:/export3 \
  -v /mnt/export4/backend:/export4 \
  -v /mnt/export5/backend:/export5 \
  -v /mnt/export6/backend:/export6 \
  -v /mnt/export7/backend:/export7 \
  -v /mnt/export8/backend:/export8 \
  minio/minio server /export1 /export2 /export3 /export4 /export5 /export6 /export7 /export8
```

### 3. Test your setup

You may unplug drives randomly and continue to perform I/O on the system.
