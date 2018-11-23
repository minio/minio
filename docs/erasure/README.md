# Minio Erasure Code Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio protects data against hardware failures and silent data corruption using erasure code and checksums. This provides the highest level of redundancy enabling data to be recovered when up to half (N/2) of the storage drives are lost.

This guide describes what Minio erasure code is and how to use it.

1. [What is Erasure Code?](#what-is-erasure-code) 
2. [Why is Erasure Code Useful?](#why-is-erasure-code-useful) 
3. [What is Bit Rot Protection?](#what-is-bit-rot-protection) 
4. [Get Started with Minio Erasure Code](#get-started-with-minio-erasure-code)

## 1. <a name="what-is-erasure-code"></a>What is Erasure Code?

*Erasure code* is a mathematical algorithm to reconstruct missing or corrupted data. Minio uses Reed-Solomon code to shard objects into variable data and parity blocks. 

By default, Minio shards the objects across N/2 data and N/2 parity drives: 

![Erasure](https://github.com/minio/minio/blob/master/docs/screenshots/erasure-code.jpg?raw=true)

**Note:** The [storage classes](https://github.com/minio/minio/tree/master/docs/erasure/storage-class) can be used for a custom configuration. N/2 data and parity blocks are recommended to ensure the best protection from drive failures.

For example, in a 12-drive configuration, an object can be sharded into a variable number of data and parity blocks across all the drives, ranging from 6-10 data blocks and 2-6 parity blocks. When running Minio Server with its default configuration, up to 6 drives can fail while data can still be reliably reconstructed from the remaining drives.

## <a name="why-is-erasure-code-useful"></a>2. Why is Erasure Code Useful?

Unlike RAID or replication backup configurations, erasure code protects data from multiple drive failures. For example, RAID6 can protect against two drive failures, whereas Minio erasure code can retain data for as many as half of the drives that are lost. 

Minio's erasure code operates at the object level and can heal one object at a time. With RAID, healing can only be performed at the volume level, which can result in long downtime. Since Minio encodes each object individually, it can heal objects incrementally. Storage servers should not require drive replacement or healing for the lifetime of the server. Minio's erasure-coded backend is designed for operational efficiency and takes full advantage of hardware acceleration when available.

## <a name="what-is-bit-rot-protection"></a>3. What is Bit Rot Protection?

*Bit rot*, also known as *data rot* or *silent data* corruption, is a data loss issue that occurs on modern hard drives. Data on the drive may become corrupted silently without signaling that an error has occurred, making bit rot more dangerous than a permanent hard drive failure.

Minio's erasure-coded backend uses high-speed [HighwayHash](https://blog.minio.io/highwayhash-fast-hashing-at-over-10-gb-s-per-core-in-golang-fee938b5218a) checksums to protect against bit rot.

## <a name="get-started-with-minio-erasure-code"></a>4. Get Started with Minio Erasure Code

### 4.1 Install Minio Server

Install Minio Server using the instructions in the [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

### 4.2 Examples of Minio Server with Erasure Code

#### Create a 12-drive configuration using the `minio` binary

```sh
minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8 /data9 /data10 /data11 /data12
```

A response similar to this one should be displayed:
```
Status:    4 Online, 0 Offline. 
Endpoint:  http://192.168.0.15:9000  http://127.0.0.1:9000
AccessKey: ZOVRF2ZDFNTNW1IYTK7K 
SecretKey: 659BKtAxYT6P_SkBlQPJude0IgUb13IBkBH_MW3X 

Browser Access:
   http://192.168.0.15:9000  http://127.0.0.1:9000

Command-line Access: https://docs.minio.io/docs/minio-client-quickstart-guide
   $ mc config host add myminio http://192.168.0.15:9000 ZOVRF2ZDFNTNW1IYTK7K 659BKtAxYT6P_SkBlQPJude0IgUb13IBkBH_MW3X

...
```

The first line indicates the status of the hard drives that are online and offline.

#### Create an eight-drive configuration using a Minio Docker image

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

### 4.3 Test the Configuration

After starting Minio Server using the examples in the previous section, unplug hard drives randomly from the server and continue to perform I/O operations against Minio Server. Minio Server should continue to operate normally, and all data stored and retrieved from Minio Server should remain intact.
