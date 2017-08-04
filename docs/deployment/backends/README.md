# Minio Server Backends [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio server is generally backend agnostic and can be deployed on various backends without any problems. However, there are different backends we recommend for optimal operation. In this document we discuss about all such backends.

## File System

Minio server runs on a POSIX compatible filesystem like `XFS`,`EXT4` and `NTFS`. To run Minio on a filesystem backend, download Minio server binary and run as

```sh
minio server /path/to/filesystem/volume
```

Read more about running Minio server with filesystem backend in [Minio quickstart guide](https://docs.minio.io/docs/minio-quickstart-guide).

## Network Attached Storage

You can use a POSIX compatible NAS or distributed filesystem such as `Linux NFS`, `GlusterFS`, `Isilon`, `VNX` and `NetApp NFS` to run Minio server. To run Minio on a NAS backend, run as

```sh
minio server /path/to/NAS
```

Minio also allows running multiple server instances backed by a single NAS. This will help you handle high network traffic. Read more on Minio shared-backend in [Minio shared-mode guide](https://github.com/minio/minio/blob/master/docs/shared-backend/README.md).

## JBOD (Just a Bunch of Disks)

Minio server can also run on JBOD attached to a single server. When you have 4 or more (and less than 16) disks attached to your server, Minio automatically enables erasure code to protect data from drive failures and silent data corruption. To run Minio with JBOD, run as

```sh
minio server /path/to/disk1 /path/to/disk2 /path/to/disk3 /path/to/disk4
```

Read more on [Minio erasure coding quickstart guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide).

## Distributed JBOD

If you have multiple JBODs distributed across a network, you can use them as Minio server backend. With distributed JBOD, you also get protection against specific node failures, in addition to data protection from by erasure coding. To run Minio with distributed JBOD, run as 

```sh
minio server /path/to/node1/disk1 /path/to/node2/disk2 /path/to/node3/disk3 /path/to/node4/disk4
```

Read more on [Minio distributed quickstart guide](https://docs.minio.io/docs/distributed-minio-quickstart-guide).

## Cloud Storage Platforms

Minio server also allows using cloud storage platforms like Microsoft Azure and AWS S3 as storage backend for your Minio server. To Minio in gateway mode, run as

```sh
minio gateway azure
```

Read more on [Minio gateway guide](https://docs.minio.io/docs/minio-cloud-storage-gateway).
