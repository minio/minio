# Shared Backend Minio Quickstart Guide [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio now supports shared backend across multiple instances.  This solves certain specific use cases.

## Use Cases

- Minio on NAS
- Minio on Distributed Filesystems
- Multi-user Shared Backend.

## Why Minio On Shared Backend?

Minio server on shared backend can help you setup a highly-available storage system. Shared backend allows Minio server to behave like an S3 gateway on top your existing NAS infrastructure. To increase the throughput of the system any new Minio server can be spawned on demand.

NOTE: Minio is only as fast as your shared NAS device.

# Get started

If you're aware of stand-alone Minio set up, the process remains largely the same.

## 1. Prerequisites

Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio).

## 2. Run Minio On Shared Backend

Below examples will clarify further for each operating system of your choice:

### Ubuntu 16.04 LTS

Example 1: Start Minio instance on a shared backend mounted and available at `/mnt/nfs`.

On linux server1
```shell
minio server /mnt/nfs
```

On linux server2
```shell
minio server /mnt/nfs
```

### Windows 2012 Server

Example 1: Start Minio instance on a shared backend mounted and available at `\\remote-server\smb`.

On windows server1
```cmd
minio.exe server \\remote-server\smb\export
```

On windows server2
```cmd
minio.exe server \\remote-server\smb\export
```

Alternatively if `\\remote-server\smb` is mounted as `D:\` drive.

On windows server1
```cmd
minio.exe server D:\export
```

On windows server2
```cmd
minio.exe server D:\export
```

## 3. Test your setup

To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the node2 endpoint as well.

## Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)


