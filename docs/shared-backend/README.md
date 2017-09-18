# Shared Backend Minio Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio shared mode lets you use single [NAS](https://en.wikipedia.org/wiki/Network-attached_storage) (like NFS, GlusterFS, and other 
distributed filesystems) as the storage backend for multiple Minio servers. Synchronization among Minio servers is taken care by design. 
Read more about the Minio shared mode design [here](https://github.com/minio/minio/blob/master/docs/shared-backend/DESIGN.md).

Minio shared mode is developed to solve several real world use cases, without any special configuration changes. Some of these are

- You have already invested in NAS and would like to use Minio to add S3 compatibility to your storage tier.
- You need to use NAS with an S3 interface due to your application architecture requirements.
- You expect huge traffic and need a load balanced S3 compatible server, serving files from a single NAS backend. 

With a proxy running in front of multiple, shared mode Minio servers, it is very easy to create a Highly Available, load balanced, AWS S3 compatible storage system. 

# Get started

If you're aware of stand-alone Minio set up, the installation and running remains the same. 

## 1. Prerequisites

Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio).

## 2. Run Minio on Shared Backend

To run Minio shared backend instances, you need to start multiple Minio servers pointing to the same backend storage. We'll see examples on how to do this in the following sections.

*Note*

- All the nodes running shared Minio need to have same access key and secret key. To achieve this, we export access key and secret key as environment variables on all the nodes before executing Minio server command.
- The drive paths below are for demonstration purposes only, you need to replace these with the actual drive paths/folders.

#### Minio shared mode on Ubuntu 16.04 LTS. 

You'll need the path to the shared volume, e.g. `/path/to/nfs-volume`. Then run the following commands on all the nodes you'd like to launch Minio.

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server /path/to/nfs-volume
```

#### Minio shared mode on Windows 2012 Server

You'll need the path to the shared volume, e.g. `\\remote-server\smb`. Then run the following commands on all the nodes you'd like to launch Minio.

```cmd
set MINIO_ACCESS_KEY=my-username
set MINIO_SECRET_KEY=my-password
minio.exe server \\remote-server\smb\export
```

*Windows Tip*

If a remote volume, e.g. `\\remote-server\smb` is mounted as a drive, e.g. `M:\`. You can use [`net use`](https://technet.microsoft.com/en-us/library/bb490717.aspx) command to map the drive to a folder. 

```cmd
set MINIO_ACCESS_KEY=my-username
set MINIO_SECRET_KEY=my-password
net use m: \\remote-server\smb\export /P:Yes
minio.exe server M:\export
```

## 3. Test your setup

To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the all the Minio shared backend endpoints.

## Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
