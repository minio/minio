# Large Bucket Support Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Erasure code in Minio is limited to 16 disks. This on its own allows storage space to hold a tenant's data. However, to cater to cases which may need larger number of disks or high storage space requirements, large bucket support was introduced.

We call it large-bucket as it allows a single Minio bucket to expand over multiple erasure code deployment sets. Without any special configuration it helps you create peta-scale storage systems. With large bucket support, you can use more than 16 disks upfront while deploying the Minio server. Internally Minio creates multiple smaller erasure coded sets, and these sets are further combined into a single namespace. This document gives a brief introduction on how to get started with large bucket deployments. To explore further on advanced uses and limitations, refer to the [design document](https://github.com/minio/minio/blob/master/docs/large-bucket/DESIGN.md).

## Get started
If you're aware of distributed Minio setup, the installation and configuration remains the same. Only new addition to the input syntax is `...` convention to abbreviate the drive arguments. Remote drives in a distributed setup are encoded as HTTP(s) URIs which can be similarly abbreviated as well.

### 1. Prerequisites
Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio).

### 2. Run Minio on many drives
We'll see examples on how to do this in the following sections.

*Note*

- All the nodes running distributed Minio need to have same access key and secret key. To achieve this, we export access key and secret key as environment variables on all the nodes before executing Minio server command.
- The drive paths below are for demonstration purposes only, you need to replace these with the actual drive paths/folders.

#### Minio large bucket on multiple drives (standalone)
You'll need the path to the disks e.g. `/export1, /export2 .... /export24`. Then run the following commands on all the nodes you'd like to launch Minio.

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server /export{1...24}
```

#### Minio large bucket on multiple servers (distributed)
You'll need the path to the disks e.g. `http://host1/export1, http://host2/export2 .... http://host4/export16`. Then run the following commands on all the nodes you'd like to launch Minio.

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...4}/export{1...16}
```

### 3. Test your setup
To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the all the Minio endpoints.

## Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
