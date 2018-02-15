# Large Bucket Support Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio large bucket support lets you use more than 16 disks by creating a number of smaller sets of erasure coded units, these units are further combined into a single namespace. Minio large bucket support is developed to solve for several real world use cases, without any special configuration changes. Some of these are

- You already have racks with many disks.
- You are looking for large capacity up-front for your object storage needs.

# Get started
If you're aware of distributed Minio setup, the installation and running remains the same. Newer syntax to use a `...` convention to abbreviate the directory arguments. Remote directories in a distributed setup are encoded as HTTP(s) URIs which can be similarly abbreviated as well.

## 1. Prerequisites
Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio).

## 2. Run Minio on many disks
To run Minio large bucket instances, you need to start multiple Minio servers pointing to the same disks. We'll see examples on how to do this in the following sections.

*Note*

- All the nodes running distributed Minio need to have same access key and secret key. To achieve this, we export access key and secret key as environment variables on all the nodes before executing Minio server command.
- The drive paths below are for demonstration purposes only, you need to replace these with the actual drive paths/folders.

### Minio large bucket on Ubuntu 16.04 LTS standalone
You'll need the path to the disks e.g. `/export1, /export2 .... /export24`. Then run the following commands on all the nodes you'd like to launch Minio.

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server /export{1...24}
```

### Minio large bucket on Ubuntu 16.04 LTS servers
You'll need the path to the disks e.g. `/export1, /export2 .... /export16`. Then run the following commands on all the nodes you'd like to launch Minio.

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...4}/export{1...16}
```

## 3. Test your setup
To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the all the Minio endpoints.

## Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
