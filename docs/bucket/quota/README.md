# Bucket Quota Configuration Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

![quota](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/quota/bucketquota.png)

Buckets can be configured to have `Hard` quota - it disallows writes to the bucket after configured quota limit is reached.

> NOTE: Bucket quotas are not supported under gateway or standalone single disk deployments.

## Prerequisites

- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)

## Set bucket quota configuration

### Set a hard quota of 1GB for a bucket `mybucket` on MinIO object storage

```sh
mc admin bucket quota myminio/mybucket --hard 1gb
```

### Verify the quota configured on `mybucket` on MinIO

```sh
mc admin bucket quota myminio/mybucket
```

### Clear bucket quota configuration for `mybucket` on MinIO

```sh
mc admin bucket quota myminio/mybucket --clear
```
