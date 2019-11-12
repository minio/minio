# Object Lock and Immutablity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows to set bucket level WORM which makes objects in the bucket immutable i.e. delete and overwrite are not allowed till stipulated time specified in the bucket's object lock configuration.

## Get Started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Set per bucket WORM

WORM on a bucket is enabled by setting object lock configuration. This configuration is applied to existing and new objects in the bucket. Below is an example sets `Governance` mode and one day retention time from object creation time of all objects in `mybucket`.

```sh
$ awscli s3api put-object-lock-configuration --bucket mybucket --object-lock-configuration 'ObjectLockEnabled=\"Enabled\",Rule={DefaultRetention={Mode=\"GOVERNANCE\",Days=1}}'
```

### 3. Note

- When global WORM is enabled by `MINIO_WORM` environment variable or `worm` field in configuration file supersedes bucket level WORM and `PUT object lock configuration` REST API is disabled.
- Currently Governance mode is treated as Compliance mode.
- Once object lock configuration is set to a bucket, existing and new objects are put in WORM mode.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
