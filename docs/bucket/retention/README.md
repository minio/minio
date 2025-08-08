# Object Lock and Immutability Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows WORM for specific objects or by configuring a bucket with default object lock configuration that applies default retention mode and retention duration to all objects. This makes objects in the bucket immutable i.e. delete of the version are not allowed until an expiry specified in the bucket's object lock configuration or object retention.

Object locking requires locking to be enabled on a bucket at the time of bucket creation refer to `mc mb --with-lock`, object locking enables versioning on the bucket and cannot be disabled.

A default retention period and retention mode can be configured on a bucket to be applied to objects created in that bucket. Independent of retention, an object can also be under legal hold. This effectively disallows all deletes of an object under legal hold until the legal hold is removed by an API call.

## Get Started

### 1. Prerequisites

- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html)
- Install `awscli` - [Installing AWS Command Line Interface](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

### 2. Set bucket WORM configuration

WORM on a bucket is enabled by setting object lock configuration. This configuration is applied to all the objects in the bucket. Below is an example to set `Governance` mode and one day retention time on `mybucket`.

```sh
awscli s3api put-object-lock-configuration --bucket mybucket --object-lock-configuration 'ObjectLockEnabled=\"Enabled\",Rule={DefaultRetention={Mode=\"GOVERNANCE\",Days=1}}'
```

### Set object lock

PutObject API allows setting per object retention mode and retention duration using `x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date` headers. This takes precedence over any bucket object lock configuration w.r.t retention.

```sh
aws s3api put-object --bucket testbucket --key lockme --object-lock-mode GOVERNANCE --object-lock-retain-until-date "2019-11-20"  --body /etc/issue
```

See <https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html> for AWS S3 spec on object locking and permissions required for object retention and governance bypass overrides.

### Set legal hold on an object

PutObject API allows setting legal hold using `x-amz-object-lock-legal-hold` header.

```sh
aws s3api put-object --bucket testbucket --key legalhold --object-lock-legal-hold-status ON --body /etc/issue
```

See <https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html> for AWS S3 spec on object locking and permissions required for specifying legal hold.

## Concepts

- If an object is under legal hold, it cannot be deleted unless the legal hold is explicitly removed for the respective version id. DeleteObjectVersion() would fail otherwise.
- In `Compliance` mode, objects cannot be deleted by anyone until retention period has expired for the given version id. If user has governance bypass permissions, an object's retention date can be extended in `Compliance` mode.
- Once object lock configuration is set to a bucket
  - New objects inherit the retention settings of the bucket object lock configuration automatically
  - Retention headers can be optionally set while uploading objects
  - Once objects are uploaded PutObjectRetention API can be called to change retention settings
- *MINIO_NTP_SERVER* environment variable can be set to remote NTP server endpoint if system time is not desired for setting retention dates.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/community/minio-object-store/integrations/aws-cli-with-minio.html)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/community/minio-object-store/developers/go/minio-go.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
