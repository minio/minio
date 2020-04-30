# Object Lock and Immutablity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows selectively specify WORM for specific objects or configuring a bucket with default object lock configuration that applies default retention mode and retention duration to all incoming objects. Essentially, this makes objects in the bucket immutable i.e. delete and overwrite are not allowed till stipulated time specified in the bucket's object lock configuration or object retention.

Object locking requires locking to be enabled on a bucket at the time of bucket creation. In addition, a default retention period and retention mode can be configured on a bucket to be applied to objects created in that bucket.

Independently of retention, an object can also be under legal hold. This effectively disallows all deletes and overwrites of an object under legal hold until the hold is lifted.

## Get Started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Set bucket WORM configuration

WORM on a bucket is enabled by setting object lock configuration. This configuration is applied to existing and new objects in the bucket. Below is an example sets `Governance` mode and one day retention time from object creation time of all objects in `mybucket`.

```sh
$ awscli s3api put-object-lock-configuration --bucket mybucket --object-lock-configuration 'ObjectLockEnabled=\"Enabled\",Rule={DefaultRetention={Mode=\"GOVERNANCE\",Days=1}}'
```

### Set object lock

PutObject API allows setting per object retention mode and retention duration using `x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date` headers. This takes precedence over any bucket object lock configuration w.r.t retention.

```sh
aws s3api put-object --bucket testbucket --key lockme --object-lock-mode GOVERNANCE --object-lock-retain-until-date "2019-11-20"  --body /etc/issue
```

See https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html for AWS S3 spec on object locking and permissions required for object retention and governance bypass overrides.

### Set legal hold on an object

PutObject API allows setting legal hold using `x-amz-object-lock-legal-hold` header.

```sh
aws s3api put-object --bucket testbucket --key legalhold --object-lock-legal-hold-status ON --body /etc/issue
```

See https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html for AWS S3 spec on object locking and permissions required for specifying legal hold.

> NOTE:
> - If an object is under legal hold, it cannot be overwritten unless the legal hold is explicitly removed.
> - In `Compliance` mode, objects cannot be overwritten or deleted by anyone until retention period is expired. If user has requisite governance bypass permissions, an object's retention date can be extended in `Compliance` mode.
> - Currently `Governance` mode does not allow overwriting an existing object as versioning is not available in MinIO. However, if user has requisite `Governance` bypass permissions, an object in `Governance` mode can be overwritten.
> - Once object lock configuration is set to a bucket, new objects inherit the retention settings of the bucket object lock configuration (if set) or the retention headers set in the PUT request or set with PutObjectRetention API call
> - *MINIO_NTP_SERVER* environment variable can be set to remote NTP server endpoint if system time is not desired for setting retention dates.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
