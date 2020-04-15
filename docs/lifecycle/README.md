# Object Lifecycle Configuration Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Enable object lifecycle configuration on buckets to setup automatic deletion of objects after a specified number of days or a specified date.

## 1. Prerequisites
- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).
- Install AWS Cli - [Installing AWS Command Line Interface](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)


## 2. Enable bucket lifecycle configuration

1. Create a bucket lifecycle configuration which expires the objects under the prefix `uploads/2015` on `2020-01-01T00:00:00.000Z` date and the objects under `temporary-uploads/` after 7 days.  Generate it as shown below:

```sh
$ cat >bucket-lifecycle.json << EOF
{
    "Rules": [
        {
            "Expiration": {
                "Date": "2020-01-01T00:00:00.000Z"
            },
            "ID": "Delete very old messenger pictures",
            "Filter": {
                "Prefix": "uploads/2015/"
            },
            "Status": "Enabled"
        },
        {
            "Expiration": {
                "Days": 7
            },
            "ID": "Delete temporary uploads",
            "Filter": {
                "Prefix": "temporary-uploads/"
            },
            "Status": "Enabled"
        }
    ]
}
EOF
```

2. Enable bucket lifecycle configuration using `aws-cli`:

```sh
$ export AWS_ACCESS_KEY_ID="your-access-key"
$ export AWS_SECRET_ACCESS_KEY="your-secret-key"
$ aws s3api put-bucket-lifecycle-configuration --bucket your-bucket --endpoint-url http://minio-server-address:port --lifecycle-configuration file://bucket-lifecycle.json
```

3. Verify that the configuration has been added

```sh
$ aws s3api get-bucket-lifecycle-configuration --bucket your-bucket --endpoint-url http://minio-server-address:port
```

## Explore Further
- [MinIO | Golang Client API Reference](https://docs.min.io/docs/golang-client-api-reference.html#SetBucketLifecycle)
- [Object Lifecycle Management](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html)
