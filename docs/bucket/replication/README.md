# Bucket Replication Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Bucket replication is designed to replicate specific objects in buckets
that are configured for replication.

To replicate objects in a bucket to a destination bucket on a target site either on the same cluster or a different cluster, start by creating version enabled buckets on both `source` and `dest` buckets. Next, the target site and destination bucket need to be configured on MinIO server by setting

`
$ mc admin bucket replication set myminio/source https://accessKey:secretKey@replica-endpoint:9000/dest --path-style "auto" --api "s3v2"
`

Note that the admin needs "s3:GetReplicationConfigurationAction" permission on source in addition to "s3:ReplicateObject" permission on destination side.

Next, the replication ARN associated with the replica URL https://replica-endpoint:9000 can be fetched from the MinIO server by using the command 
`mc admin bucket remote myminio/source https://replica-endpoint:9000`

The replication configuration can now be added to the source bucket by applying the json file with replication configuration. The ReplicationArn is passed in as a json element in the configuration.

The replication configuration follows [S3 Spec](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html). Any
objects uploaded to the source bucket that meet replication criteria will now automatically be replicated by the MinIO server. Replication can be stopped at any time by disabling specific rules in the configuration or deleting the replication configuration.

When an object is deleted from the source bucket, the replica will not be deleted as per S3 spec.
![delete](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/DELETE_bucket_replication.png)

When object locking is used in conjunction with replication, both source and destination buckets needs to have object locking enabled. Similarly objects encrypted on the server side, will be replicated if destination also supports
encryption.

Replication status can be seen in the metadata on the source and destination objects. On the source side, the `X-Amz-Replication-Status` changes from `PENDING` to `COMPLETE` or `FAILED` after replication attempt is made. On the destination side, a `X-Amz-Replication-Status` status of `REPLICA` indicates that the object
was replicated successfully. Any replication failures are automatically reattempted during a periodic disk usage crawl.
![put](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/PUT_bucket_replication.png)

![head](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/HEAD_bucket_replication.png)

## Additional notes
The [S3 Spec]([S3 Spec](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html) has configuration such as IAM Role,AccessControlTranslation, Metrics and SourceSelectionCriteria which are not required for MinIO.
