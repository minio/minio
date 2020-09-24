# Bucket Replication Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Bucket replication is designed to replicate selected objects in a bucket to a destination bucket.

To replicate objects in a bucket to a destination bucket on a target site either in the same cluster or a different cluster, start by enabling [versioning](https://docs.minio.io/docs/minio-bucket-versioning-guide.html) for both source and destination buckets. Finally, the target site and the destination bucket need to be configured on the source MinIO server.

## Highlights
- Supports source and destination buckets to have the same name unlike AWS S3, addresses variety of usecases such as *Splunk*, *Veeam* site to site DR.
- Supports object locking/retention across source and destination buckets natively out of the box, unlike AWS S3.
- Simpler implementation than [AWS S3 Bucket Replication Config](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html) with requirements such as IAM Role, AccessControlTranslation, Metrics and SourceSelectionCriteria are not needed with MinIO.
- Active-Active replication

## How to use?
Create a replication target on the source cluster as shown below:

```
mc admin bucket remote add myminio/srcbucket https://accessKey:secretKey@replica-endpoint:9000/destbucket --service replication --region us-east-1
Role ARN = 'arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket'
```

>  The user running the above command needs *s3:GetReplicationConfiguration* and *s3:GetBucketVersioning* permission on the source cluster. We do not recommend running root credentials/super admin with replication, instead create a dedicated user. The access credentials used at the destination requires *s3:ReplicateObject* permission.

The *source* bucket should have following minimal permission policy:
```
{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:GetReplicationConfiguration",
    "s3:ListBucket",
    "s3:GetBucketLocation",
    "s3:GetBucketVersioning"
   ],
   "Resource": [
    "arn:aws:s3:::srcbucket"
   ]
  }
}
```
The access key provided for the replication *target* cluster should have these minimal permissions:
```
{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:GetBucketVersioning"
   ],
   "Resource": [
    "arn:aws:s3:::destbucket"
   ]
  },
  {
   "Effect": "Allow",
   "Action": [
    "s3:ReplicateTags",
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionTagging",
    "s3:PutObject",
    "s3:ReplicateObject"
   ],
   "Resource": [
    "arn:aws:s3:::destbucket/*"
   ]
  }
 ]
}

```
Once successfully created and authorized, the `mc admin bucket remote add` command generates a replication target ARN.  This command lists all the currently authorized replication targets:
```
mc admin bucket remote ls myminio/srcbucket --service "replication"
Role ARN = 'arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket'
```

The replication configuration can now be added to the source bucket by applying the json file with replication configuration. The Role ARN above is passed in as a json element in the configuration.

```json
{
  "Role" :"arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket",
  "Rules": [
    {
      "Status": "Enabled",
      "Priority": 1,
      "DeleteMarkerReplication": { "Status": "Disabled" },
      "Filter" : {
        "And": {
            "Prefix": "Tax",
            "Tags": [
                {
                "Key": "Year",
                "Value": "2019"
                },
                {
                "Key": "Company",
                "Value": "AcmeCorp"
                }
            ]
        }
      },
      "Destination": {
        "Bucket": "arn:aws:s3:::destbucket",
        "StorageClass": "STANDARD"
      }
    }
  ]
}
```

```
mc replicate add myminio/srcbucket/Tax --priority 1 --arn "arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket" --tags "Year=2019&Company=AcmeCorp" --storage-class "STANDARD" --remote-bucket "destbucket"
Replication configuration applied successfully to myminio/srcbucket.
```

The replication configuration follows [AWS S3 Spec](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html). Any objects uploaded to the source bucket that meet replication criteria will now be automatically replicated by the MinIO server to the remote destination bucket. Replication can be disabled at any time by disabling specific rules in the configuration or deleting the replication configuration entirely.


When an object is deleted from the source bucket, the replica will not be deleted as per S3 spec.

![delete](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/DELETE_bucket_replication.png)

When object locking is used in conjunction with replication, both source and destination buckets needs to have object locking enabled. Similarly objects encrypted on the server side, will be replicated if destination also supports encryption.

Replication status can be seen in the metadata on the source and destination objects. On the source side, the `X-Amz-Replication-Status` changes from `PENDING` to `COMPLETE` or `FAILED` after replication attempt either succeeded or failed respectively. On the destination side, a `X-Amz-Replication-Status` status of `REPLICA` indicates that the object was replicated successfully. Any replication failures are automatically re-attempted during a periodic disk crawl cycle.

To perform bi-directional replication, repeat the above process on the target site - this time setting the source bucket as the replication target.

It is recommended that replication be run in a system with atleast two CPU's available to the process, so that replication can run in its own thread.

![put](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/PUT_bucket_replication.png)

![head](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/replication/HEAD_bucket_replication.png)

## Explore Further
- [MinIO Bucket Versioning Implementation](https://docs.minio.io/docs/minio-bucket-versioning-guide.html)
- [MinIO Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
