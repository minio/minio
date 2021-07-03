# Bucket Lifecycle Configuration Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Enable object lifecycle configuration on buckets to setup automatic deletion of objects after a specified number of days or a specified date.

## 1. Prerequisites
- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).
- Install `mc` - [mc Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)

## 2. Enable bucket lifecycle configuration

- Create a bucket lifecycle configuration which expires the objects under the prefix `old/` on `2020-01-01T00:00:00.000Z` date and the objects under `temp/` after 7 days.
- Enable bucket lifecycle configuration using `mc`:

```sh
$ mc ilm import play/testbucket <<EOF
{
    "Rules": [
        {
            "Expiration": {
                "Date": "2020-01-01T00:00:00.000Z"
            },
            "ID": "OldPictures",
            "Filter": {
                "Prefix": "old/"
            },
            "Status": "Enabled"
        },
        {
            "Expiration": {
                "Days": 7
            },
            "ID": "TempUploads",
            "Filter": {
                "Prefix": "temp/"
            },
            "Status": "Enabled"
        }
    ]
}
EOF
```

```
Lifecycle configuration imported successfully to `play/testbucket`.
```

- List the current settings
```
$ mc ilm ls play/testbucket
     ID     |  Prefix  |  Enabled   | Expiry |  Date/Days   |  Transition  |    Date/Days     |  Storage-Class   |       Tags
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
OldPictures |   old/   |    ✓       |  ✓     |  1 Jan 2020  |     ✗        |                  |                  |
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
TempUploads |  temp/   |    ✓       |  ✓     |   7 day(s)   |     ✗        |                  |                  |
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
```

## 3. Activate ILM versioning features

This will only work with a versioned bucket, take a look at [Bucket Versioning Guide](https://docs.min.io/docs/minio-bucket-versioning-guide.html) for more understanding.

### 3.1 Automatic removal of non current objects versions

A non-current object version is a version which is not the latest for a given object. It is possible to set up an automatic removal of non-current versions when a version becomes older than a given number of days.

e.g., To scan objects stored under `user-uploads/` prefix and remove versions older than one year.
```
{
    "Rules": [
        {
            "ID": "Removing all old versions",
            "Filter": {
                "Prefix": "users-uploads/"
            },
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 365
            },
            "Status": "Enabled"
        }
    ]
}
```

### 3.2 Automatic removal of delete markers with no other versions

When an object has only one version as a delete marker, the latter can be automatically removed after a certain number of days using the following configuration:

```
{
    "Rules": [
        {
            "ID": "Removing all delete markers",
            "Expiration": {
                "DeleteMarker": true
            },
            "Status": "Enabled"
        }
    ]
}
```
## 4. Enable ILM transition feature

In Erasure mode, MinIO supports tiering to public cloud providers such as GCS, AWS and Azure as well as to other MinIO clusters via the ILM transition feature. This will allow transitioning of older objects to a different cluster or the public cloud by setting up transition rules in the bucket lifecycle configuration. This feature enables applications to optimize storage costs by moving less frequently accessed data to a cheaper storage without compromising accessibility of data.

To transition objects in a bucket to a destination bucket on a different cluster, applications need to specify a transition tier defined on MinIO instead of storage class while setting up the ILM lifecycle rule.

> To create a transition tier for transitioning objects to a prefix `testprefix` in `azurebucket` on Azure blob using `mc`:

```
 mc admin tier add azure source AZURETIER --endpoint https://blob.core.windows.net --access-key AZURE_ACCOUNT_NAME --secret-key AZURE_ACCOUNT_KEY  --bucket azurebucket --prefix testprefix1/
```
> The admin user running this command needs the "admin:SetTier" and "admin:ListTier" permissions if not running as root.

Using above tier, set up a lifecycle rule with transition:
```
 mc ilm add --expiry-days 365 --transition-days 45 --storage-class "AZURETIER" myminio/srcbucket
```

Note: In the case of S3, it is possible to create a tier from MinIO running in EC2 to S3 using AWS role attached to EC2 as credentials instead of accesskey/secretkey:
```
mc admin tier add s3 source S3TIER --bucket s3bucket --prefix testprefix/ --use-aws-role
```

Once transitioned, GET or HEAD on the object will stream the content from the transitioned tier. In the event that the object needs to be restored temporarily to the local cluster, the AWS [RestoreObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html) can be utilized.

```
aws s3api restore-object --bucket srcbucket \
--key object \
--restore-request Days=3
```

### 4.1 Monitoring transition events
`s3:ObjectTransition:Complete` and `s3:ObjectTransition:Failed` events can be used to monitor transition events between the source cluster and transition tier. To watch lifecycle events, you can enable bucket notification on the source bucket with `mc event add`  and specify `--event ilm` flag.

Note that transition event notification is a MinIO extension.

## Explore Further
- [MinIO | Golang Client API Reference](https://docs.min.io/docs/golang-client-api-reference.html#SetBucketLifecycle)
- [Object Lifecycle Management](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html)
