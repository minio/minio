# ILM Tiering Design [![slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Lifecycle transition functionality provided in [bucket lifecycle guide](https://github.com/minio/minio/master/docs/bucket/lifecycle/README.md) allows tiering of content from MinIO object store to public clouds or other MinIO clusters.

Transition tiers can be added to MinIO using `mc admin tier add` command to associate a `gcs`, `s3` or `azure` bucket or prefix path on a bucket to the tier name.
Lifecycle transition rules can be applied to buckets (both versioned and un-versioned) by specifying the tier name defined above as the transition storage class for the lifecycle rule.

## Implementation

ILM tiering takes place when a object placed in the bucket meets lifecycle transition rules and becomes eligible for tiering. MinIO scanner (which runs at one minute intervals, each time scanning one sixteenth of the namespace), picks up the object for tiering. The data is moved to the remote tier in entirety, leaving only the object metadata on MinIO.

The data on the backend is stored under the `bucket/prefix` specified in the tier configuration with a custom name derived from a randomly generated uuid - e.g. `0b/c4/0bc4fab7-2daf-4d2f-8e39-5c6c6fb7e2d3`. The first two prefixes are characters 1-2,3-4 from the uuid. This format allows tiering to any cloud irrespective of whether the cloud in question supports versioning. The reference to the transitioned object name and transitioned tier is stored as part of the internal metadata for the object (or its version) on MinIO.

Extra metadata maintained internally in `xl.meta` for a transitioned object

```
...
        "MetaSys": {
          "x-minio-internal-transition-status": "Y29tcGxldGU=",
          "x-minio-internal-transition-tier": "R0NTVElFUjE=",
          "x-minio-internal-transitioned-object": "ZDIvN2MvZDI3Y2MwYWMtZGIzNC00ZGM1LWIxNDUtYjI5MGNjZjU1MjY5"
        },
```

When a transitioned object is restored temporarily to local MinIO instance via PostRestoreObject API, the object data is copied back from the remote tier, and additional metadata for the restored object is maintained as referenced below. Once the restore period expires, the local copy of the object is removed by the scanner during its periodic runs.

```
...
        "MetaUsr": {
         "X-Amz-Restore-Expiry-Days": "4",
          "X-Amz-Restore-Request-Date": "Mon, 22 Feb 2021 21:10:09 GMT",
          "x-amz-restore": "ongoing-request=false, expiry-date=Sat, 27 Feb 2021 00:00:00 GMT",
...
```

### Encrypted/Object locked objects

For objects under SSE-S3 or SSE-C encryption, the encrypted content from MinIO cluster is copied as is to the remote tier without any decryption. The content is decrypted as it is streamed from remote tier on `GET/HEAD`. Objects under retention are protected because the metadata present on MinIO server ensures that the object (version) is not deleted until retention period is over. Administrators need to ensure that the remote tier bucket is under proper access control.

### Transition Status

MinIO specific extension header `X-Minio-Transition` is displayed on `HEAD/GET` to predict expected transition date on the object. Once object is transitioned to the remote tier,`x-amz-storage-class` shows the tier name to which object transitioned. Additional headers such as "X-Amz-Restore-Expiry-Days", "x-amz-restore", and "X-Amz-Restore-Request-Date" are displayed when a object is under restore/has been restored to local MinIO cluster.

### Expiry or removal events

An object that is in transition tier will be deleted once the object hits expiry date or if removed via `mc rm` (`mc rm --vid` in the case of delete of a specific object version). Other rules specific to legal hold and object locking precede any lifecycle rules.

### Additional notes

Tiering and lifecycle transition are applicable only to erasure/distributed MinIO.

## Explore Further

- [MinIO | Golang Client API Reference](https://docs.min.io/community/minio-object-store/developers/go/API.html#SetBucketLifecycle)
- [Object Lifecycle Management](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html)
