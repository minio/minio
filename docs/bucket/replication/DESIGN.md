# Bucket Replication Design [![slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

This document explains the design approach of server side bucket replication. If you're looking to get started with replication, we suggest you go through the [Bucket replication guide](https://github.com/minio/minio/blob/master/docs/bucket/replication/README.md) first.

## Overview
Replication relies on immutability provided by versioning to sync objects between the configured source and replication target. Replication results in the object data, metadata, last modification time and version ID all being identical between the source and target. Thus version ordering is automatically guaranteed on the source and target clusters.

### Replication of object version and metadata
If an object meets replication rules as set in the replication configuration, `X-Amz-Replication-Status` is first set to `PENDING` as the PUT operation completes and replication is queued (unless synchronous replication is in place). After replication is performed, the metadata on the source object version changes to `COMPLETED` or `FAILED` depending on whether replication succeeded. The object version on the target shows `X-Amz-Replication-Status` of `REPLICA`

All replication failures are picked up by the scanner which runs at a one minute frequency, each time scanning upto a sixteenth of the namespace. Object versions marked `PENDING` or `FAILED` are re-queued for replication.

Replication speed depends on the cluster load, number of objects in the object store as well as storage speed. In addition, any bandwidth limits set via `mc admin bucket remote add` could also contribute to replication speed. The number of workers used for replication defaults to 100. Based on network bandwidth and system load, the number of workers used in replication can be configured using `mc admin config set alias api` to set the `replication_workers`. The prometheus metrics exposed by MinIO can be used to plan resource allocation and bandwidth management to optimize replication speed.

If synchronous replication is configured above, replication is attempted right away prior to returning the PUT object response. In the event that the replication target is down, the `X-Amz-Replication-Status` is marked as `FAILED` and resynced with target when the scanner runs again.

Any metadata changes on the source object version, such as metadata updates via PutObjectTagging, PutObjectRetention, PutObjectLegalHold and COPY api are replicated in a similar manner to target version, with the `X-Amz-Replication-Status` again cycling through the same states.

The description above details one way replication from source to target w.r.t incoming object uploads and metadata changes to source object version. If active-active replication is configured, any incoming uploads and metadata changes to versions created on the target, will sync back to the source and be marked as `REPLICA` on the source. AWS, as well as MinIO do not by default sync metadata changes on a object version marked `REPLICA` back to source. This requires a setting in the replication configuration called [replica modification sync](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-replication-adds-support-two-way-replication/).

For active-active replication, automatic failover occurs on `GET/HEAD` operations if object or object version requested qualifies for replication and is missing on one site, but present on the other. This allows the applications to take full advantage of two-way replication even before the two sites get fully synced.

### Replication of DeleteMarker and versioned Delete

MinIO allows DeleteMarker replication and versioned delete replication by setting `--replicate delete,delete-marker` while setting up replication configuration using `mc replicate add`. The MinIO implementation is based on V2 configuration, however it has been extended to allow both DeleteMarker replication and replication of versioned deletes with the `DeleteMarkerReplication` and `DeleteReplication` fields in the replication configuration. By default, this is set to `Disabled` unless the user specifies it while adding a replication rule.

Similar to object version replication, DeleteMarker replication also cycles through `PENDING` to `COMPLETED` or `FAILED` states for the `X-Amz-Replication-Status` on the source when a delete marker is set (i.e. performing `mc rm` on an object without specifying a version).After replication syncs the delete marker on the target, the DeleteMarker on the target shows `X-Amz-Replication-Status` of `REPLICA`. The status of DeleteMarker replication is returned by `X-Minio-Replication-DeleteMarker-Status` header on `HEAD/GET` calls for the delete marker version in question - i.e with `mc stat --version-id dm-version-id`

It must be noted that if active-active replication is set up with delete marker replication, there is potential for duplicate delete markers to be created if both source and target concurrently set a Delete Marker or if one/both of the clusters went down at tandem before the replication event was synced.This is an unavoidable side-effect in active-active replication caused by allowing delete markers set on a object version with `REPLICA` status back to source.

In the case of versioned deletes a.k.a permanent delete of a version by doing a `mc rm --version-id` on a object, replication implementation marks a object version permanently deleted as `PENDING` purge and deletes the version from source after syncing to the target and ensuring target version is deleted. The delete marker being deleted or object version being deleted will still be visible on listing with `mc ls --versions` until the sync is completed. Objects marked as deleted will not be accessible via `GET` or `HEAD` requests and would return a http response code of `405`. The status of versioned delete replication on the source can be queried by `HEAD` request on the delete marker versionID or object versionID in question.
An additional header `X-Minio-Replication-Delete-Status` is returned which would show `PENDING` or `FAILED` status if the replication is still not caught up.

Note that synchronous replication, i.e. when remote target is configured with --sync mode in `mc admin bucket remote add` does not apply to `DELETE` operations. The version being deleted on the source cluster needs to maintain state and ensure that the operation is mirrored to the target cluster prior to completing on the source object version. Since this needs to account for the target cluster availability and the need to serialize concurrent DELETE operations on different versions of the same object during multi DELETE operations, the current implementation queues the `DELETE` operations in both sync and async modes.

### Existing object replication
Existing object replication works similar to regular replication. Objects qualifying for existing object replication are detected when scanner runs, and will be replicated if existing object replication is enabled and applicable replication rules are satisfied. Because replication depends on the immutability of versions, only pre-existing objects created while versioning was enabled can be replicated. Even if replication rules are disabled and re-enabled later, the objects created during the interim will be synced as the scanner queues them. For saving iops, objects qualifying for
existing object replication are not marked as `PENDING` prior to replication.

If the remote site is fully lost and objects previously replicated need to be re-synced, the `mc replicate resync` command with optional flag of `--older-than` needs to be used to trigger re-syncing of previously replicated objects. This command generates a ResetID which is a unique UUID saved to the remote target config along with the applicable date(defaults to time of initiating the reset). All objects created prior to this date are eligible for re-replication if existing object replication is enabled for the replication rule the object satisifes. At the time of completion of replication, `X-Minio-Replication-Reset-Status` is set in the metadata with the timestamp of replication and ResetID. For saving iops, the objects which are re-replicated are not first set to `PENDING` state.

### Internal metadata for replication

`xl.meta` that is in use for [versioning](https://github.com/minio/minio/blob/master/docs/bucket/versioning/DESIGN.md) has additional metadata for replication of objects,delete markers and versioned deletes.

### Metadata for object replication

```
...
    "MetaSys": {},
        "MetaUsr": {
          "X-Amz-Replication-Status": "COMPLETED",
          "content-type": "application/octet-stream",
          "etag": "8315e643ed6a5d7c9962fc0a8ef9c11f"
        },
        "PartASizes": [
          26
        ],
...
```

### Additional replication metadata for DeleteMarker

```
{
      "DelObj": {
        "ID": "8+jguy20TOuzUCN2PTrESA==",
        "MTime": 1613601949645331516,
        "MetaSys": {
          "X-Amz-Replication-Status": "Q09NUExFVEVE"
        }
      },
      "Type": 2
    }
```

### Additional replication metadata for versioned delete

```
{
      "DelObj": {
        "ID": "8+jguy20TOuzUCN2PTrESA==",
        "MTime": 1613601949645331516,
        "MetaSys": {
          "purgestatus": "RkFJTEVE"
        }
      },
      "Type": 2
    }
```

## Explore Further
- [MinIO Bucket Versioning Implementation](https://docs.minio.io/docs/minio-bucket-versioning-guide.html)
- [MinIO Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
