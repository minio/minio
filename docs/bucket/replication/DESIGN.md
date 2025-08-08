# Bucket Replication Design [![slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

This document explains the design approach of server side bucket replication. If you're looking to get started with replication, we suggest you go through the [Bucket replication guide](https://github.com/minio/minio/blob/master/docs/bucket/replication/README.md) first.

## Overview

Replication relies on immutability provided by versioning to sync objects between the configured source and replication target. Replication results in the object data, metadata, last modification time and version ID all being identical between the source and target. Thus version ordering is automatically guaranteed on the source and target clusters.

### Replication of object version and metadata

If an object meets replication rules as set in the replication configuration, `X-Amz-Replication-Status` is first set to `PENDING` as the PUT operation completes and replication is queued (unless synchronous replication is in place). After replication is performed, the metadata on the source object version changes to `COMPLETED` or `FAILED` depending on whether replication succeeded. The object version on the target shows `X-Amz-Replication-Status` of `REPLICA`

All replication failures are picked up by the scanner which runs at a one minute frequency, each time scanning up to a sixteenth of the namespace. Object versions marked `PENDING` or `FAILED` are re-queued for replication.

Replication speed depends on the cluster load, number of objects in the object store as well as storage speed. In addition, any bandwidth limits set via `mc admin bucket remote add` could also contribute to replication speed. The number of workers used for replication defaults to 100. Based on network bandwidth and system load, the number of workers used in replication can be configured using `mc admin config set alias api` to set the `replication_workers`. The prometheus metrics exposed by MinIO can be used to plan resource allocation and bandwidth management to optimize replication speed.

If synchronous replication is configured above, replication is attempted right away prior to returning the PUT object response. In the event that the replication target is down, the `X-Amz-Replication-Status` is marked as `FAILED` and resynced with target when the scanner runs again.

Any metadata changes on the source object version, such as metadata updates via PutObjectTagging, PutObjectRetention, PutObjectLegalHold and COPY api are replicated in a similar manner to target version, with the `X-Amz-Replication-Status` again cycling through the same states.

The description above details one way replication from source to target w.r.t incoming object uploads and metadata changes to source object version. If active-active replication is configured, any incoming uploads and metadata changes to versions created on the target, will sync back to the source and be marked as `REPLICA` on the source. AWS, as well as MinIO do not by default sync metadata changes on a object version marked `REPLICA` back to source. This requires a setting in the replication configuration called [replica modification sync](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-replication-adds-support-two-way-replication/).

For active-active replication, automatic failover occurs on `GET/HEAD` operations if object or object version requested qualifies for replication and is missing on one site, but present on the other. This allows the applications to take full advantage of two-way replication even before the two sites get fully synced.

In the case of multi destination replication, the replication status shows `COMPLETED` only after the replication operation succeeds on each of the targets specified in the replication configuration. If multiple targets are configured to use active-active replication and multi destination replication, the administrator should ensure that the replication features enabled (such as replica metadata sync, delete marker replication etc) are identical to avoid asymmetric state. This is because all replication activity is inherently a one-way operation from source to target, irrespective of the number of targets.

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

Note that objects with `null` versions, i.e. objects created prior to enabling versioning break the immutability guarantees provided by versioning. When existing object replication is enabled, these objects will be replicated as `null` versions to the remote targets provided they are not present on the target or if `null` version of object on source is newer than the `null` version of object on target.

If the remote site is fully lost and objects previously replicated need to be re-synced, the `mc replicate resync start` command with optional flag of `--older-than` needs to be used to trigger re-syncing of previously replicated objects. This command generates a ResetID which is a unique UUID saved to the remote target config along with the applicable date(defaults to time of initiating the reset). All objects created prior to this date are eligible for re-replication if existing object replication is enabled for the replication rule the object satisfies. At the time of completion of replication, `x-minio-internal-replication-reset-arn:<arn>` is set in the metadata with the timestamp of replication and ResetID. For saving iops, the objects which are re-replicated are not first set to `PENDING` state.

This is a slower operation that does not use replication queues and is designed to walk the namespace and replicate objects one at a time so as not to impede server load. Ideally, resync should not be initiated for multiple buckets simultaneously - progress of the syncing can be monitored by looking at `mc replicate resync status alias/bucket --remote-bucket <arn>`. In the event that resync operation failed to replicate some versions, they would be picked up by the healing mechanism in-built as part of the scanner. If the resync operation reports a failed status or in the event of a cluster restart while resync is in progress, a fresh `resync start` can be issued - this will replicate previously unsynced content at the cost of additional overhead in additional metadata updates.

### Multi destination replication

The replication design for multiple sites works in a similar manner as described above for two site scenario. However there are some
important exceptions.

Replication status on the source cluster will be marked as `COMPLETED` only after replication is completed on all targets. If one or more targets failed replication, the replication status is reflected as `PENDING`.

If 3 or more targets are participating in active-active replication, the replication configuration for replica metadata sync, delete marker replication and delete replication should match to avoid inconsistent picture between the clusters. It is not recommended to turn on asymmetric replication - for e.g. if three sites A,B,C are participating in replication, it would be better to avoid replication setups like A -> [B, C], B -> A. In this particular example, an object uploaded to A will be replicated to B,C. If replica metadata sync is turned on in site B, any metadata updates on a replica version made in B would reflect in A, but not in C.

### Internal metadata for replication

`xl.meta` that is in use for [versioning](https://github.com/minio/minio/blob/master/docs/bucket/versioning/DESIGN.md) has additional metadata for replication of objects,delete markers and versioned deletes.

### Metadata for object replication - on source

```
...
  "MetaSys": {
      "x-minio-internal-inline-data": "dHJ1ZQ==",
      "x-minio-internal-replication-status": "YXJuOm1pbmlvOnJlcGxpY2F0aW9uOjo2YjdmYzFlMS0wNmU4LTQxMTUtYjYxNy00YTgzZGIyODhmNTM6YnVja2V0PUNPTVBMRVRFRDthcm46bWluaW86cmVwbGljYXRpb246OmI5MGYxZWEzLWMzYWQtNDEyMy1iYWE2LWZjMDZhYmEyMjA2MjpidWNrZXQ9Q09NUExFVEVEOw==",
      "x-minio-internal-replication-timestamp": "MjAyMS0wOS0xN1QwMTo0MzozOC40MDQwMDA0ODNa",
      "x-minio-internal-tier-free-versionID": "OWZlZjk5N2QtMjMzZi00N2U3LTlkZmMtNWYxNzc3NzdlZTM2"
    },
    "MetaUsr": {
      "X-Amz-Replication-Status": "COMPLETED",
      "content-type": "application/octet-stream",
      "etag": "8315e643ed6a5d7c9962fc0a8ef9c11f"
    },
...
```

### Metadata for object replication - on target

```
...
  "MetaSys": {
      "x-minio-internal-inline-data": "dHJ1ZQ==",
      "x-minio-internal-replica-status": "UkVQTElDQQ==",
      "x-minio-internal-replica-timestamp": "MjAyMS0wOS0xN1QwMTo0MzozOC4zODg5ODU4ODRa"
    },
    "MetaUsr": {
      "X-Amz-Replication-Status": "REPLICA",
      "content-type": "application/octet-stream",
      "etag": "8315e643ed6a5d7c9962fc0a8ef9c11f",
      "x-amz-storage-class": "STANDARD"
    },
...
```

### Additional replication metadata for DeleteMarker

```
...
 {
      "DelObj": {
      "ID": "u8H5pYQFRMKgkIgkpSKIkQ==",
      "MTime": 1631843124147668389,
      "MetaSys": {
        "x-minio-internal-replication-status": "YXJuOm1pbmlvOnJlcGxpY2F0aW9uOjpiOTBmMWVhMy1jM2FkLTQxMjMtYmFhNi1mYzA2YWJhMjIwNjI6YnVja2V0PUNPTVBMRVRFRDthcm46bWluaW86cmVwbGljYXRpb246OjZiN2ZjMWUxLTA2ZTgtNDExNS1iNjE3LTRhODNkYjI4OGY1MzpidWNrZXQ9Q09NUExFVEVEOw==",
        "x-minio-internal-replication-timestamp": "U3VuLCAzMSBEZWMgMDAwMCAxOTowMzo1OCBHTVQ="
      }
    },
    "Type": 2
}
```

### Additional replication metadata for versioned delete

```
{ 
    "DelObj": {
      "ID": "u8H5pYQFRMKgkIgkpSKIkQ==",
      "MTime": 1631843124147668389,
      "MetaSys": {
        "purgestatus": "YXJuOm1pbmlvOnJlcGxpY2F0aW9uOjpiOTBmMWVhMy1jM2FkLTQxMjMtYmFhNi1mYzA2YWJhMjIwNjI6YnVja2V0PUNPTVBMRVRFO2FybjptaW5pbzpyZXBsaWNhdGlvbjo6NmI3ZmMxZTEtMDZlOC00MTE1LWI2MTctNGE4M2RiMjg4ZjUzOmJ1Y2tldD1GQUlMRUQ7",
        "x-minio-internal-replication-status": "YXJuOm1pbmlvOnJlcGxpY2F0aW9uOjpiOTBmMWVhMy1jM2FkLTQxMjMtYmFhNi1mYzA2YWJhMjIwNjI6YnVja2V0PTthcm46bWluaW86cmVwbGljYXRpb246OjZiN2ZjMWUxLTA2ZTgtNDExNS1iNjE3LTRhODNkYjI4OGY1MzpidWNrZXQ9Ow==",
        "x-minio-internal-replication-timestamp": "U3VuLCAzMSBEZWMgMDAwMCAxOTowMzo1OCBHTVQ="
      }
    },
    "Type": 2
}
```

### Additional Metadata for object replication resync - on source

```
...
  "MetaSys": {
    ...
    "x-minio-internal-replication-reset-arn:minio:replication::af470089-d354-4473-934c-9e1f52f6da89:bucket": "TW9uLCAwNyBGZWIgMjAyMiAyMDowMzo0MCBHTVQ7ZGMxMWQzNDgtMTAwMS00ODA3LWFhNjEtOGY2MmFiNWQ5ZjU2",
    ...
  },
...
```

### Additional Metadata for resync replication of delete-markers - on source

```
...
  "MetaSys": {
    "x-minio-internal-replication-reset-arn:minio:replication::af470089-d354-4473-934c-9e1f52f6da89:bucket": "TW9uLCAwNyBGZWIgMjAyMiAyMDowMzo0MCBHTVQ7ZGMxMWQzNDgtMTAwMS00ODA3LWFhNjEtOGY2MmFiNWQ5ZjU2",
  ...
  }
...
```

## Explore Further

- [MinIO Bucket Versioning Implementation](https://docs.min.io/community/minio-object-store/administration/object-management/object-versioning.html)
- [MinIO Client Quickstart Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart)
