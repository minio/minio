# MinIO Server Limits Per Tenant

For optimal production setup MinIO recommends Linux kernel version 4.x and later.

## Erasure Code (Multiple Drives / Servers)

| Item                                                            | Specification |
|:----------------------------------------------------------------|:--------------|
| Maximum number of servers per cluster                           | no-limit      |
| Minimum number of servers                                       | 02            |
| Minimum number of drives per server when server count is 1      | 02            |
| Minimum number of drives per server when server count is 2 or 3 | 01            |
| Minimum number of drives per server when server count is 4      | 01            |
| Maximum number of drives per server                             | no-limit      |
| Read quorum                                                     | N/2           |
| Write quorum                                                    | N/2+1         |

## Limits of S3 API

| Item                                                                            | Specification                                                                   |
|:--------------------------------------------------------------------------------|:--------------------------------------------------------------------------------|
| Maximum number of buckets                                                       | unlimited (we recommend not beyond 500000 buckets) - see NOTE:                  |
| Maximum number of objects per bucket                                            | no-limit                                                                        |
| Maximum object size                                                             | 50 TiB                                                                          |
| Minimum object size                                                             | 0 B                                                                             |
| Maximum object size per PUT operation                                           | 5 TiB                                                                           |
| Maximum number of parts per upload                                              | 10,000                                                                          |
| Part size range                                                                 | 5 MiB to 5 TiB. Last part can be 0 B to 5 TiB                                   |
| Maximum number of parts returned per list parts request                         | 10000                                                                           |
| Maximum number of objects returned per list objects request                     | 1000                                                                            |
| Maximum number of multipart uploads returned per list multipart uploads request | 1000                                                                            |
| Maximum length for bucket names                                                 | 63                                                                              |
| Maximum length for object names                                                 | 1024                                                                            |
| Maximum length for '/' separated object name segment                            | 255                                                                             |
| Maximum number of versions per object                                           | 10000 (can be configured to higher values but we do not recommend beyond 10000) |

> NOTE:  While MinIO does not implement an upper boundary on buckets, your cluster's hardware has natural limits that depend on the workload and its scaling patterns. We strongly recommend [MinIO SUBNET](https://min.io/pricing) for architecture and sizing guidance for your production use case.

## List of Amazon S3 APIs not supported on MinIO

We found the following APIs to be redundant or less useful outside of AWS S3. If you have a different view on any of the APIs we missed, please consider opening a [GitHub issue](https://github.com/minio/minio/issues) with relevant details on why MinIO must implement them.

### List of Amazon S3 Bucket APIs not supported on MinIO

- BucketACL (Use [bucket policies](https://docs.min.io/community/minio-object-store/administration/identity-access-management/policy-based-access-control.html) instead)
- BucketCORS (CORS enabled by default on all buckets for all HTTP verbs, you can optionally restrict the CORS domains)
- BucketWebsite (Use [`caddy`](https://github.com/caddyserver/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- BucketAnalytics, BucketMetrics, BucketLogging (Use [bucket notification](https://docs.min.io/community/minio-object-store/administration/monitoring/bucket-notifications.html) APIs)

### List of Amazon S3 Object APIs not supported on MinIO

- ObjectACL (Use [bucket policies](https://docs.min.io/community/minio-object-store/administration/identity-access-management/policy-based-access-control.html) instead)

## Object name restrictions on MinIO

- Object name restrictions on MinIO are governed by OS and filesystem limitations. For example object names that contain characters `^*|\/&";` are unsupported on Windows platform or any other file systems that do not support filenames with special characters.

> **This list is non exhaustive, it depends on the operating system and filesystem under use - please consult your operating system vendor for a more comprehensive list of special characters**.

MinIO recommends using Linux operating system for production workloads.

- Objects must not have conflicting objects as parent objects, applications using this behavior should change their behavior and use non-conflicting unique keys, for example situations such as following conflicting key patterns are not supported.

```
PUT <bucketname>/a/b/1.txt
PUT <bucketname>/a/b
```

```
PUT <bucketname>/a/b
PUT <bucketname>/a/b/1.txt
```
