## MinIO Server Limits Per Tenant

### Erasure Code (Multiple Drives / Servers)

| Item                                                            | Specification |
|:----------------------------------------------------------------|:--------------|
| Maximum number of servers per cluster                           | no-limit      |
| Maximum number of federated clusters                            | no-limit      |
| Minimum number of servers                                       | 02            |
| Minimum number of drives per server when server count is 1      | 04            |
| Minimum number of drives per server when server count is 2 or 3 | 02            |
| Minimum number of drives per server when server count is 4      | 01            |
| Maximum number of drives per server                             | no-limit      |
| Read quorum                                                     | N/2           |
| Write quorum                                                    | N/2+1         |

### Limits of S3 API

| Item                                                                            | Specification                                 |
|:--------------------------------------------------------------------------------|:----------------------------------------------|
| Maximum number of buckets                                                       | no-limit                                      |
| Maximum number of objects per bucket                                            | no-limit                                      |
| Maximum object size                                                             | 5 TiB                                         |
| Minimum object size                                                             | 0 B                                           |
| Maximum object size per PUT operation                                           | 5 TiB                                         |
| Maximum number of parts per upload                                              | 10,000                                        |
| Part size range                                                                 | 5 MiB to 5 GiB. Last part can be 0 B to 5 GiB |
| Maximum number of parts returned per list parts request                         | 10000                                         |
| Maximum number of objects returned per list objects request                     | 4500                                          |
| Maximum number of multipart uploads returned per list multipart uploads request | 1000                                          |
| Maximum length for bucket names                                                 | 63                                            |
| Maximum length for object names                                                 | 1024                                          |
| Maximum length for '/' seperated object name segment                            | 255                                           |

### List of Amazon S3 API's not supported on MinIO
We found the following APIs to be redundant or less useful outside of AWS S3. If you have a different view on any of the APIs we missed, please open a [GitHub issue](https://github.com/minio/minio/issues).

#### List of Amazon S3 Bucket API's not supported on MinIO

- BucketACL (Use [bucket policies](https://docs.min.io/docs/minio-client-complete-guide#policy) instead)
- BucketCORS (CORS enabled by default on all buckets for all HTTP verbs)
- BucketWebsite (Use [`caddy`](https://github.com/caddyserver/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- BucketAnalytics, BucketMetrics, BucketLogging (Use [bucket notification](https://docs.min.io/docs/minio-client-complete-guide#events) APIs)
- BucketRequestPayment

#### List of Amazon S3 Object API's not supported on MinIO

- ObjectACL (Use [bucket policies](https://docs.min.io/docs/minio-client-complete-guide#policy) instead)
- ObjectTorrent

### Object name restrictions on MinIO
Object names that contain characters `^*|\/&";` are unsupported on Windows and other file systems which do not support filenames with these characters. NOTE: This list is not an exhaustive, it depends on the operating system and filesystem under use.

For best experience we recommend that you use distributions that ship fairly recent Linux kernel such as CentOS 8, Ubuntu 18.04 LTS along with XFS as the choice for your backend filesystem.
