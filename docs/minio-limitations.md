## Minio Server Limits Per Tenant
We found the following APIs to be redundant or less useful outside of AWS. If you have a different view on any of the APIs we missed, please open a [github issue](https://github.com/minio/minio/issues).

### Erasure Code (Multiple Drives / Servers)

|Item|Specification|
|:---|:---|
|Maximum number of drives| 16|
|Minimum number of drives| 4|
|Read quorum| N/2|
|Write quorum| N/2+1|

### Browser Access

|Item|Specification|
|:---|:---|
|Web browser upload size limit| 5GB|

### Limits of S3 API

|Item|Specification|
|:---|:---|
|Maximum number of buckets| no-limit|
|Maximum number of objects per bucket| no-limit|
|Maximum object size|	5 TiB|
|Minimum object size| 0 B|
|Maximum object size per PUT operation| 5 GiB|
|Maximum number of parts per upload| 	10,000|
|Part size|5 MiB to 5 GiB. Last part can be 0 B to 5 GiB|
|Maximum number of parts returned per list parts request| 1000|
|Maximum number of objects returned per list objects request| 1000|
|Maximum number of multipart uploads returned per list multipart uploads request| 1000|

###  List of Amazon S3 Bucket API's not supported on Minio.

- BucketACL (Use bucket policies instead)
- BucketCORS (CORS enabled by default)
- BucketLifecycle (Not required for Minio's XL backend)
- BucketReplication (Use `mc mirror` instead)
- BucketVersions, BucketVersioning (Use `s3git`)
- BucketWebsite (Use `caddy` or `nginx`)
- BucketAnalytics, BucketMetrics, BucketLogging (Use bucket notification APIs)
- BucketRequestPayment
- BucketTagging

### List of Amazon S3 Object API's not supported on Minio.

- ObjectACL (Use bucket policies instead)
- ObjectTorrent
