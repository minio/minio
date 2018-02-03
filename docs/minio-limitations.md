## Minio Server Limits Per Tenant

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
|Web browser upload size limit| 5GiB|

### Limits of S3 API

|Item|Specification|
|:---|:---|
|Maximum number of buckets| no-limit|
|Maximum number of objects per bucket| no-limit|
|Maximum object size|	5 TiB|
|Minimum object size| 0 B|
|Maximum object size per PUT operation| 5 TiB|
|Maximum number of parts per upload| 	10,000|
|Part size|5 MiB to 5 GiB. Last part can be 0 B to 5 GiB|
|Maximum number of parts returned per list parts request| 1000|
|Maximum number of objects returned per list objects request| 1000|
|Maximum number of multipart uploads returned per list multipart uploads request| 1000|

We found the following APIs to be redundant or less useful outside of AWS S3. If you have a different view on any of the APIs we missed, please open a [github issue](https://github.com/minio/minio/issues).

###  List of Amazon S3 Bucket API's not supported on Minio.

- BucketACL (Use [bucket policies](http://docs.minio.io/docs/minio-client-complete-guide#policy) instead)
- BucketCORS (CORS enabled by default on all buckets for all HTTP verbs)
- BucketLifecycle (Not required for Minio erasure coded backend)
- BucketReplication (Use [`mc mirror`](http://docs.minio.io/docs/minio-client-complete-guide#mirror) instead)
- BucketVersions, BucketVersioning (Use [`s3git`](https://github.com/s3git/s3git))
- BucketWebsite (Use [`caddy`](https://github.com/mholt/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- BucketAnalytics, BucketMetrics, BucketLogging (Use [bucket notification](http://docs.minio.io/docs/minio-client-complete-guide#events) APIs)
- BucketRequestPayment
- BucketTagging

### List of Amazon S3 Object API's not supported on Minio.

- ObjectACL (Use [bucket policies](http://docs.minio.io/docs/minio-client-complete-guide#policy) instead)
- ObjectTorrent

### Object name restrictions on Minio.

Object names that contain characters `^*|\&#34; are unsupported on Windows and other file systems which do not support filenames with these characters.
