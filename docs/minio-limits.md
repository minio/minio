## Minio Server Limits Per Tenant

This topic lists the limits for various parameters supported by Minio Server, Amazon S3 APIs that are not supported, and character restrictions for object names.

1. [Supported Number of Drives and Servers for Erasure Code](#supported-number-of-drives-and-servers-for-erasure-code) 
2. [Maximum Browser Upload Size](#maximum-browser-upload-size) 
3. [Maximum Sizes Using the S3 API](#maximum-sizes-using-the-s3-api) 
4. [List of Amazon S3 APIs not Supported on Minio](#list-of-amazon-s3-apis-not-supported-on-Minio) 
5. [Object Name Restrictions](#object-name-restrictions)

### 1. <a name="supported-number-of-drives-and-servers-for-erasure-code"></a>Supported Number of Drives and Servers for Erasure Code

|**Item**|**Limit**|
|:---|:---|
|Maximum number of servers| 32|
|Minimum number of servers| 2|
|Maximum number of drives per server| Unlimited|
|Read quorum| N/2|
|Write quorum| N/2 + 1|

### 2. <a name="maximum-browser-upload-size"></a>Maximum Browser Upload Size 

|**Item**|**Limit**|
|:---|:---|
|Web browser upload size limit| 5 TB|

### 3. <a name="maximum-sizes-using-the-s3-api"></a>Maximum Sizes Using the S3 API

|**Item**|**Limit**|
|:---|:---|
|Maximum number of buckets| no-limit|
|Maximum number of objects per bucket| no-limit|
|Maximum object size| 5 TB|
|Minimum object size| 0 B|
|Maximum object size per PUT operation| 5 TB|
|Maximum number of parts per upload| 	10,000|
|Part size|5 MB to 5 GB. The last part can be 0 B to 5 GB.|
|Maximum number of parts returned per list in a request for parts| 1000|
|Maximum number of objects returned per list in a request for objects| 1000|
|Maximum number of multi-part uploads returned per list in a request for multi-part uploads| 1000|

### 4. <a name="list-of-amazon-s3-apis-not-supported-on-Minio"></a>List of Amazon S3 APIs Not Supported on Minio
The following APIs are considered redundant or less useful outside of AWS S3. If there are additional APIs that should be added to this list, please open a [github issue](https://github.com/minio/minio/issues).

#### 4.1 List of Amazon S3 Bucket APIs Not Supported on Minio:

- `BucketACL` (use [bucket policies](https://docs.minio.io/docs/minio-client-complete-guide#policy) instead)
- `BucketCORS` (CORS is enabled by default on all buckets for all HTTP verbs)
- `BucketLifecycle` (not required for the Minio erasure-coded backend)
- `BucketReplication` (use [`mc mirror`](https://docs.minio.io/docs/minio-client-complete-guide#mirror) instead)
- `BucketVersions`, `BucketVersioning` (use [`s3git`](https://github.com/s3git/s3git))
- `BucketWebsite` (use [`caddy`](https://github.com/mholt/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- `BucketAnalytics`, `BucketMetrics`, `BucketLogging` (use [bucket notification](https://docs.minio.io/docs/minio-client-complete-guide#events) APIs)
- `BucketRequestPayment`
- `BucketTagging`

#### 4.2 List of Amazon S3 Object APIs Not Supported on Minio:

- `ObjectACL` (use [bucket policies](https://docs.minio.io/docs/minio-client-complete-guide#policy) instead)
- `ObjectTorrent`
- `ObjectVersions`

### 5. <a name="object-name-restrictions"></a>Object Name Restrictions
Minio does not support object names that contain any of the following characters: `^*|\/&#";`. This restriction is applicable to file systems that do not support filenames containing these characters. There may be additional filename character restrictions on other file systems.
