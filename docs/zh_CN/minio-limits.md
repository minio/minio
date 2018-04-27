## Minio服务限制/租户

### 纠删码 (多块硬盘 / 服务)


|项目|参数|
|:---|:---|
|最大驱动器数量|16|
|最小驱动器数量|4|
|读仲裁|N / 2|
|写仲裁|N / 2+1 |

### 浏览器访问

|项目|参数|
|:---|:---|
|Web浏览器上传大小限制| 5GB|

### Limits of S3 API

|项目|参数|
|:---|:---|
|最大桶数|无限额|
|每桶最大对象数|无限额|
|最大对象大小| 5 TB |
|最小对象大小| 0 B |
|每次PUT操作的最大对象大小| 5 GB |
|每次上传的最大Part数量| 	10,000|
|Part大小 |5MB到5GB. 最后一个part可以从0B到5GB|
|每次list parts请求可返回的part最大数量| 1000|
|每次list objects请求可返回的object最大数量| 1000|
|每次list multipart uploads请求可返回的multipart uploads最大数量| 1000|

我们认为下列AWS S3的API有些冗余或者说用处不大，因此我们在minio中没有实现这些接口。如果您有不同意见，欢迎在[github](https://github.com/minio/minio/issues)上提issue。

###  Minio不支持的Amazon S3 Bucket API

- BucketACL (可以用 [bucket policies](http://docs.minio.io/docs/minio-client-complete-guide#policy))
- BucketCORS (所有HTTP方法的所有存储桶都默认启用CORS)
- BucketLifecycle (Minio纠删码不需要)
- BucketReplication (可以用 [`mc mirror`](http://docs.minio.io/docs/minio-client-complete-guide#mirror))
- BucketVersions, BucketVersioning (可以用 [`s3git`](https://github.com/s3git/s3git))
- BucketWebsite (可以用 [`caddy`](https://github.com/mholt/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- BucketAnalytics, BucketMetrics, BucketLogging (可以用 [bucket notification](http://docs.minio.io/docs/minio-client-complete-guide#events) APIs)
- BucketRequestPayment
- BucketTagging

### Minio不支持的Amazon S3 Object API.

- ObjectACL (可以用 [bucket policies](http://docs.minio.io/docs/minio-client-complete-guide#policy))
- ObjectTorrent
