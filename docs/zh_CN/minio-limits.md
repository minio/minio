## 每个租户的MinIO服务器限制

### 纠删码 (多块 硬盘/服务器)

|条目|限制|
|:---|:---|
|每个群集的最大服务器数| 不限|
|联盟集群的最大数量 | no-limit|
|最小服务器数| 02|
|服务器数为1时，每台服务器的最小硬盘数 | 04 |
|服务器数为2或3时，每台服务器的最小硬盘数 | 02|
|服务器数为4时，每台服务器的最小硬盘数 | 01|
|每台服务器的最大硬盘数| 不限|
|读仲裁|N / 2|
|写仲裁|N / 2+1 |


### 浏览器访问

|条目|限制|
|:---|:---|
|Web浏览器上传大小限制| 5GB|

### S3 API的限制

|条目|限制|
|:---|:---|
|最大存储桶数|不限|
|每个存储桶的最大对象数|不限|
|最大对象大小| 5 TB |
|最小对象大小| 0 B |
|每次PUT操作的最大对象大小| 5 GB |
|每次上传的最大分片数量| 	10,000|
|分片大小 |5MB到5GB. 最后一个分片可以从0B到5GB|
|每次list分片请求可返回的最大分片数量| 10000|
|每次list 对象请求可返回的最大对象数量| 10000|
|每次list multipart uploads请求可返回的multipart uploads最大数量| 1000|

###  MinIO不支持的Amazon S3 API
我们认为下列AWS S3的API有些冗余或者说用处不大，因此我们在minio中没有实现这些接口。如果您有不同意见，欢迎在[github](https://github.com/minio/minio/issues)上提issue。

#### MinIO不支持的Amazon S3 Bucket API

- BucketACL (可以用 [存储桶策略](https://docs.min.io/cn/minio-client-complete-guide#policy))
- BucketCORS (所有HTTP方法的所有存储桶都默认启用CORS)
- BucketWebsite (可以用 [`caddy`](https://github.com/mholt/caddy) or [`nginx`](https://www.nginx.com/resources/wiki/))
- BucketAnalytics, BucketMetrics, BucketLogging (可以用 [存储桶通知](https://docs.min.io/cn/minio-client-complete-guide#events) API)
- BucketRequestPayment

#### MinIO不支持的Amazon S3 Object API.

- ObjectACL (可以用 [存储桶策略](https://docs.min.io/cn/minio-client-complete-guide#policy))
- ObjectTorrent

### MinIO上的对象名称限制
在Windows上对象名称不能包含`^*|\/&";`这些特殊字符，如果其他系统文件名不支持这些特殊字符，那么在这些系统上对象名称也不能使用这些特殊字符。请注意，此列表并不详尽，并且取决于文档本身的维护者。