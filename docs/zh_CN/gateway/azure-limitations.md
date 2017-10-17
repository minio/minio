## Minio Azure网关限制

网关继承了下列Azure限制:

- 最大的Multipart part size是100MB.
- 最大的对象大小是10000*100 MB = 1TB
- 不支持针对前缀的存储桶策略，仅支持顶层存储桶策略。
- 网关重启意味着正在进行的multipart上传也需要重新启动，即客户端必须再一次运行NewMultipartUpload。
这是因为S3客户端在NewMultipartUpload中发送元数据，但Azure希望在CompleteMultipartUpload（Azure术语中的PutBlockList）中设置元数据。
  This is because S3 clients send metadata in NewMultipartUpload but Azure expects metadata to
  be set during CompleteMultipartUpload (PutBlockList in Azure terminology). 在NewMultipartUpload期间，我们将客户端发送的元数据存储在内存中，以便以后可以在CompleteMultipartUpload中将其设置在Azure上。如果网关穗启，这些信息也会丢失。
- 存储桶名称不支持以 "."开始。
- 调用DeleteBucket() 可删除非空存储桶。

其它限制:
- 目前ListMultipartUploads实现不完整，只要存在任意上传的parts,就会返回。
- 不支持存储桶通知。
