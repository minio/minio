## Minio Azure Gateway Limitations

Gateway inherits the following Azure limitations:

- No support for prefix based bucket policies. Only top level bucket policy is supported.
- Gateway restart implies all the ongoing multipart uploads must be restarted.
  i.e clients must again start with NewMultipartUpload
  This is because S3 clients send metadata in NewMultipartUpload but Azure expects metadata to
  be set during CompleteMultipartUpload (PutBlockList in Azure terminology). We store the metadata
  sent by the client during NewMultipartUpload in memory so that it can be set on Azure later during
  CompleteMultipartUpload. When the gateway is restarted this information is lost.
- Bucket names with "." in the bucket name is not supported.
- Non-empty buckets get removed on a DeleteBucket() call.

Other limitations:
- Current implementation of ListMultipartUploads is incomplete. Right now it returns if the object with name "prefix" has any uploaded parts.
- Bucket notification not supported.
