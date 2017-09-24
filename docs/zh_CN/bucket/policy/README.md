## 访问策略

该包实现基于访问策略语言规范（Access Policy Language specification）的解析和验证存储桶访问策略 - http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

### 支持以下效果

    Allow
    Deny

### 支持以下操作

    s3:GetObject
    s3:ListBucket
    s3:PutObject
    s3:GetBucketLocation
    s3:DeleteObject
    s3:AbortMultipartUpload
    s3:ListBucketMultipartUploads
    s3:ListMultipartUploadParts

### 支持下列条件

    StringEquals
    StringNotEquals
    StringLike
    StringNotLike
    IpAddress
    NotIpAddress

每个条件支持的key

    s3:prefix
    s3:max-keys
    aws:Referer
    aws:SourceIp

### 是否支持嵌套策略

不支持嵌套策略
