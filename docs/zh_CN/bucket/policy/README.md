## Access Policy

This package implements parsing and validating bucket access policies based on Access Policy Language specification - http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

### Supports following effects.

    Allow
    Deny

### Supports following set of operations.

    s3:GetObject
    s3:ListBucket
    s3:PutObject
    s3:GetBucketLocation
    s3:DeleteObject
    s3:AbortMultipartUpload
    s3:ListBucketMultipartUploads
    s3:ListMultipartUploadParts

### Supports following conditions.

    StringEquals
    StringNotEquals
    StringLike
    StringNotLike
    IpAddress
    NotIpAddress

Supported applicable condition keys for each conditions.

    s3:prefix
    s3:max-keys
    aws:Referer
    aws:SourceIp

### Nested policy support.

Nested policies are not allowed.
