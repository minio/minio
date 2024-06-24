# CORS settings for MinIO

There are two ways to apply CORS in MinIO:

1. Global CORS settings

Global CORS applies to every API endpoint that the server exposes. The only configuration that is supported is the list of allowed origins in `cors_allow_origin` or env var `MINIO_API_CORS_ALLOW_ORIGIN`, as a server parameter. This is the original method.

2. Bucket-specific CORS configuration

This option allows for more fine-grained control over CORS, and can be applied on a per-bucket basis. It uses the same spec as S3, that allows up to 100 rules per bucket. See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html

- If a bucket does not have a CORS configuration, then any CORS requests for that bucket will fall back to the global CORS settings.
- Otherwise, if the bucket does have a CORS config, then CORS requests for that bucket will use the config only, and not fall back to global CORS.

## Configuring

The API calls are `PutBucketCors`, `GetBucketCors`, `DeleteBucketCors`, same as AWS.

- The `minio-go` SDK has methods to make these calls.
- `mc` CLI tool exposes commands that you can use to manage CORS for a given bucket.
