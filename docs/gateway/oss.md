# Minio OSS Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio Gateway adds Amazon S3 compatibility to Alibaba Cloud Object Storage Service (OSS).

## Run Minio Gateway for OSS.

### Using Docker
```
docker run -p 9000:9000 --name azure-s3 \
 -e "MINIO_ACCESS_KEY=ossaccesskey" \
 -e "MINIO_SECRET_KEY=osssecretkey" \
 minio/minio gateway oss
```

### Using Binary
```
export MINIO_ACCESS_KEY=ossaccesskey
export MINIO_SECRET_KEY=osssecretkey
minio gateway oss
```

## Test using Minio Browser
Minio Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/minio-browser-gateway.png)

## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc config host add myoss http://gateway-ip:9000 ossaccesskey osssecretkey
```

### List buckets on OSS
```
mc ls myb2
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-bucket/
[2017-02-26 22:10:11 PST]     0B test-bucket1/
```

### Known limitations

Gateway inherits the following OSS limitations:

- Bucket names with "." in the bucket name are not supported.
- Custom metadata with "_" in the key is not supported.

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further

- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
