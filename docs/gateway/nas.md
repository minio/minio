# Minio NAS Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio Gateway adds Amazon S3 compatibility to NAS storage. You may run multiple minio instances on the same shared NAS volume as a distributed object gateway.

## Run Minio Gateway for NAS Storage
### Using Docker
```
docker run -p 9000:9000 --name nas-s3 \
 -e "MINIO_ACCESS_KEY=minio" \
 -e "MINIO_SECRET_KEY=minio123" \
 minio/minio:edge gateway nas /shared/nasvol
```

### Using Binary
```
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway nas /shared/nasvol
```
## Test using Minio Browser
Minio Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/minio-browser-gateway.png)

## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc config host add mynas http://gateway-ip:9000 access_key secret_key
```

### List buckets on nas
```
mc ls mynas
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-bucket/
[2017-02-26 22:10:11 PST]     0B test-bucket1/
```

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
