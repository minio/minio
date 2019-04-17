# MinIO HDFS Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
MinIO HDFS gateway adds Amazon S3 API support to Hadoop HDFS filesystem. Applications can use both the S3 and file APIs concurrently without requiring any data migration. Since the gateway is stateless and shared-nothing, you may elastically provision as many MinIO instances as needed to distribute the load.

## Run MinIO Gateway for HDFS Storage

### Using Docker

Please ensure to replace `hdfs://namenode:8200` with *an* actual name node ip/hostname and port.

```
docker run -p 9000:9000 --name hdfs-s3 \
 -e "MINIO_ACCESS_KEY=minio" \
 -e "MINIO_SECRET_KEY=minio123" \
 minio/minio gateway hdfs hdfs://namenode:8200
```

### Using Binary

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
minio gateway hdfs hdfs://namenode:8200
```

## Test using MinIO Browser

*MinIO gateway* comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/minio-browser-gateway.png)

## Test using MinIO Client `mc`

`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`

```
mc config host add myhdfs http://gateway-ip:9000 access_key secret_key
```

### List buckets on hdfs

```
mc ls myhdfs
[2017-02-22 01:50:43 PST]     0B user/
[2017-02-26 21:43:51 PST]     0B datasets/
[2017-02-26 22:10:11 PST]     0B assets/
```

### Known limitations
Gateway inherits the following limitations of HDFS storage layer:
- No bucket policy support (HDFS has no such concept)
- No bucket notification APIs are not supported (HDFS has no support for fsnotify)
- No server side encryption support (Intentionally not implemented)
- No server side compression support (Intentionally not implemented)

## Roadmap
- Additional metadata support for PutObject operations
- Additional metadata support for Multipart operations
- Background append to provide concurrency support for multipart operations

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
