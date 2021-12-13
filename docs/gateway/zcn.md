# MinIO ZCN Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
MinIO ZCN gateway adds Amazon S3 API support to 0chain dStorage filesystem. Applications can use both the S3 and file APIs concurrently without requiring any data migration. Since the gateway is stateless and shared-nothing, you may elastically provision as many MinIO instances as needed to distribute the load.

## Run MinIO Gateway for HDFS Storage
As a prerequisite to run MinIO ZCN gateway, you need a 0chain credentials; wallet.json, config.yaml and allocation.txt.

### Using Binary
```
git clone git@github.com:0chain/minio.git
cd minio
go mod tidy
go build .
export MINIO_ROOT_USER=someminiouser
export MINIO_ROOT_PASSWORD=someminiopassword
./minio gateway zcn --configDir /path/to/config/dir --allocation abc
> Note: allocation and configDir both are optional. By default configDir takes ~/.zcn as configDir and if allocation is not provided in command then configDir/allocation.txt is looked for.
```

## Test using MinIO Console
*MinIO gateway* comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

| Dashboard                                                                                   | Creating a bucket                                                                           |
| -------------                                                                               | -------------                                                                               |
| ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic1.png?raw=true) | ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic2.png?raw=true) |

## Test using MinIO Client `mc`

`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc config host add zcn http://localhost:9000 miniouser miniopassword
mc ls zcn //List your buckets

[2017-02-22 01:50:43 PST]     0B user/
[2017-02-26 21:43:51 PST]     0B datasets/
[2017-02-26 22:10:11 PST]     0B assets/
```

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
