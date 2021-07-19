# MinIO NAS Gateway [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO Gateway adds Amazon S3 compatibility to NAS storage. You may run multiple minio instances on the same shared NAS volume as a distributed object gateway.

## Run MinIO Gateway for NAS Storage

### Using Docker

Please ensure to replace `/shared/nasvol` with actual mount path.

```
podman run \
 -p 9000:9000 \
 -p 9001:9001 \
 --name nas-s3 \
 -e "MINIO_ROOT_USER=minio" \
 -e "MINIO_ROOT_PASSWORD=minio123" \
 -v /shared/nasvol:/container/vol \
 minio/minio gateway nas /container/vol --console-address ":9001"
```

### Using Binary

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
minio gateway nas /shared/nasvol
```

## Test using MinIO Console

MinIO Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

| Dashboard                                                                                   | Creating a bucket                                                                           |
| -------------                                                                               | -------------                                                                               |
| ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic1.png?raw=true) | ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic2.png?raw=true) |

## Test using MinIO Client `mc`

`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`

```
mc alias set mynas http://gateway-ip:9000 access_key secret_key
```

### List buckets on nas

```
mc ls mynas
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-bucket/
[2017-02-26 22:10:11 PST]     0B test-bucket1/
```

## Breaking changes

There will be a breaking change after the release version 'RELEASE.2020-06-22T03-12-50Z'.

### The file-based config settings are deprecated in NAS

The support for admin config APIs will be removed. This will include getters and setters like `mc admin config get` and `mc admin config`  and any other `mc admin config` options. The reason for this change is to avoid un-necessary reloads of the config from the disk. And to comply with the Environment variable based settings like other gateways.

### Migration guide

The users who have been using the older config approach should migrate to ENV settings by setting environment variables accordingly.

For example,

Consider the following webhook target config.

```
notify_webhook:1 endpoint=http://localhost:8080/ auth_token= queue_limit=0 queue_dir=/tmp/webhk client_cert= client_key=
```

The corresponding environment variable setting can be

```
export MINIO_NOTIFY_WEBHOOK_ENABLE_1=on
export MINIO_NOTIFY_WEBHOOK_ENDPOINT_1=http://localhost:8080/
export MINIO_NOTIFY_WEBHOOK_QUEUE_DIR_1=/tmp/webhk
```

> NOTE: Please check the docs for the corresponding ENV setting. Alternatively, We can obtain other ENVs in the form `mc admin config set alias/ <sub-sys> --env`

## Symlink support

NAS gateway implementation allows symlinks on regular files,

### Behavior

- For reads symlink resolves to file symlink points to.
- For deletes
  - Delete of symlink deletes the symlink but not the real file to which the symlink points.
  - Delete of actual file automatically makes symlink'ed file invisible, dangling symlinks won't be visible.

#### Caveats
- Disallows follow of directory symlinks to avoid security issues, and leaving them as is on namespace makes them very inconsistent.
- Dangling symlinks are ignored automatically.

*Directory symlinks is not and will not be supported as there are no safe ways to handle them.*

## Explore Further
- [`mc` command-line interface](https://docs.min.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.min.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.min.io/docs/golang-client-quickstart-guide)
