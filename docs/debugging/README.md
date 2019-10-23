# MinIO Server Debugging Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

### HTTP Trace
HTTP tracing can be enabled by using [`mc admin trace`](https://github.com/minio/mc/blob/master/docs/minio-admin-complete-guide.md#command-trace---display-minio-server-http-trace) command.

Example:
```sh
minio server /data
```

Default trace is succinct only to indicate the API operations being called and the HTTP response status.
```sh
mc admin trace myminio
```

To trace entire HTTP request
```sh
mc admin trace --verbose myminio
```

To trace entire HTTP request and also internode communication
```sh
mc admin trace --all --verbose myminio
```

