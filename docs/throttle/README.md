# MinIO Server Throttling Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO server allows to throttle incoming requests:

- limit the number of active requests allowed across the cluster
- limit the wait duration for each request in the queue

These values are enabled using server's configuration or environment variables.

## Examples
### Configuring connection limit
If you have traditional spinning (hdd) drives, some applications with high concurrency might require MinIO cluster to be tuned such that to avoid random I/O on the drives. The way to convert high concurrent I/O into a sequential I/O is by reducing the number of concurrent operations allowed per cluster. This allows MinIO cluster to be operationally resilient to such workloads, while also making sure the drives are at optimal efficiency and responsive.

Example: Limit a MinIO cluster to accept at max 1600 simultaneous S3 API requests across all nodes of the cluster.

```sh
export MINIO_API_REQUESTS_MAX=1600
export MINIO_ACCESS_KEY=your-access-key
export MINIO_SECRET_KEY=your-secret-key
minio server http://server{1...8}/mnt/hdd{1...16}
```

or

```sh
mc admin config set myminio/ api requests_max=1600
mc admin service restart myminio/
```

> NOTE: A zero value of `requests_max` means unlimited and that is the default behavior.

### Configuring connection (wait) deadline
This value works in conjunction with max connection setting, setting this value allows for long waiting requests to quickly time out when there is no slot available to perform the request.

This will reduce the pileup of waiting requests when clients are not configured with timeouts. Default wait time is *10 seconds* if *MINIO_API_REQUESTS_MAX* is enabled. This may need to be tuned to your application needs.

Example: Limit a MinIO cluster to accept at max 1600 simultaneous S3 API requests across 8 servers, and set the wait deadline of *2 minutes* per API operation.

```sh
export MINIO_API_REQUESTS_MAX=1600
export MINIO_API_REQUESTS_DEADLINE=2m
export MINIO_ACCESS_KEY=your-access-key
export MINIO_SECRET_KEY=your-secret-key
minio server http://server{1...8}/mnt/hdd{1...16}
```

or

```sh
mc admin config set myminio/ api requests_max=1600 requests_deadline=2m
mc admin service restart myminio/
```

