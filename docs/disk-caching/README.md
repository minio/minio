# Disk Cache Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Disk caching feature here refers to the use of caching disks to store content closer to the tenants. For instance, if you access an object from a lets say `gateway azure` setup and download the object that gets cached, each subsequent request on the object gets served directly from the cache drives until it expires. This feature allows MinIO users to have

- Object to be delivered with the best possible performance.
- Dramatic improvements for time to first byte for any object.

## Get started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Run MinIO gateway with cache

Disk caching can be enabled by setting the `cache` environment variables for MinIO gateway . `cache` environment variables takes the mounted drive(s) or directory paths, cache expiry duration (in days) and any wildcard patterns to exclude from being cached.

Following example uses `/mnt/drive1`, `/mnt/drive2` ,`/mnt/cache1` ... `/mnt/cache3` for caching, with expiry up to 90 days while excluding all objects under bucket `mybucket` and all objects with '.pdf' as extension while starting a s3 gateway setup. Cache max usage is restricted to 80% of disk capacity in this example.

```bash
export MINIO_CACHE="on"
export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/cache{1...3}"
export MINIO_CACHE_EXPIRY=90
export MINIO_CACHE_EXCLUDE="*.pdf;mybucket/*"
export MINIO_CACHE_QUOTA=80
minio gateway s3
```

### 3. Test your setup

To test this setup, access the MinIO gateway via browser or [`mc`](https://docs.min.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from all the MinIO endpoints.

# Explore Further

- [Disk cache design](https://github.com/minio/minio/blob/master/docs/disk-caching/DESIGN.md)
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
