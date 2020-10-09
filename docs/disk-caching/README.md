# Disk Cache Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Disk caching feature here refers to the use of caching disks to store content closer to the tenants. For instance, if you access an object from a lets say `gateway azure` setup and download the object that gets cached, each subsequent request on the object gets served directly from the cache drives until it expires. This feature allows MinIO users to have

- Object to be delivered with the best possible performance.
- Dramatic improvements for time to first byte for any object.

## Get started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Run MinIO gateway with cache

Disk caching can be enabled by setting the `cache` environment variables for MinIO gateway . `cache` environment variables takes the mounted drive(s) or directory paths, any wildcard patterns to exclude from being cached,low and high watermarks for garbage collection and the minimum accesses before caching an object.

Following example uses `/mnt/drive1`, `/mnt/drive2` ,`/mnt/cache1` ... `/mnt/cache3` for caching, while excluding all objects under bucket `mybucket` and all objects with '.pdf' as extension on a s3 gateway setup. Objects are cached if they have been accessed three times or more.Cache max usage is restricted to 80% of disk capacity in this example. Garbage collection is triggered when high watermark is reached - i.e. at 72% of cache disk usage and clears least recently accessed entries until the disk usage drops to low watermark - i.e. cache disk usage drops to 56% (70% of 80% quota)

```bash
export MINIO_CACHE="on"
export MINIO_CACHE_DRIVES="/mnt/drive1,/mnt/drive2,/mnt/cache{1...3}"
export MINIO_CACHE_EXCLUDE="*.pdf,mybucket/*"
export MINIO_CACHE_QUOTA=80
export MINIO_CACHE_AFTER=3
export MINIO_CACHE_WATERMARK_LOW=70
export MINIO_CACHE_WATERMARK_HIGH=90

minio gateway s3
```

The `CACHE_WATERMARK` numbers are percentages of `CACHE_QUOTA`. 
In the example above this means that  `MINIO_CACHE_WATERMARK_LOW` is effectively `0.8 * 0.7 * 100 = 56%` and the `MINIO_CACHE_WATERMARK_HIGH` is effectively `0.8 * 0.9 * 100 = 72%` of total disk space.     


### 3. Test your setup

To test this setup, access the MinIO gateway via browser or [`mc`](https://docs.min.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from all the MinIO endpoints.

# Explore Further

- [Disk cache design](https://github.com/minio/minio/blob/master/docs/disk-caching/DESIGN.md)
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
