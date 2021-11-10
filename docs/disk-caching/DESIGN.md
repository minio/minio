# Disk Caching Design [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

This document explains some basic assumptions and design approach, limits of the disk caching feature. If you're looking to get started with disk cache, we suggest you go through the [getting started document](https://github.com/minio/minio/blob/master/docs/disk-caching/README.md) first.

## Supported environment variables

| Environment                | Description                                                                             |
| :----------------------    | ------------------------------------------------------------                            |
| MINIO_CACHE_DRIVES         | list of mounted cache drives or directories separated by ","                            |
| MINIO_CACHE_EXCLUDE        | list of cache exclusion patterns separated by ","                                       |
| MINIO_CACHE_QUOTA          | maximum permitted usage of the cache in percentage (0-100)                              |
| MINIO_CACHE_AFTER          | minimum number of access before caching an object                                       |
| MINIO_CACHE_WATERMARK_LOW  | % of cache quota at which cache eviction stops                                          |
| MINIO_CACHE_WATERMARK_HIGH | % of cache quota at which cache eviction starts                                         |
| MINIO_CACHE_RANGE          | set to "on" or "off" caching of independent range requests per object, defaults to "on" |
| MINIO_CACHE_COMMIT         | set to 'writeback' or 'writethrough' for upload caching                                 |

## Use-cases

The edge serves as a gateway cache, creating an intermediary between the application and the public cloud. In this scenario, the gateways are backed by servers with a number of either hard drives or flash drives and are deployed in edge data centers. All access to the public cloud goes through these caches (write-through cache), so data is uploaded to the public cloud with strict consistency guarantee. Subsequent reads are served from the cache based on ETAG match or the cache control headers.

This architecture reduces costs by decreasing the bandwidth needed to transfer data, improves performance by keeping data cached closer to the application and also reduces the operational cost - the data is still kept in the public cloud, just cached at the edge.

Following example shows:

- start MinIO gateway to s3 with edge caching enabled on '/mnt/drive1', '/mnt/drive2' and '/mnt/export1 ... /mnt/export24' drives
- exclude all objects under 'mybucket', exclude all objects with '.pdf' as extension.
- cache only those objects accessed atleast 3 times.
- cache garbage collection triggers in at high water mark (i.e. cache disk usage reaches 90% of cache quota) or at 72% and evicts oldest objects by access time until low watermark is reached ( 70% of cache quota) , i.e. 63% of disk usage.

```sh
export MINIO_CACHE_DRIVES="/mnt/drive1,/mnt/drive2,/mnt/export{1..24}"
export MINIO_CACHE_EXCLUDE="mybucket/*,*.pdf"
export MINIO_CACHE_QUOTA=80
export MINIO_CACHE_AFTER=3
export MINIO_CACHE_WATERMARK_LOW=70
export MINIO_CACHE_WATERMARK_HIGH=90

minio gateway s3 https://s3.amazonaws.com
```

### Run MinIO gateway with cache on Docker Container
### Stable
Cache drives need to have `strictatime` or `relatime` enabled for disk caching feature. In this example, mount the xfs file system on /mnt/cache with `strictatime` or `relatime` enabled.

```sh
truncate -s 4G /tmp/data
```

### Build xfs filesystem on /tmp/data
```
mkfs.xfs /tmp/data
```

### Create mount dir
```
sudo mkdir /mnt/cache  #
```

### Mount xfs on /mnt/cache with atime.
```
sudo mount -o relatime /tmp/data /mnt/cache
```

### Start using the cached drive with S3 gateway
```
podman run --net=host -e MINIO_ROOT_USER={s3-access-key} -e MINIO_ROOT_PASSWORD={s3-secret-key} \
    -e MINIO_CACHE_DRIVES=/cache -e MINIO_CACHE_QUOTA=99 -e MINIO_CACHE_AFTER=0 \
    -e MINIO_CACHE_WATERMARK_LOW=90 -e MINIO_CACHE_WATERMARK_HIGH=95 \
    -v /mnt/cache:/cache  quay.io/minio/minio gateway s3 --console-address ":9001"
```

## Assumptions

- Disk cache quota defaults to 80% of your drive capacity.
- The cache drives are required to be a filesystem mount point with [`atime`](http://kerolasa.github.io/filetimes.html) support to be enabled on the drive. Alternatively writable directories with atime support can be specified in MINIO_CACHE_DRIVES
- Garbage collection sweep happens whenever cache disk usage reaches high watermark with respect to the configured cache quota , GC evicts least recently accessed objects until cache low watermark is reached with respect to the configured cache quota. Garbage collection runs a cache eviction sweep at 30 minute intervals.
- An object is only cached when drive has sufficient disk space.

## Behavior

Disk caching caches objects for **downloaded** objects i.e

- Caches new objects for entries not found in cache while downloading. Otherwise serves from the cache.
- Bitrot protection is added to cached content and verified when object is served from cache.
- When an object is deleted, corresponding entry in cache if any is deleted as well.
- Cache continues to work for read-only operations such as GET, HEAD when backend is offline.
- Cache-Control and Expires headers can be used to control how long objects stay in the cache. ETag of cached objects are not validated with backend until expiry time as per the Cache-Control or Expires header is met.
- All range GET requests are cached by default independently, this may be not desirable in all situations when cache storage is limited and where downloading an entire object at once might be more optimal. To optionally turn this feature off, and allow downloading entire object in the background `export MINIO_CACHE_RANGE=off`.
- To ensure security guarantees, encrypted objects are normally not cached. However, if you wish to encrypt cached content on disk, you can set MINIO_CACHE_ENCRYPTION_MASTER_KEY environment variable to set a cache KMS
master key to automatically encrypt all cached content.

  Note that cache KMS master key is not recommended for use in production deployments. If the MinIO server/gateway machine is ever compromised, the cache KMS master key must also be treated as compromised.
  Support for external KMS to manage cache KMS keys is on the roadmap,and would be ideal for production use cases.

- `MINIO_CACHE_COMMIT` setting of `writethrough` allows caching of single and multipart uploads synchronously if enabled. By default, however single PUT operations are cached asynchronously on write without any special setting.

- Partially cached stale uploads older than 24 hours are automatically cleaned up.

- Expiration happens automatically based on the configured interval as explained above, frequently accessed objects stay alive in cache for a significantly longer time.

> NOTE: `MINIO_CACHE_COMMIT` also has a value of `writeback` which allows staging single uploads in cache before committing to remote. It is not possible to stage multipart uploads in the cache for consistency reasons - hence, multipart uploads will be cached synchronously even if `writeback` is set.

### Crash Recovery

Upon restart of minio gateway after a running minio process is killed or crashes, disk caching resumes automatically. The garbage collection cycle resumes and any previously cached entries are served from cache.

## Limits

- Bucket policies are not cached, so anonymous operations are not supported when backend is offline.
- Objects are distributed using deterministic hashing among the list of configured cache drives. If one or more drives go offline, or cache drive configuration is altered in any way, performance may degrade to O(n) lookup time depending on the number of disks in cache.
