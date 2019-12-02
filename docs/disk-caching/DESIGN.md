# Disk Caching Design [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

This document explains some basic assumptions and design approach, limits of the disk caching feature. If you're looking to get started with disk cache, we suggest you go through the [getting started document](https://github.com/minio/minio/blob/master/docs/disk-caching/README.md) first.

## Command-line

```
minio gateway <name> -h
...
...
  CACHE:
     MINIO_CACHE_DRIVES: List of mounted cache drives or directories delimited by ","
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ","
     MINIO_CACHE_EXPIRY: Cache expiry duration in days
     MINIO_CACHE_QUOTA: Maximum permitted usage of the cache in percentage (0-100).
...
...

  7. Start MinIO gateway to s3 with edge caching enabled on '/mnt/drive1', '/mnt/drive2' and '/mnt/export1 ... /mnt/export24',
     exclude all objects under 'mybucket', exclude all objects with '.pdf' as extension
     with expiry up to 40 days.
     $ export MINIO_CACHE_DRIVES="/mnt/drive1,/mnt/drive2,/mnt/export{1..24}"
     $ export MINIO_CACHE_EXCLUDE="mybucket/*,*.pdf"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_QUOTA=80
     $ minio gateway s3
```

## Assumptions

- Disk cache size defaults to 80% of your drive capacity.
- The cache drives are required to be a filesystem mount point with [`atime`](http://kerolasa.github.io/filetimes.html) support to be enabled on the drive. Alternatively writable directories with atime support can be specified in MINIO_CACHE_DRIVES
- Expiration of each cached entry takes user provided expiry as a hint, and defaults to 90 days if not provided.
- Garbage collection sweep of the expired cache entries happens whenever cache usage is > 80% of drive capacity, GC continues until sufficient disk space is reclaimed.
- An object is only cached when drive has sufficient disk space.

## Behavior

Disk caching caches objects for **downloaded** objects i.e

- Caches new objects for entries not found in cache while downloading. Otherwise serves from the cache.
- Bitrot protection is added to cached content and verified when object is served from cache.
- When an object is deleted, corresponding entry in cache if any is deleted as well.
- Cache continues to work for read-only operations such as GET, HEAD when backend is offline.
- Cache-Control and Expires headers can be used to control how long objects stay in the cache. ETag of cached objects are not validated with backend until expiry time as per the Cache-Control or Expires header is met.
- To ensure security guarantees, encrypted objects are normally not cached. However, if you wish to encrypt cached content on disk, you can set MINIO_CACHE_ENCRYPTION_MASTER_KEY environment variable to set a cache KMS
master key to automatically encrypt all cached content.

  Note that cache KMS master key is not recommended for use in production deployments. If the MinIO server/gateway machine is ever compromised, the cache KMS master key must also be treated as compromised.
  Support for external KMS to manage cache KMS keys is on the roadmap,and would be ideal for production use cases.

> NOTE: Expiration happens automatically based on the configured interval as explained above, frequently accessed objects stay alive in cache for a significantly longer time.

### Crash Recovery

Upon restart of minio gateway after a running minio process is killed or crashes, disk caching resumes automatically. The garbage collection cycle resumes and any previously cached entries are served from cache.

## Limits

- Bucket policies are not cached, so anonymous operations are not supported when backend is offline.
- Objects are distributed using deterministic hashing among the list of configured cache drives. If one or more drives go offline, or cache drive configuration is altered in any way, performance may degrade to a linear lookup time depending on the number of disks in cache.
