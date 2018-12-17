# Disk Caching Design [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This topic explains the attributes and requirements, design approach, and limits of the disk caching feature. To get started with the disk caching feature, see the [Disk Cache Quickstart Guide](https://github.com/minio/minio/blob/master/docs/disk-caching/README.md).

- [General Command-line Usage](#general-command-line-usage) 
- [Attributes and Requirements](#attributes-and-requirements) 
- [Behavior](#behavior) 
- [Limits](#limits) 

## <a name="general-command-line-usage"></a>1. General Command-line Usage

The following shows the general usage for Minio Server:

```
minio server -h
...
...
  CACHE:
     MINIO_CACHE_DRIVES: List of mounted cache drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache as a percentage (0-100).
...
...

  7. Start Minio Server with edge caching enabled on '/mnt/drive1', '/mnt/drive2' and '/mnt/export1 ... /mnt/export24',
     exclude all objects under 'mybucket', exclude all objects with '.pdf' as an extension
     with an expiry of up to 40 days.
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/export{1..24}"
     $ export MINIO_CACHE_EXCLUDE="mybucket/*;*.pdf"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
     $ minio server /home/shared
```

## <a name="attributes-and-requirements"></a>2. Attributes and Requirements

The disk cache feature has the following attributes and requirements:
* The disk cache size defaults to 80% of the drive's capacity.
* A cache drive must be a file system mount point with [`atime`](http://kerolasa.github.io/filetimes.html) support enabled on the drive. Alternatively, writable directories with `atime` support can be specified via `MINIO_CACHE_DRIVES`.
* The expiration of each cached entry takes a user-provided expiry as a hint, and defaults to 90 days if not provided.
* The garbage collector sweeps the expired cache entries whenever cache usage is great than 80% of the drive's capacity. Garbage collection continues until sufficient disk space is reclaimed.
* An object is only cached when the drive has sufficient disk space.

## <a name="behavior"></a>3. Behavior

The disk caching feature caches objects for both **uploaded** and **downloaded** objects and behaves as follows:
* New objects are cached for entries that are not found in the cache while downloading; otherwise the object is served from the cache.
* All objects that have been successfully uploaded are cached, replacing an existing cached entry of the same object if required.
* When an object is deleted, the corresponding entry in the cache, if any, is deleted as well.
* The cache continues to work for read-only operations such as GET and HEAD when the backend is offline.
* The cache disallows write operations when the backend is offline.

**Note:** Expiration happens automatically based on the configured interval as explained above. Objects that are frequently accessed stay alive in the cache for a significantly longer time.

### 3.1 Crash Recovery
Upon restarting Minio Server after a running Minio process is killed or crashes, disk caching resumes automatically. The garbage collection cycle also resumes and any previously cached entries are served from the cache.

## <a name="limits"></a>4. Limits

Disk caching has the following limitations:
* Bucket policies are not cached, so anonymous operations are not supported when the backend is offline.
* Objects are distributed using deterministic hashing among the list of configured cache drives. If one or more drives go offline, or the cache drive configuration is altered, performance may degrade to a linear lookup time depending on the number of disks in the cache.
