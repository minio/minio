# 磁盘缓存设计 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

本文档介绍了一些基本假设和设计方法，以及磁盘缓存功能的限制。如果您希望入门使用磁盘缓存，建议您先阅读[入门文档](https://github.com/minio/minio/blob/master/docs/zh_CN/disk-caching/README.md)。

## 命令行

```
minio gateway <name> -h
...
...
  CACHE:
     MINIO_CACHE_DRIVES: List of mounted cache drives or directories delimited by ","
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ","
     MINIO_CACHE_QUOTA: Maximum permitted usage of the cache in percentage (0-100).
     MINIO_CACHE_AFTER: Minimum number of access before caching an object.
     MINIO_CACHE_WATERMARK_LOW: % of cache quota at which cache eviction stops
     MINIO_CACHE_WATERMARK_HIGH: % of cache quota at which cache eviction starts
     MINIO_CACHE_RANGE: set to "on" or "off" caching of independent range requests per object, defaults to "on"


...
...

  Start MinIO gateway to s3 with edge caching enabled on '/mnt/drive1', '/mnt/drive2' and '/mnt/export1 ... /mnt/export24',
     exclude all objects under 'mybucket', exclude all objects with '.pdf' as extension. Cache only those objects accessed atleast 3 times. Garbage collection triggers in at high water mark (i.e. cache disk usage reaches 90% of cache quota) or at 72% and evicts oldest objects by access time until low watermark is reached ( 70% of cache quota) , i.e. 63% of disk usage.
     $ export MINIO_CACHE_DRIVES="/mnt/drive1,/mnt/drive2,/mnt/export{1..24}"
     $ export MINIO_CACHE_EXCLUDE="mybucket/*,*.pdf"
     $ export MINIO_CACHE_QUOTA=80
     $ export MINIO_CACHE_AFTER=3
     $ export MINIO_CACHE_WATERMARK_LOW=70
     $ export MINIO_CACHE_WATERMARK_HIGH=90

     $ minio gateway s3
```

### 在Docker容器上运行具有缓存的MinIO网关
### Stable
缓存驱动器需要为磁盘缓存功能启用`strictatime` 或者 `relatime`。在此示例中，在启用了`strictatime`或者`relatime`的情况下，将xfs文件系统挂载在/mnt/cache上。

```
truncate -s 4G /tmp/data
mkfs.xfs /tmp/data     # build xfs filesystem on /tmp/data
sudo mkdir /mnt/cache  # create mount dir
sudo mount -o relatime /tmp/data /mnt/cache # mount xfs on /mnt/cache with atime.
docker pull minio/minio
docker run --net=host -e MINIO_ACCESS_KEY={s3-access-key} -e MINIO_SECRET_KEY={s3-secret-key} -e MINIO_CACHE_DRIVES=/cache -e MINIO_CACHE_QUOTA=99 -e MINIO_CACHE_AFTER=0 -e MINIO_CACHE_WATERMARK_LOW=90 -e MINIO_CACHE_WATERMARK_HIGH=95  -v /mnt/cache:/cache  minio/minio:latest gateway s3

```

## 假设

- 磁盘缓存配额默认为驱动器容量的80％。
- 缓存驱动器必须是启用[`atime`](http://kerolasa.github.io/filetimes.html)支持的文件系统挂载点。另外，可以在MINIO_CACHE_DRIVES中指定具有atime支持的可写目录。
- 每当缓存磁盘使用率相对于配置的缓存配额达到高水位线时，就会发生垃圾收集扫描，GC会清理最近访问最少的对象，直到达到相对于配置的缓存配额的低水位线为止。垃圾收集每隔30分钟运行一次缓存清理扫描。
- 仅在驱动器具有足够的磁盘空间时才缓存对象。

## 行为

磁盘缓存为**下载的**对象缓存，即

- 下载时如果在缓存中找不到的该条目，则缓存为新对象，否则从缓存中获取。
- 当从缓存中获取对象时，也会将Bitrot保护添加到缓存的内容中并进行验证。
- 删除对象时，也会删除缓存中的相应条目（如果有）。
- 后端离线时，缓存可继续用于诸如GET，HEAD之类的只读操作。
- Cache-Control和Expires头信息可用于控制对象在缓存中保留的时间。直到满足Cache-Control或Expires的到期时间，才会使用后端验证缓存对象的ETag。
- 默认情况下，所有range GET请求都会单独缓存 ，但是这并不适用于所有情形，比如当缓存存储受到限制时，因为这种情形下一次性下载整个对象才是最佳选择。 可以选择通过`export MINIO_CACHE_RANGE=off`关闭此功能，以便可以在后台下载整个对象。
- 为了确保安全性，通常不缓存加密对象。但是，如果希望加密磁盘上的缓存内容，则可以通过MINIO_CACHE_ENCRYPTION_MASTER_KEY环境变量设置一个缓存KMS主密钥，以自动加密所有缓存内容。

  请注意，不建议将缓存KMS主密钥用于生产部署中。如果MinIO server/gateway 机器曾经遭到破解，则缓存KMS主密钥也必须被视为受到破解。
  支持外部KMS来管理缓存KMS密钥已经在计划中，这对于生产部署而言将是理想的选择。

> 注意：根据上述配置的间隔，过期会自动发生，经常访问的对象在缓存中的存活时间会更长。

### Crash 恢复

minio进程被杀死或者崩溃后重启minio网关，磁盘缓存将自动恢复。垃圾回收周期将恢复，并且任何先前缓存的条目都可以正常使用。

## 限制

- 存储桶策略未缓存，因此后端离线时不支持匿名操作。
- 使用确定性的散列，对象会被分配到已配置的缓存驱动器上。如果一个或多个驱动器离线，或者更改了缓存驱动器配置，则性能可能会降低到线性查找时间，具体取决于缓存中的磁盘数量。
