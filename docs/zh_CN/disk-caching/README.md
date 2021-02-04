# 磁盘缓存快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

这里的磁盘缓存功能是指使用缓存磁盘将内容存储在更靠近租户的地方。例如，假设你通过`gateway azure`设置访问一个对象并下载下来进行缓存，那接下来的请求都会直接访问缓存磁盘上的对象，直至其过期失效。此功能允许MinIO用户：
- 对象的读取速度性能最佳。
- 任何对象的首字节时间得到显著改善。

## 开始

### 1. 前置条件
安装MinIO - [MinIO快速入门](https://docs.min.io/cn/minio-quickstart-guide)。

### 2. 运行带缓存的MinIO网关
可以通过设置`cache`环境变量为MinIO网关启用磁盘缓存。配置`cache`环境变量需要指定磁盘路径、使用通配符方式指定的不需要进行缓存的对象、用于缓存垃圾回收的高低水位线以及缓存一个对象前的最小访问次数（译者注：就是对象被访问多少次后才缓存它）。

下面示例使用`/mnt/drive1`, `/mnt/drive2` ,`/mnt/cache1` ... `/mnt/cache3`来做缓存，并且`mybucket`下的所有对象以及后缀名为`.pdf`的对象不做缓存。如果对象被访问过三次及以上，则将其缓存。在此示例中，缓存最大使用量限制为磁盘容量的80％。当达到高水位线的时候垃圾回收会被触发（即缓存磁盘使用率达到72%的时候），这时候会清理最近最少使用的条目直到磁盘使用率降到低水位线为止（即缓存磁盘使用率降到56%）。

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

`CACHE_WATERMARK`的值是`CACHE_QUOTA`的百分比。
在上面的示例中，`MINIO_CACHE_WATERMARK_LOW`实际上是磁盘总空间的`0.8 * 0.7 * 100 = 56%`，`MINIO_CACHE_WATERMARK_LOW`实际上是磁盘总空间的`0.8 * 0.9 * 100 = 72%`。


### 3. 验证设置是否成功
要验证是否部署成功，你可以通过浏览器或者[`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)来访问刚刚部署的MinIO网关。你应该可以看到上传的文件在所有MinIO节点上都可以访问。

# 了解更多
- [磁盘缓存设计](https://github.com/minio/minio/blob/master/docs/zh_CN/disk-caching/DESIGN.md)
- [`mc`快速入门](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用 `s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用 `minio-go` SDK](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO文档](https://docs.min.io/cn/)
