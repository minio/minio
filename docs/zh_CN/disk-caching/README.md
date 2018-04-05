# 磁盘缓存快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

这里的磁盘缓存功能是指使用缓存磁盘来存储租户常用的一些数据。例如，假设你通过`gateway azure`设置访问一个对象并下载下来进行缓存，那接下来的请求都会直接访问缓存磁盘上的对象，直至其过期失效。此功能允许Minio用户：
- 对象的读取速度性能最佳。
- 任何对象的首字节时间得到显著改善。

## 开始

### 1. 前期条件
安装Minio - [Minio快速入门](https://docs.minio.io/docs/minio)。

### 2. 运行Minio缓存
磁盘缓存可以通过修改Minio服务的`cache`配置来进行开启。配置`cache`设置需要指定磁盘路径、缓存过期时间（以天为单位）以及使用统配符方式指定的不需要进行缓存的对象。

```json
"cache": {
	"drives": ["/mnt/drive1", "/mnt/drive2", "/mnt/drive3"],
	"expiry": 90,
	"exclude": ["*.pdf","mybucket/*"]
},
```

缓存设置也可以通过环境变量设置。设置后，环境变量会覆盖任何`cache`配置中的值。下面示例使用`/mnt/drive1`, `/mnt/drive2` 和 `/mnt/drive3`来做缓存，90天失效，并且`mybucket`下的所有对象以及后缀名为`.pdf`的对象不做缓存。

```bash
export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3"
export MINIO_CACHE_EXPIRY=90
export MINIO_CACHE_EXCLUDE="*.pdf;mybucket/*"
minio server /export{1...24}
```

### 3. 验证设置是否成功
要验证是否部署成功，你可以通过浏览器或者[`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide)来访问刚刚部署的Minio服务。你应该可以看到上传的文件在所有Minio节点上都可以访问。

# 了解更多
- [磁盘缓存设计](https://github.com/minio/minio/blob/master/docs/disk-caching/DESIGN.md)
- [mc快速入门](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [使用 aws-cli](https://docs.minio.io/docs/aws-cli-with-minio)
- [使用 s3cmd](https://docs.minio.io/docs/s3cmd-with-minio)
- [使用 minio-go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [Minio文档](https://docs.minio.io)
