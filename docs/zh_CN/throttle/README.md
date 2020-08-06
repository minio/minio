# MinIO服务器限制设置指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO服务器允许限制传入的请求：

- 限制整个集群中允许的活动请求数
- 限制队列中每个请求的等待时间

这些值可以通过服务器的配置或者环境变量启用。

## 示例
### 配置连接限制
如果您使用传统的机械（hdd）硬盘，则某些具有高并发性的应用程序可能需要调整MinIO群集，以避免驱动器上出现随机I/O。将高并发I/O转换为顺序I/O的方法是通过减少每个集群允许的并发操作数。这使MinIO集群在操作上可以应对此类工作负载，同时还可以确保驱动器具有最佳效率和响应能力。

示例：限制MinIO群集在群集的所有节点上最多接受1600个S3兼容的API请求。

```sh
export MINIO_API_REQUESTS_MAX=1600
export MINIO_ACCESS_KEY=your-access-key
export MINIO_SECRET_KEY=your-secret-key
minio server http://server{1...8}/mnt/hdd{1...16}
```

或者

```sh
mc admin config set myminio/ api requests_max=1600
mc admin service restart myminio/
```

> 注意: `requests_max`为零意味着无限制，这也是默认的行为。

### 配置连接（等待）期限
此值与最大连接设置一起使用，设置此值可允许长时间等待的请求，在没有可用空间执行请求时快速超时。

当客户端未配置超时时，这将减少等待请求的堆积。如果启用了*MINIO_API_REQUESTS_MAX*，则默认等待时间为*10秒*。根据您的应用程序需求，这可能需要进行调整。

示例: 限制MinIO群集在8个服务器上，最多可以接受1600个S3兼容API并发请求，并将每个API操作的等待期限设置为*2分钟*。

```sh
export MINIO_API_REQUESTS_MAX=1600
export MINIO_API_REQUESTS_DEADLINE=2m
export MINIO_ACCESS_KEY=your-access-key
export MINIO_SECRET_KEY=your-secret-key
minio server http://server{1...8}/mnt/hdd{1...16}
```

或者

```sh
mc admin config set myminio/ api requests_max=1600 requests_deadline=2m
mc admin service restart myminio/
```

