# 分布式MinIO快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

分布式MinIO可以让你将多块硬盘（甚至在不同的机器上）组成一个对象存储服务。由于硬盘分布在不同的节点上，分布式MinIO可以经受多点故障而且可以保障全部数据的恢复。   

## 分布式MinIO有什么好处?

在大数据领域，通常的设计理念都是无中心和分布式。MinIO分布式模式可以帮助你搭建一个高可用的对象存储服务，你可以使用这些存储设备，而不用考虑其在网络中的位置。 

### 数据保护


分布式MinIO采用 [纠错码`erasure code`](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)来防范多个节点宕机和[位衰减`bit rot`](https://github.com/minio/minio/blob/master/docs/zh_CN/erasure/README.md#what-is-bit-rot-protection)。  

分布式MinIO至少需要4个节点，使用分布式MinIO自动引入了纠删码功能。

### 高可用

单机MinIO服务存在单点故障，相反，如果是一个N节点的分布式MinIO,只要有N/2节点在线，你的数据就是安全的。不过你需要至少有N/2+1个节点   [Quorum](https://github.com/minio/dsync#lock-process) 来创建新的对象。

例如，一个8节点的MinIO集群，每个节点一块盘，就算4个节点宕机，这个集群仍然是可读的，不过你需要5个节点才能写数据或者创建新的对象。

### 限制

分布式MinIO单租户存在最少4个盘最多16个盘的限制（受限于纠删码）。这种限制确保了MinIO的简洁，同时仍拥有伸缩性。如果你需要搭建一个多租户环境，你可以轻松的使用编排工具（Kubernetes）来管理多个MinIO实例。

注意，只要遵守分布式MinIO的限制，你可以组合不同的节点和每个节点几块盘。比如，你可以使用2个节点，每个节点4块盘，或者4个节点，每个节点4块盘，或者8个节点，每个节点两块盘，再或者32个服务器，每个服务器24个盘，诸如此类。

你也可以使用[存储类](https://github.com/minio/minio/tree/master/docs/erasure/storage-class)在整个磁盘上设置个性化的数据和奇偶分布。

### 一致性保障

在分布式和单机模式下的MinIO，所有读写操作都严格遵守**read-after-write**一致性模型。

# 开始

如果你了解MinIO单机模式的搭建的话，分布式搭建的流程基本一样，MinIO服务基于命令行传入的参数自动切换成单机模式还是分布式模式。 

## 1. 前提条件

安装MinIO - [MinIO快速入门](https://docs.min.io/cn/minio-quickstart-guide).

## 2. 运行分布式MinIO

启动一个分布式MinIO实例，你只需要把硬盘位置做为参数传给minio server命令即可，然后，你需要在所有其它节点运行同样的命令。

*注意* 

- 分布式MinIO里所有的节点需要有同样的access秘钥和secret秘钥，这样这些节点才能建立联接。为了实现这个，你需要在执行minio server命令之前，必须先将所有节点的access秘钥和secret秘钥export成环境变量。 
- 所有的节点在运行分布式MinIOn时都必须保持同样的环境，也就是同样的操作系统，同样的磁盘数量和同样的相互连接。
- 如果域名需要被设置，MINIO_DOMAIN环境变量应该被定义并且被输出。
- 分布式MinIO使用的磁盘里必须是干净的，里面没有数据。
- 下面示例里的IP仅供示例参考，你需要改成你真实用到的IP和文件夹路径。 
- 分布式MinIO里的节点时间差不能超过3秒，你可以使用[NTP](http://www.ntp.org/) 来保证节点间时间一致。
- 在Windows下运行分布式MinIO处于实验阶段，请谨慎使用。

示例1: 启动分布式MinIO实例，一共8个节点，每个节点1块盘，而且都安装在`/export1`下。您需要在8个节点上都运行下面的命令:
![分布式MinIO, 8个节点，每个节点一块盘](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_8.jpg?raw=true)


#### GNU/Linux 和 macOS

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://192.168.1.1{1...8}/export1
```

_注意_：`{1...n}`显示有3个点！只使用2个点的`{1..4}`将由shell解释，不会被传递给minio服务器，影响擦除编码顺序，这可能会影响性能和高可用性。_总是使用`{1...n}`(3个点!)去允许minio服务器最优地擦除代码数据。

## 3. 验证

验证是否部署成功，使用浏览器访问MinIO服务或者使用 [`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)。多个节点的存储容量和就是分布式MinIO的存储容量。

## 了解更多

- [MinIO超大存储桶支持指南](https://docs.min.io/cn
/minio-large-bucket-support-quickstart-guide)
- [MinIO纠删码快速入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)
- [使用 `mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用 `s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用 `minio-go` SDK ](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO官方文档](https://docs.min.io/cn)
