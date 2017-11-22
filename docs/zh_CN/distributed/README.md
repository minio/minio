# 分布式Minio快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

分布式Minio可以让你将多块硬盘（甚至在不同的机器上）组成一个对象存储服务。由于硬盘分布在不同的节点上，分布式Minio避免了单点故障。   

## 分布式Minio有什么好处?

在大数据领域，通常的设计理念都是无中心和分布式。Minio分布式模式可以帮助你搭建一个高可用的对象存储服务，你可以使用这些存储设备，而不用考虑其真实物理位置。 

### 数据保护

分布式Minio采用 [erasure code](https://docs.minio.io/docs/zh_CN/minio-erasure-code-quickstart-guide)来防范多个节点宕机和[位衰减`bit rot`](https://github.com/minio/minio/blob/master/docs/zh_CN/erasure/README.md#what-is-bit-rot-protection)。  
分布式Minio至少需要4个节点，使用分布式Minio自动引入了纠删码功能。

### 高可用

单机Minio服务存在单点故障，相反，如果是一个N节点的分布式Minio,只要有N/2节点在线，你的数据就是安全的。不过你需要至少有N/2+1个节点   [Quorum](https://github.com/minio/dsync#lock-process) 来创建新的对象。

例如，一个8节点的Minio集群，每个节点一块盘，就算4个节点宕机，这个集群仍然是可读的，不过你需要5个节点才能写数据。

### 限制

分布式Minio单租户存在最少4个盘最多16个盘的限制（受限于纠删码）。这种限制确保了Minio的简洁，同时仍拥有伸缩性。如果你需要搭建一个多租户环境，你可以轻松的使用编排工具（Kubernetes）来管理多个Minio实例。

注意，只要遵守分布式Minio的限制，你可以组合不同的节点和每个节点几块盘。比如，你可以使用2个节点，每个节点4块盘，也可以使用4个节点，每个节点两块盘，诸如此类。

### 一致性

Minio在分布式和单机模式下，所有读写操作都严格遵守**read-after-write**一致性模型。

# 开始吧

如果你了解Minio单机模式的搭建的话，分布式搭建的流程基本一样，Minio服务基于命令行传入的参数自动切换成单机模式还是分布式模式。 

## 1. 前提条件

安装Minio - [Minio快速入门](https://docs.minio.io/docs/zh_CN/minio-quickstart-guide).

## 2. 运行分布式Minio

启动一个分布式Minio实例，你只需要把硬盘位置做为参数传给minio server命令即可，然后，你需要在所有其它节点运行同样的命令。

*注意* 

- 分布式Minio里所有的节点需要有同样的access秘钥和secret秘钥，这样这些节点才能建立联接。为了实现这个，你需要在执行minio server命令之前，先将access秘钥和secret秘钥export成环境变量。 
- 分布式Minio使用的磁盘里必须是干净的，里面没有数据。
- 下面示例里的IP仅供示例参考，你需要改成你真实用到的IP和文件夹路径。 
- 分布式Minio里的节点时间差不能超过3秒，你可以使用[NTP](http://www.ntp.org/) 来保证时间一致。
- 在Windows下运行分布式Minio处于实验阶段，请悠着点使用。

示例1: 启动分布式Minio实例，8个节点，每节点1块盘，需要在8个节点上都运行下面的命令。 

#### GNU/Linux 和 macOS

```shell
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://192.168.1.11/export1 http://192.168.1.12/export2 \
               http://192.168.1.13/export3 http://192.168.1.14/export4 \
               http://192.168.1.15/export5 http://192.168.1.16/export6 \
               http://192.168.1.17/export7 http://192.168.1.18/export8
```
#### Windows 

```cmd
set MINIO_ACCESS_KEY=<ACCESS_KEY>
set MINIO_SECRET_KEY=<SECRET_KEY>
minio.exe server http://192.168.1.11/C:/data http://192.168.1.12/C:/data ^
                  http://192.168.1.13/C:/data http://192.168.1.14/C:/data ^
                  http://192.168.1.15/C:/data http://192.168.1.16/C:/data ^
                  http://192.168.1.17/C:/data http://192.168.1.18/C:/data
```

![分布式Minio,8节点，每个节点一块盘](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_8.jpg?raw=true)

示例2: 启动分布式Minio实例，4节点，每节点4块盘，需要在4个节点上都运行下面的命令。

#### GNU/Linux 和 macOS

```shell
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://192.168.1.11/export1 http://192.168.1.11/export2 \
               http://192.168.1.11/export3 http://192.168.1.11/export4 \
               http://192.168.1.12/export1 http://192.168.1.12/export2 \
               http://192.168.1.12/export3 http://192.168.1.12/export4 \
               http://192.168.1.13/export1 http://192.168.1.13/export2 \
               http://192.168.1.13/export3 http://192.168.1.13/export4 \
               http://192.168.1.14/export1 http://192.168.1.14/export2 \
               http://192.168.1.14/export3 http://192.168.1.14/export4
```

#### Windows

```cmd
set MINIO_ACCESS_KEY=<ACCESS_KEY>
set MINIO_SECRET_KEY=<SECRET_KEY>
minio.exe server http://192.168.1.11/C:/data1 http://192.168.1.11/C:/data2 ^
                  http://192.168.1.11/C:/data3 http://192.168.1.11/C:/data4 ^
                  http://192.168.1.12/C:/data1 http://192.168.1.12/C:/data2 ^
                  http://192.168.1.12/C:/data3 http://192.168.1.12/C:/data4 ^
                  http://192.168.1.13/C:/data1 http://192.168.1.13/C:/data2 ^
                  http://192.168.1.13/C:/data3 http://192.168.1.13/C:/data4 ^                  
                  http://192.168.1.14/C:/data1 http://192.168.1.14/C:/data2 ^
                  http://192.168.1.14/C:/data3 http://192.168.1.14/C:/data4
```

![分布式Minio,4节点，每节点4块盘](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_16.jpg?raw=true)

## 3. 验证

验证是否部署成功，使用浏览器访问Minio服务或者使用 [`mc`](https://docs.minio.io/docs/zh_CN/minio-client-quickstart-guide)。多个节点的存储容量和就是分布式Minio的存储容量。

## 了解更多
- [Minio纠删码快速入门](https://docs.minio.io/docs/zh_CN/minio-erasure-code-quickstart-guide)
- [使用 `mc`](https://docs.minio.io/docs/zh_CN/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.minio.io/docs/zh_CN/aws-cli-with-minio)
- [使用 `s3cmd](https://docs.minio.io/docs/zh_CN/s3cmd-with-minio)
- [使用 `minio-go` SDK ](https://docs.minio.io/docs/zh_CN/golang-client-quickstart-guide)
- [minio官方文档](https://docs.minio.io)
