# MinIO共享后端存储快速入门[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO共享模式可以让你使用一个[NAS](https://en.wikipedia.org/wiki/Network-attached_storage) 做为多个MinIO服务的存储后端。我我们在设计时已经对多个MinIO服务之间的同步做了很多的处理。更多MinIO共享模式的设计文档，请访问[这里](https://github.com/minio/minio/blob/master/docs/shared-backend/DESIGN.md).

MinIO共享模式是为了解决在真实场景中存在的一些问题，而且不需要做额外的配置。
如果你有下列需求，则可以考虑MinIO共享模式

- 你已经买了NAS设备，并想在存储层使用MinIO来增加S3兼容。
- 你的应用架构要求你使用带有S3接口的NAS。
- 你的应用存在亚历山大的流量，你想给你的NAS设备增加一个有负载均衡能力的S3兼容的服务。 

在多个共享模式的MinIO服务前面加一层代理，你很容易就能获得一个高可用，负载均衡，AWS S3兼容的存储系统。

# 开始

如果你知道怎么部署单机MinIO,共享模式的部署和运行也是一样一样的。

## 1. 前提条件

安装MinIO - [MinIO快速入门](https://docs.min.io/cn/).

## 2. 在共享后端存储上运行MinIO

为了让MinIO在共享后端上运行起来，你需要启动多个MinIO服务，这些服务指向同一个后端存储。下面我们就来讲解如何设置。

*注意*

- 使用共享存储的所有MinIO节点需要有相同的access key和secret key。为了做到这一点，我们在所有节点上将access key和secret key export成环境变量，然后再去启动MinIO服务。
- 下面出现的存储路径都是为了演示目的，在真实环境中使用时，你应该替换成你真实要用的路径。

#### MinIO shared mode on Ubuntu 16.04 LTS. 

你需要将文件夹指向共享存储，比如`/path/to/nfs-volume`，然后在所有MinIO节点上运行下面的命令。

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio gateway nas /path/to/nfs-volume
```

#### MinIO shared mode on Windows 2012 Server

你需要将文件夹指向共享存储，比如`\\remote-server\smb`. 然后在所有MinIO节点上运行下面的命令。

```cmd
set MINIO_ACCESS_KEY=my-username
set MINIO_SECRET_KEY=my-password
minio.exe gateway nas \\remote-server\smb\export
```

*Windows提示*

如果一个远程的volume, 比如`\\remote-server\smb`挂载成一个硬盘, 比如`M:\`. 你可以使用[`net use`](https://technet.microsoft.com/en-us/library/bb490717.aspx)命令将这块盘映射到一个文件夹。

```cmd
set MINIO_ACCESS_KEY=my-username
set MINIO_SECRET_KEY=my-password
net use m: \\remote-server\smb\export /P:Yes
minio.exe gateway nas M:\export
```

## 3. 验证

为了验证部署是否成功，可能通过浏览器或者[`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)访问MinIO。你应该可以从各个MinIO节点访问上传的文件。

## 了解更多
- [使用`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用`aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用`s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用`minio-go` SDK](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO文档](https://docs.min.io)
