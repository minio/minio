# 使用Docker Compose部署MinIO [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Docker Compose允许定义和运行单主机，多容器Docker应用程序。

使用Compose，您可以使用Compose文件来配置MinIO服务。 然后，使用单个命令，您可以通过你的配置创建并启动所有分布式MinIO实例。 分布式MinIO实例将部署在同一主机上的多个容器中。 这是建立基于分布式MinIO的开发，测试和分期环境的好方法。

## 1. 前提条件

* 熟悉 [Docker Compose](https://docs.docker.com/compose/overview/).
* Docker已经在本机安装，从[这里](https://www.docker.com/community-edition#/download)下载相关的安装器。

## 2. 在Docker Compose上运行分布式MinIO

在Docker Compose上部署分布式MinIO,请下载[docker-compose.yaml](https://github.com/minio/minio/blob/master/docs/orchestration/docker-compose/docker-compose.yaml?raw=true)到你的当前工作目录。Docker Compose会pull MinIO Docker Image,所以你不需要手动去下载MinIO binary。然后运行下面的命令

### GNU/Linux and macOS

```sh
docker-compose pull
docker-compose up
```

### Windows

```sh
docker-compose.exe pull
docker-compose.exe up
```

现在每个实例都可以访问，端口从9001到9004，请在浏览器中访问http://127.0.0.1:9001/

### 注意事项

* 默认情况下Docker Compose file使用的是最新版的MinIO server的Docker镜像，你可以修改image tag来拉取指定版本的[MinIO Docker image](https://hub.docker.com/r/minio/minio/).

* 默认情况下会创建4个minio实例，你可以添加更多的MinIO服务（最多总共16个）到你的MinIO Comose deployment。添加一个服务
  * 复制服务定义并适当地更改新服务的名称。
  * 更新每个服务中的命令部分。
  * 更新要为新服务公开的端口号。 另外，请确保分配给新服务的端口尚未使用。

  关于分布式MinIO的更多资料，请访问[这里](https://docs.min.io/cn/distributed-minio-quickstart-guide).

* Docker compose file中的MinIO服务使用的端口是9001到9004，这允许多个服务在主机上运行。

### 了解更多
- [Docker Compose概述](https://docs.docker.com/compose/overview/)
- [MinIO Docker快速入门](https://docs.min.io/cn/minio-docker-quickstart-guide)
- [使用Docker Swarm部署MinIO](https://docs.min.io/cn/deploy-minio-on-docker-swarm)
- [MinIO纠删码快速入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)
