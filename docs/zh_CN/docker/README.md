# Minio Docker 快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## 前提条件
您的机器已经安装docker. 从 [这里](https://www.docker.com/community-edition#/download)下载相关软件。

## 在Docker中运行Minio单点模式。
Minio 需要一个持久卷来存储配置和应用数据。不过, 如果只是为了测试一下, 您可以通过简单地传递一个目录（在下面的示例中为`/ data`）启动Minio。这个目录会在容器启动时在容器的文件系统中创建，不过所有的数据都会在容器退出时丢失。

```sh
docker run -p 9000:9000 minio/minio server /data
```

要创建具有永久存储的Minio容器，您需要将本地持久目录从主机操作系统映射到虚拟配置`~/.minio` 并导出`/data`目录。 为此，请运行以下命令

#### GNU/Linux 和 macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### Windows
```sh
docker run -p 9000:9000 --name minio1 \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

## 在Docker中运行Minio分布式模式
分布式Minio可以通过 [Docker Compose](https://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-compose) 或者 [Swarm mode](https://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-swarm)进行部署。这两者之间的主要区别是Docker Compose创建了单个主机，多容器部署，而Swarm模式创建了一个多主机，多容器部署。

这意味着Docker Compose可以让你快速的在你的机器上快速使用分布式Minio-非常适合开发，测试环境；而Swarm模式提供了更健壮，生产级别的部署。

## Minio Docker提示

### Minio自定义Access和Secret密钥
要覆盖Minio的自动生成的密钥，您可以将Access和Secret密钥设为环境变量。 Minio允许常规字符串作为Access和Secret密钥。

#### GNU/Linux 和 macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### Windows
```powershell
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

### 使用Docker secrets进行Minio Access和Secret密钥自定义
要覆盖Minio的自动生成的密钥,你可以把secret和access秘钥创建成[Docker secrets](https://docs.docker.com/engine/swarm/secrets/). Minio允许常规字符串作为Access和Secret密钥。

```
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

使用`docker service`创建Minio服务，并读取Docker secrets。
```
docker service create --name="minio-service" --secret="access_key" --secret="secret_key" minio/minio server /data
```

更多 `docker service`信息，请访问 [这里](https://docs.docker.com/engine/swarm/how-swarm-mode-works/services/)

### 获取容器ID
在容器中使用Docker命令, 你需要知道这个容器的 `容器ID` 。 为了获取 `Container ID`, 运行

```sh
docker ps -a
```

`-a` flag 确保你获取所有的容器(创建的，正在运行的，退出的)，然后从输出中识别`Container ID`。

### 启动和停止容器
启动容器,你可以使用 [`docker start`](https://docs.docker.com/engine/reference/commandline/start/) 命令。

```sh
docker start <container_id>
```

停止一下正在运行的容器, 使用 [`docker stop`](https://docs.docker.com/engine/reference/commandline/stop/) 命令。
```sh
docker stop <container_id>
```

### Minio容器日志
获取Minio日志，使用 [`docker logs`](https://docs.docker.com/engine/reference/commandline/logs/) 命令。

```sh
docker logs <container_id>
```

### 监控MinioDocker容器
监控Minio容器使用的资源,使用 [`docker stats`](https://docs.docker.com/engine/reference/commandline/stats/) 命令.

```sh
docker stats <container_id>
```

## 了解更多

* [在Docker Compose上部署Minio](https://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-compose)
* [在Docker Swarm上部署Minio](https://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-swarm)
* [分布式Minio快速入门](https://docs.minio.io/docs/zh_CN/distributed-minio-quickstart-guide)
* [Minio纠删码模式快速入门](https://docs.minio.io/docs/zh_CN/minio-erasure-code-quickstart-guide)
