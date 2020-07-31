# MinIO Docker 快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## 前提条件
您的机器已经安装docker. 从 [这里](https://www.docker.com/community-edition#/download)下载相关软件。

## 在Docker中运行MinIO单点模式。
MinIO 需要一个持久卷来存储配置和应用数据。不过, 如果只是为了测试一下, 您可以通过简单地传递一个目录（在下面的示例中为`/ data`）启动MinIO。这个目录会在容器启动时在容器的文件系统中创建，不过所有的数据都会在容器退出时丢失。

```sh
docker run -p 9000:9000 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /data
```

要创建具有永久存储的MinIO容器，您需要将本地持久目录从主机操作系统映射到虚拟配置`~/.minio` 并导出`/data`目录。 为此，请运行以下命令

#### GNU/Linux 和 macOS
```sh
docker run -p 9000:9000 \
  --name minio1 \
  -v /mnt/data:/data \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /data
```

#### Windows
```sh
docker run -p 9000:9000 \
  --name minio1 \
  -v D:\data:/data \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /data
```

## 在Docker中运行MinIO分布式模式
分布式MinIO可以通过 [Docker Compose](https://docs.min.io/cn/deploy-minio-on-docker-compose) 或者 [Swarm mode](https://docs.min.io/cn/deploy-minio-on-docker-swarm)进行部署。这两者之间的主要区别是Docker Compose创建了单个主机，多容器部署，而Swarm模式创建了一个多主机，多容器部署。

这意味着Docker Compose可以让你快速的在你的机器上快速使用分布式MinIO-非常适合开发，测试环境；而Swarm模式提供了更健壮，生产级别的部署。

## MinIO Docker提示

### MinIO自定义Access和Secret密钥
要覆盖MinIO的自动生成的密钥，您可以将Access和Secret密钥设为环境变量。 MinIO允许常规字符串作为Access和Secret密钥。

#### GNU/Linux 和 macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/data:/data \
  minio/minio server /data
```

#### Windows
```powershell
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  minio/minio server /data
```

### 以普通用户身份运行MinIO Docker
Docker提供了标准化的机制，可以以非root用户身份运行docker容器。

#### GNU/Linux 和 macOS
在 Linux 和 macOS 上， 你可以使用 `--user` 以普通用户身份来运行容器。

> 注意: 在使用`--user`前，一定要确保--user指定的用户具备 *${HOME}/data* 的写入权限。
```sh
mkdir -p ${HOME}/data
docker run -p 9000:9000 \
  --user $(id -u):$(id -g) \
  --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY" \
  -v ${HOME}/data:/data \
  minio/minio server /data
```

#### Windows
在windows上， 你需要用到 [Docker集成Windows身份验证](https://success.docker.com/article/modernizing-traditional-dot-net-applications#integratedwindowsauthentication) 和 [创建具有Active Directory支持的容器](https://blogs.msdn.microsoft.com/containerstuff/2017/01/30/create-a-container-with-active-directory-support/) 的能力

> 注意: 在使用`credentialspec=`欠，要确保你的AD/Windows用户具备 *D:\data* 的写入权限。

```powershell
docker run -p 9000:9000 \
  --name minio1 \
  --security-opt "credentialspec=file://myuser.json"
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  minio/minio server /data
```

### 使用Docker secrets进行MinIO Access和Secret密钥自定义
要覆盖MinIO的自动生成的密钥,你可以把secret和access秘钥创建成[Docker secrets](https://docs.docker.com/engine/swarm/secrets/). MinIO允许常规字符串作为Access和Secret密钥。

```
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

使用`docker service`创建MinIO服务，并读取Docker secrets。
```
docker service create --name="minio-service" --secret="access_key" --secret="secret_key" minio/minio server /data
```

更多 `docker service`信息，请访问 [这里](https://docs.docker.com/engine/swarm/how-swarm-mode-works/services/)

#### 自定义MinIO Access和Secret密钥文件
要使用其他密钥名称，请把上面的`access_key` 和 `secret_key`替换为你自定义的名称(比如`my_secret_key`,`my_custom_key`)。使用如下命令运行服务
```
docker service create --name="minio-service" \
  --secret="my_access_key" \
  --secret="my_secret_key" \
  --env="MINIO_ACCESS_KEY_FILE=my_access_key" \
  --env="MINIO_SECRET_KEY_FILE=my_secret_key" \
  minio/minio server /data
```

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

### MinIO容器日志
获取MinIO日志，使用 [`docker logs`](https://docs.docker.com/engine/reference/commandline/logs/) 命令。

```sh
docker logs <container_id>
```

### 监控MinioDocker容器
监控MinIO容器使用的资源,使用 [`docker stats`](https://docs.docker.com/engine/reference/commandline/stats/) 命令.

```sh
docker stats <container_id>
```

## 了解更多


* [在Docker Compose上部署MinIO](https://docs.min.io/cn/deploy-minio-on-docker-compose)
* [在Docker Swarm上部署MinIO](https://docs.min.io/cn/deploy-minio-on-docker-swarm)
* [分布式MinIO快速入门](https://docs.min.io/cn/distributed-minio-quickstart-guide)
* [MinIO纠删码模式快速入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)

