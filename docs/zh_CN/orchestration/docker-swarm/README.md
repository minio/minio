# 使用Docker Swarm部署Minio [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine在Swarm模式下提供集群管理和编排功能。 Minio服务器可以在Swarm的分布式模式下轻松部署，创建一个多租户，高可用性和可扩展的对象存储。

从[Docker Engine v1.13.0](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/) (Docker Compose v3.0)开始, Docker Swarm和Compose 二者[cross-compatible](https://docs.docker.com/compose/compose-file/#version-3)。这允许将Compose file用作在Swarm上部署服务的模板。 我们使用Docker Compose file创建分布式Minio设置。

## 1. 前提条件

* 熟悉[Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/).
* Docker engine v1.13.0运行在[networked host machines]集群上(https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines).

## 2. 创建Swarm
在管理节点上创建一个swarm,请运行下面的命令

```shell
docker swarm init --advertise-addr <MANAGER-IP>
```
一旦swarm初使化了，你可以看到下面的响应信息

```shell
docker swarm join \
  --token  SWMTKN-1-49nj1cmql0jkz5s954yi3oex3nedyz0fb0xx14ie39trti4wxv-8vxv8rssmk743ojnwacrr2e7c \
  192.168.99.100:2377
```

你现在可以运行上述命令[添加worker节点](https://docs.docker.com/engine/swarm/swarm-tutorial/add-nodes/)到swarm。更多关于创建swarm的细节步骤，请访问[Docker documentation site](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/).

## 3. 为Minio创建Docker secret

```shell
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

## 4. 部署分布式minio服务

在你的Swarm master上下载[Docker Compose file](https://github.com/minio/minio/blob/master/docs/orchestration/docker-swarm/docker-compose-secrets.yaml?raw=true) ，然后运行下面的命令

```shell
docker stack deploy --compose-file=docker-compose-secrets.yaml minio_stack
```

这将把Compose file里描述的服务部署为Docker stack`minio_stack`。 更多 `docker stack` [命令参考](https://docs.docker.com/engine/reference/commandline/stack/)。

在stack成功部署之后，你可以通过[Minio Client](https://docs.minio.io/docs/zh_CN/minio-client-complete-guide) `mc` 或者浏览器访问http://[Node_Public_IP_Address]:[Expose_Port_on_Host]来访问你的Minio server。

## 4. 删除分布式Minio services

删除分布式Minio services以及相关的网络，请运行下面的命令

```shell
docker stack rm minio_stack
```
Swarm不会自动删除为Minio服务创建的host volumes,如果下次新的Minio服务不熟到swarm上，可能会导致损坏。因此，我们建议手动删除所有Minio使用的volumes。为此，到每一个swarm的节点上，列出所有的volumes

```shell
docker volume ls
```
然后删除`minio_stack` volumes

```shell
docker volume rm volume_name 
```

### 注意事项

* 默认情况下Docker Compose file使用的是最新版的Minio server的Docker镜像，你可以修改image tag来拉取指定版本的[Minio Docker image](https://hub.docker.com/r/minio/minio/).

* 默认情况下会创建4个minio实例，你可以添加更多的Minio服务（最多总共16个）到你的Minio Comose deployment。添加一个服务
  * 复制服务定义并适当地更改新服务的名称。
  * 更新每个服务中的命令部分。
  * 更新要为新服务公开的端口号。 另外，请确保分配给新服务的端口尚未使用。

  关于分布式Minio的更多资料，请访问[这里](https://docs.minio.io/docs/zh_CN/distributed-minio-quickstart-guide).

* 默认情况下，Minio服务使用的是`local` volume driver. 更多配置选项，请访问[Docker documentation](https://docs.docker.com/compose/compose-file/#/volume-configuration-reference) 。

* Docker compose file中的Minio服务使用的端口是9001到9004，这允许多个服务在主机上运行。更多配置选项，请访问[Docker documentation](https://docs.docker.com/compose/compose-file/#/ports).

* Docker Swarm默认使用的是ingress做负载均衡，你可以跟据需要配置[external load balancer based](https://docs.docker.com/engine/swarm/ingress/#/configure-an-external-load-balancer)。

### 了解更多
- [Docker Swarm mode概述](https://docs.docker.com/engine/swarm/)
- [Minio Docker快速入门](https://docs.minio.io/docs/zh_CN/minio-docker-quickstart-guide)
- [使用Docker Compose部署Minio](https://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-compose)
- [Minio纠删码快速入门](https://docs.minio.io/docs/zh_CN/minio-erasure-code-quickstart-guide)
