# MinIO多租户（Multi-tenant）部署指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## 单机部署

要在单台机器上托管多个租户，为每个租户运行一个MinIO server,使用不同的HTTPS端口、配置和数据目录。

#### 示例1：单主机，单磁盘

以下示例在一块磁盘上托管三个租户。
```sh
minio --config-dir ~/tenant1 server --address :9001 /data/tenant1
minio --config-dir ~/tenant2 server --address :9002 /data/tenant2
minio --config-dir ~/tenant3 server --address :9003 /data/tenant3
```

![示例1](https://github.com/minio/minio/blob/master/docs/screenshots/Example-1.jpg?raw=true)

#### 示例2：单主机，多块磁盘 (erasure code)

以下示例在多块磁盘上托管三个租户。
```sh
minio --config-dir ~/tenant1 server --address :9001 /disk1/data/tenant1 /disk2/data/tenant1 /disk3/data/tenant1 /disk4/data/tenant1
minio --config-dir ~/tenant2 server --address :9002 /disk1/data/tenant2 /disk2/data/tenant2 /disk3/data/tenant2 /disk4/data/tenant2
minio --config-dir ~/tenant3 server --address :9003 /disk1/data/tenant3 /disk2/data/tenant3 /disk3/data/tenant3 /disk4/data/tenant3
```
![示例2](https://github.com/minio/minio/blob/master/docs/screenshots/Example-2.jpg?raw=true)

## 分布式部署
要在分布式环境中托管多个租户，同时运行多个分布式MinIO实例。

#### 示例3 : 多主机，多块磁盘 (erasure code)

以下示例在一个4节点集群中托管三个租户。在4个节点里都执行下列命令：

```sh
export MINIO_ACCESS_KEY=<TENANT1_ACCESS_KEY>
export MINIO_SECRET_KEY=<TENANT1_SECRET_KEY>
minio --config-dir ~/tenant1 server --address :9001 http://192.168.10.11/data/tenant1 http://192.168.10.12/data/tenant1 http://192.168.10.13/data/tenant1 http://192.168.10.14/data/tenant1

export MINIO_ACCESS_KEY=<TENANT2_ACCESS_KEY>
export MINIO_SECRET_KEY=<TENANT2_SECRET_KEY>
minio --config-dir ~/tenant2 server --address :9002 http://192.168.10.11/data/tenant2 http://192.168.10.12/data/tenant2 http://192.168.10.13/data/tenant2 http://192.168.10.14/data/tenant2

export MINIO_ACCESS_KEY=<TENANT3_ACCESS_KEY>
export MINIO_SECRET_KEY=<TENANT3_SECRET_KEY>
minio --config-dir ~/tenant3 server --address :9003 http://192.168.10.11/data/tenant3 http://192.168.10.12/data/tenant3 http://192.168.10.13/data/tenant3 http://192.168.10.14/data/tenant3
```

![示例3](https://github.com/minio/minio/blob/master/docs/screenshots/Example-3.jpg?raw=true)

## 云端可伸缩部署
对于大型多租户MinIO部署，我们建议使用一个流行的容器编排平台，比如Kubernetes、DC/OS，或者是Docker Swarm.参考 [这个文档](https://docs.min.io/cn/minio-deployment-quickstart-guide) ,学习如何在编排平台中使用MinIO。


