# MinIO部署快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO是一个[云原生](https://baike.baidu.com/item/Cloud%20Native/19865304?fr=aladdin)的应用程序，旨在在多租户环境中以可持续的方式进行扩展。编排（orchestration）平台为MinIO的扩展提供了非常好的支撑。以下是各种编排平台的MinIO部署文档:

| Orchestration平台|
|:---|
| [`Docker Swarm`](https://docs.min.io/cn/deploy-minio-on-docker-swarm) |
| [`Docker Compose`](https://docs.min.io/cn/deploy-minio-on-docker-compose) |
| [`Kubernetes`](https://docs.min.io/cn/deploy-minio-on-kubernetes) |
| [`DC/OS`](https://docs.min.io/cn/deploy-minio-on-dc-os) |

## 为什么说MinIO是云原生的（cloud-native）?
云原生这个词代表的是一些思想的集合，比如微服务部署，可伸缩，而不是说把一个单体应用改造成容器部署。一个云原生的应用在设计时就考虑了移植性和可伸缩性，而且可以通过简单的复制即可实现水平扩展。现在兴起的编排平台，像Swarm、Kubernetes以及DC/OS，让大规模集群的复制和管理变得前所未有的简单，哪里不会点哪里。

容器提供了隔离的应用执行环境，编排平台通过容器管理以及复制功能提供了无缝的扩展。MinIO继承了这些，针对每个租户提供了存储环境的隔离。

MinIO是建立在云原生的基础上，有纠删码、分布式和共享存储这些特性。MinIO专注于并且只专注于存储，而且做的还不错。它可以通过编排平台复制一个MinIO实例就实现了水平扩展。

> 在一个云原生环境中，伸缩性不是应用的一个功能而是编排平台的功能。

现在的应用、数据库，key-store这些，很多都已经部署在容器中，并且通过编排平台进行管理。MinIO提供了一个健壮的、可伸缩、AWS S3兼容的对象存储，这是MinIO的立身之本，凭此在云原生应用中占据一席之地。

![Cloud-native](https://github.com/minio/minio/blob/master/docs/screenshots/MinIO_Cloud_Native_Arch.jpg?raw=true)
