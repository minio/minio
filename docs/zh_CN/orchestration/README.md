# Minio部署快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio是一个[云原生](https://baike.baidu.com/item/Cloud%20Native/19865304?fr=aladdin)的应用程序，旨在在多租户环境中以可持续的方式进行扩展。Orchestration平台为Minio的扩展提供了非常好的支撑。以下是各种orchestration平台的Minio部署文档
Minio is a cloud-native application designed to scale in a sustainable manner in multi-tenant environments. Orchestration platforms provide perfect launchpad for Minio to scale. Below is the list of Minio deployment documents for various orchestration platforms:

| Orchestration平台|
|:---|
| [`Docker Swarm`](http://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-swarm) |
| [`Docker Compose`](http://docs.minio.io/docs/zh_CN/deploy-minio-on-docker-compose) |
| [`Kubernetes`](http://docs.minio.io/docs/zh_CN/deploy-minio-on-kubernetes) |
| [`DC/OS`](http://docs.minio.io/docs/zh_CN/deploy-minio-on-dc-os) |

## 为什么说Minio是云原生的（cloud-native）?
云原生这个词代表的是一些思想的集合，比如微服务部署，可伸缩，而不是说把一个单体应用改造成容器部署。一个云原生的应用在设计时就考虑了移植性和可伸缩性，而且可以通过简单的复制即可实现水平扩展。现在兴起的编排平台，想Swarm,Kubernetes,以及DC/OS，让大规模集群的复制和管理变得前所未有的简单，哪里不会点哪里。

容器提供了隔离的应用执行环境，编排（orchestration）平台通过容器管理以及复制功能提供了无缝的扩展。Minio继承了这些，针对每个租户提供了存储环境的隔离。

Minio是建立在云原生的基础上，有纠删码、分布式和共享存储这些特性。Minio专注于并且只专注于存储，而且做的还不错。它可以通过编排平台复制一个Minio实例就实现了水平扩展。

> 在一个云原生环境中，伸缩性不是应用的一个功能而是编排平台的功能。

现在的应用、数据库，key-store这些，很多都已经部署在容器中，并且通过编排平台进行管理。Minio提供了一个健壮的、可伸缩、AWS S3兼容的对象存储，这是Minio的立身之本，凭此在云原生应用中占据一席之地。

![Cloud-native](https://github.com/minio/minio/blob/master/docs/screenshots/Minio_Cloud_Native_Arch.jpg?raw=true)
