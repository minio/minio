# 使用minikube在本地部署分布式Minio
 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minikube在计算机的VM中运行单节点Kubernetes集群。 这样可以轻松地在计算机上本地运行的Kubernetes上部署分布式Minio服务器。

## 1. 前提条件

本机已经安装[Minikube](https://github.com/kubernetes/minikube/blob/master/README.md#installation) 和 [`kubectl`](https://kubernetes.io/docs/user-guide/prereqs/)


## 2. 步骤

* 下载 `minio_distributed.sh` 和 `statefulset.yaml`

```sh
wget https://raw.githubusercontent.com/minio/minio/master/docs/orchestration/minikube/minio_distributed.sh  
wget https://raw.githubusercontent.com/minio/minio/master/docs/orchestration/minikube/statefulset.yaml
```

* 在命令提示符下执行`minio_distributed.sh`脚本。

```sh
./minio_distributed.sh
```

脚本执行成功后，您应该会收到一个这样的输出

```sh
service "minio-public" created
service "minio" created
statefulset "minio" created
```
这意味着Minio部署在您当地的Minikube安装中。

请注意，服务“minio-public”是一个[clusterIP]（https://kubernetes.io/docs/user-guide/services/#publishing-services---service-types）服务。 它在集群内部IP上暴露服务。 通过`kubectl port-forward`命令连接到Minio实例，执行

```
kubectl port-forward minio-0 9000:9000
```

Minio服务器现在可以在`http:// localhost:9000`访问，使用`statefulset.yaml`文件中所述的accessKey和secretKey。

## 3. 注意

Minikube目前不支持动态配置，因此我们手动创建PersistentVolumes（PV）和PersistentVolumeClaims（PVC）。 创建PV和PVC后，我们将调用`statefulset.yaml`配置文件来创建分布式的Minio设置。
此设置在笔记本电脑/计算机上运行。 因此，只有一个磁盘用作所有minio实例PV的后端。 Minio将这些PV视为单独的磁盘，并报告可用存储不正确。
