# 使用Kubernetes部署Minio [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Kubernetes的部署和状态集提供了在独立，分布式或共享模式下部署Minio服务器的完美平台。 在Kubernetes上部署Minio有多种选择，您可以选择最适合您的。

- Minio [Helm](https://helm.sh) Chart通过一个简单的命令即可提供自定义而且简单的Minio部署。更多关于Minio Helm部署的资料，请访问[这里](#prerequisites).

- 你也可以浏览Kubernetes [Minio示例](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/README.md) ，通过`.yaml`文件来部署Minio。

- 如果您想在Kubernetes上开始使用Minio，而无需创建真正的容器集群，您也可以使用Minikube [deploy Minio locally](https://raw.githubusercontent.com/minio/minio/master/docs/orchestration/minikube/README.md)。

<a name="prerequisites"></a>
## 1. 前提条件

* 默认standaline模式下，需要开启Beta API的Kubernetes 1.4+。
* [distributed 模式](#distributed-minio)，需要开启Beta API的Kubernetes 1.5+。
* 底层支持PV provisioner。
* 你的K8s集群里需要有Helm package manager [installed](https://github.com/kubernetes/helm#install)。

## 2. 使用Helm Chart部署Minio

安装 Minio chart

```bash
$ helm install stable/minio
```
以上命令以默认配置在Kubernetes群集上部署Minio。 以下部分列出了Minio图表的所有可配置参数及其默认值。

### 配置

| 参数                  | 描述                         | 默认值                                                 |
|----------------------------|-------------------------------------|---------------------------------------------------------|
| `image`                    | Minio镜像名称                | `minio/minio`                                           |
| `imageTag`                 | Minio镜像tag. 可选值在 [这里](https://hub.docker.com/r/minio/minio/tags/).| `RELEASE.2017-08-05T00-00-53Z`|
| `imagePullPolicy`          | Image pull policy                   | `Always`                                                |
| `mode`                     | Minio server模式 (`standalone`, `shared` 或者 `distributed`)| `standalone`                     |
| `numberOfNodes`            | 节点数 (仅对分布式模式生效). 可选值 4 <= x <= 16 | `4`    |
| `accessKey`                | 默认access key                  | `AKIAIOSFODNN7EXAMPLE`                                  |
| `secretKey`                | 默认secret key                  | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`              |
| `configPath`               | 默认配置文件路径         | `~/.minio`                                              |
| `mountPath`                | 默认挂载路径| `/export`                                        |
| `serviceType`              | Kubernetes service type             | `LoadBalancer`                                          |
| `servicePort`              | Kubernetes端口 | `9000`                                              |
| `persistence.enabled`      | 是否使用持久卷存储数据 | `true`                                                  |
| `persistence.size`         | 持久卷大小     | `10Gi`                                                  |
| `persistence.storageClass` | 持久卷类型    | `generic`                                               |
| `persistence.accessMode`   | ReadWriteOnce 或者 ReadOnly           | `ReadWriteOnce`                                         |
| `resources`                | CPU/Memory 资源需求/限制 | Memory: `256Mi`, CPU: `100m`                            |

你可以通过`--set key=value[,key=value]`给`helm install`。 比如,

```bash
$ helm install --name my-release \
  --set persistence.size=100Gi \
    stable/minio
```

上述命令部署了一个带上100G持久卷的Minio服务。

或者，您可以提供一个YAML文件，用于在安装chart时指定参数值。 例如，

```bash
$ helm install --name my-release -f values.yaml stable/minio
```

### 分布式Minio

默认情况下，此图表以独立模式提供Minio服务器。 要在[分布式模式](https://docs.minio.io/docs/zh_CN/distributed-minio-quickstart-guide)中配置Minio服务器，请将`mode`字段设置为`distributed`,

```bash
$ helm install --set mode=distributed stable/minio
```

上述命令部署了个带有4个节点的分布式Minio服务器。 要更改分布式Minio服务器中的节点数，请设置`numberOfNodes`属性。


```bash
$ helm install --set mode=distributed,numberOfNodes=8 stable/minio
```

上述命令部署了个带有8个节点的分布式Minio服务器。注意一下，`numberOfNodes`取值范围是[4,16]。

#### StatefulSet [限制](http://kubernetes.io/docs/concepts/abstractions/controllers/statefulsets/#limitations)，适用于分布式Minio

* StatefulSets需要持久化存储，所以如果 `mode`设成 `distributed`的话，`persistence.enabled`参数不生效。
* 卸载分布式Minio版本时，需要手动删除与StatefulSet关联的卷。

### Shared Minio

如需采用[shared mode](https://github.com/minio/minio/blob/master/docs/shared-backend/README.md)部署Minio, 将`mode` 设为`shared`,

```bash
$ helm install --set mode=shared stable/minio
```

上述命令规定了4个Minio服务器节点，一个存储。 要更改共享的Minio部署中的节点数，请设置`numberOfNodes`字段，

```bash
$ helm install --set mode=shared,numberOfNodes=8 stable/minio
```

上述命令规定了Minio服务有8个节点，采用shared模式。

### 持久化

这里规定了PersistentVolumeClaim并将相应的持久卷挂载到默认位置`/export`。 您需要Kubernetes集群中的物理存储才能使其工作。 如果您宁愿使用`emptyDir`，请通过以下方式禁用PersistentVolumeClaim：

```bash
$ helm install --set persistence.enabled=false stable/minio
```

> *"当Pod分配给节点时，首先创建一个emptyDir卷，只要该节点上的Pod正在运行，它就会存在。 当某个Pod由于任何原因从节点中删除时，emptyDir中的数据将永久删除。"*

## 3. 使用Helm更新Minio版本

您可以更新现有的Minio Helm Release以使用较新的Minio Docker镜像。 为此，请使用`helm upgrade`命令：

```bash
$ helm upgrade --set imageTag=<replace-with-minio-docker-image-tag> <helm-release-name> stable/minio
```

如果更新成功，你可以看到下面的输出信息

```bash
Release "your-helm-release" has been upgraded. Happy Helming!
```

## 4. 卸载Chart

假设你的版本被命名为`my-release`，使用下面的命令删除它：

```bash
$ helm delete my-release
```

该命令删除与chart关联的所有Kubernetes组件，并删除该release。

### 提示

* 在Kubernetes群集中运行的chart的实例称为release。 安装chart后，Helm会自动分配唯一的release名称。 你也可以通过下面的命令设置你心仪的名称：

```bash
$ helm install --name my-release stable/minio
```

* 为了覆盖默认的秘钥，可在运行helm install时将access key和secret key做为参数传进去。

```bash
$ helm install --set accessKey=myaccesskey,secretKey=mysecretkey \
    stable/minio
```

### 了解更多
- [Minio纠删码快速入门](https://docs.minio.io/docs/zh_CN/minio-erasure-code-quickstart-guide)
- [Kubernetes文档](https://kubernetes.io/docs/home/)
- [Helm package manager for kubernetes](https://helm.sh/)
