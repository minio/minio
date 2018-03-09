# 使用Kubernetes做Minio的云原生部署 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## 目录
- [前提条件](#前提条件)
- [Minio独立模式部署](#Minio-Standalone模式部署)
    - [Minio独立模式快速入门](#Minio独立模式快速入门)
    - [创建持久卷声明](#Minio独立模式快速入门)
    - [创建Minio的部署](#创建Minio的部署)
    - [创建Miniio服务](#创建Miniio服务)
  - [更新已有的Minio部署](#更新已有的Minio部署)
  - [独立模式资源清理](#独立模式资源清理)

- [Minio分布式服务部署](#Minio分布式服务部署)
    - [分布式快速入门](#分布式快速入门)
    - [创建Minio Headless服务](#创建Minio-Headless服务)
    - [创建Minio Statefulset](#创建Minio-Statefulset)
    - [创建负载均衡服务](#创建负载均衡服务)
  - [更新已存在的Minio StatefulSet](#更新已存在的Minio-StatefulSet)
  - [分布式模式资源清理](#分布式模式资源清理)

- [Minio GCS 网关部署](#Minio-GCS网关部署)
    - [GCS 网关快速入门](#GCS-网关快速入门)
    - [创建GCS凭据](#创建GCS凭据)
    - [创建Minio GCS Gateway部署](#创建Minio-GCS-Gateway部署)
    - [创建Minio LoadBalancer服务](#创建Minio-LoadBalancer服务)
    - [更新现有的Minio GCS部署](#更新现有的Minio-GCS部署)
  - [GCS网关资源清理](#GCS网关资源清理) 

## 前提条件

运行该示例，你需要安装并运行Kubernetes版本>=1.4的集群，而且已经安装 [`kubectl`](https://kubernetes.io/docs/tasks/kubectl/install/) 命令行工具。请访问
[getting started guides](https://kubernetes.io/docs/getting-started-guides/)获取响应平台的安装指导。
## Minio Standalone模式部署

以下部分描述了如何在Kubernetes上部署一个独立的 [Minio](https://minio.io/) 服务。部署使用的是Docker Hub上的 [官方Minio Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) 。

此部分使用了以下Kubernetes的核心组件:

- [_Pods_](https://kubernetes.io/docs/user-guide/pods/)
- [_Services_](https://kubernetes.io/docs/user-guide/services/)
- [_Deployments_](https://kubernetes.io/docs/user-guide/deployments/)
- [_Persistent Volume Claims_](https://kubernetes.io/docs/user-guide/persistent-volumes/#persistentvolumeclaims)

### Minio独立模式快速入门

运行下面的命令快速启动

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-pvc.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-deployment.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-service.yaml?raw=true
```

### 创建持久卷声明

Minio需要持久卷来存储对象。如果没有持久卷，Minio实例中的数据将会存到容器的文件系统中，而且在容器重启时会被清除的干干净净。
创建一个持久卷声明（PVC），为Minio实例请求存储。Kubernetes寻找与群集中的PVC请求匹配的PV，并自动将其绑定到PVC。

这是一个PVC的描述

```sh
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  # 此名称唯一标识PVC。 将在以下部署中使用。
  name: minio-pv-claim
  annotations:
    volume.alpha.kubernetes.io/storage-class: anything
  labels:
    app: minio-storage-claim
spec:
  # 关于 access modes的更多细节，访问这里: http://kubernetes.io/docs/user-guide/persistent-volumes/#access-modes
  accessModes:
    - ReadWriteOnce
  resources:
    # This is the request for storage. Should be available in the cluster.
    requests:
      storage: 10Gi
```

创建一个持久卷声明

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-pvc.yaml?raw=true
persistentvolumeclaim "minio-pv-claim" created
```

### 创建Minio的部署

部署封装了副本集和pod - 因此，如果pod掉线，复制控制器会确保另一个pod自动出现。 这样，您就不必担心pod失败，并且可以提供稳定的Minio服务。

这是一个部署的描述

```sh
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  # This name uniquely identifies the Deployment
  name: minio-deployment
spec:
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        # Label is used as selector in the service.
        app: minio
    spec:
      # Refer to the PVC created earlier
      volumes:
      - name: data
        persistentVolumeClaim:
          # Name of the PVC created earlier
          claimName: minio-pv-claim
      containers:
      - name: minio
        # Pulls the default Minio image from Docker Hub
        image: minio/minio:RELEASE.2017-05-05T01-14-51Z
        args:
        - server
        - /data
        env:
        # Minio access key and secret key
        - name: MINIO_ACCESS_KEY
          value: "minio"
        - name: MINIO_SECRET_KEY
          value: "minio123"
        ports:
        - containerPort: 9000
        # Mount the volume into the pod
        volumeMounts:
        - name: data # must match the volume name, above
          mountPath: "/data"
```

创建一个部署

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### 创建Miniio服务

现在您正在运行Minio部署，您可能希望在内部（集群内）访问它，或者将其作为服务暴露在外部（集群外部，也可能是公共Internet）IP地址，具体取决于用例。 您可以使用服务来实现此目的。 有三种主要的服务类型 - 默认类型是ClusterIP，它将集群内部的连接暴露给服务。 NodePort和LoadBalancer是向外部流量提供服务的两种类型。

在此示例中，我们通过创建LoadBalancer服务来公开Minio部署。 
这是服务描述。

```sh
apiVersion: v1
kind: Service
metadata:
  name: minio-service
spec:
  type: LoadBalancer
  ports:
    - port: 9000
      targetPort: 9000
      protocol: TCP
  selector:
    app: minio
```
创建Minio服务

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-service.yaml?raw=true
service "minio-service" created
```

`LoadBalancer` 服务需要几分钟才能启动。 要检查服务是否已成功创建，请运行命令

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### 更新已有的Minio部署

您可以更新现有的Minio部署以使用较新的Minio版本。 为此，请使用`kubectl set image`命令：

```sh
kubectl set image deployment/minio-deployment minio=<replace-with-new-minio-image>
```

Kubernetes将重新启动部署以更新镜像。 成功更新后，您将收到以下消息：

```
deployment "minio-deployment" image updated
```

### 独立模式资源清理

你可以清理集群占用的资源，请运行：

```sh
kubectl delete deployment minio-deployment \
&&  kubectl delete pvc minio-pv-claim \
&& kubectl delete svc minio-service
```

## Minio分布式服务部署

以下文档介绍了在Kubernetes上部署[分布式Minio](https://docs.minio.io/docs/zh_CN/distributed-minio-quickstart-guide)服务器的过程。 本示例使用Docker Hub的[官方Minio Docker镜像](https://hub.docker.com/r/minio/minio/~/dockerfile/)。

此示例使用以下Kubernetes的核心组件：

- [_Pods_](https://kubernetes.io/docs/concepts/workloads/pods/pod/)
- [_Services_](https://kubernetes.io/docs/concepts/services-networking/service/)
- [_Statefulsets_](https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/)

### 分布式快速入门

运行下面的命令快速启动

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-headless-service.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-statefulset.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-service.yaml?raw=true
```

###创建Minio Headless服务

Headless服务控制在其中创建StatefulSets的域。此服务管理的域采用以下格式：`$(service name).$(namespace).svc.cluster.local`（其中“cluster.local”是集群域），此域中的pod采用形式: `$(pod-name-{i}).$(service name).$(namespace).svc.cluster.local`。这里需要DNS来解析在Statefulset中创建的每个pods的URL。

这是Headless service的描述。

```sh
apiVersion: v1
kind: Service
metadata:
  name: minio
  labels:
    app: minio
spec:
  clusterIP: None
  ports:
    - port: 9000
      name: minio
  selector:
    app: minio
```

创建Headless服务

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-headless-service.yaml?raw=true
service "minio" created
```

###创建Minio Statefulset

StatefulSet为每个pod提供确定性名称和唯一身份，从而轻松部署有状态的分布式应用程序。 要启动分布式Minio，您需要将驱动器位置作为参数传递到minio服务的命令。 然后，您需要在所有参与的pod上运行相同的命令。 StatefulSets提供了一个完美的方式来处理这个要求。

这是Statefulset的描述。

```sh
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: minio
spec:
  serviceName: minio
  replicas: 4
  template:
    metadata:
      annotations:
        pod.alpha.kubernetes.io/initialized: "true"
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        env:
        - name: MINIO_ACCESS_KEY
          value: "minio"
        - name: MINIO_SECRET_KEY
          value: "minio123"
        image: minio/minio:RELEASE.2017-05-05T01-14-51Z
        args:
        - server
        - http://minio-0.minio.default.svc.cluster.local/data
        - http://minio-1.minio.default.svc.cluster.local/data
        - http://minio-2.minio.default.svc.cluster.local/data
        - http://minio-3.minio.default.svc.cluster.local/data
        ports:
        - containerPort: 9000
        # These volume mounts are persistent. Each pod in the PetSet
        # gets a volume mounted based on this field.
        volumeMounts:
        - name: data
          mountPath: /data
  # These are converted to volume claims by the controller
  # and mounted at the paths mentioned above.
  volumeClaimTemplates:
  - metadata:
      name: data
      annotations:
        volume.alpha.kubernetes.io/storage-class: anything
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 10Gi
```

创建Statefulset

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-statefulset.yaml?raw=true
statefulset "minio" created
```

### 创建负载均衡服务

现在您已经运行了Minio statefulset，您可能希望在内部（集群内）访问它，或将其作为服务暴露在外部（集群外，也可能是公用Internet）的IP地址，具体取决于用例。 您可以使用服务来实现此目的。 有三种主要的服务类型 - 默认类型是ClusterIP，它将集群内部的连接暴露给服务。 NodePort和LoadBalancer是向外部流量提供服务的两种类型。

在此示例中，我们通过创建LoadBalancer服务来公开Minio部署。
 这是服务描述。

```sh
apiVersion: v1
kind: Service
metadata:
  name: minio-service
spec:
  type: LoadBalancer
  ports:
    - port: 9000
      targetPort: 9000
      protocol: TCP
  selector:
    app: minio
```
创建Minio service

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-service.yaml?raw=true
service "minio-service" created
```

`LoadBalancer` 服务需要几分钟才能启动。 要检查服务是否已成功创建，请运行命令

```sh
$ kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

###更新已经存在的Minio StatefulSet
您可以更新现有的Minio StatefulSet以使用较新的Minio版本。 为此，请使用`kubectl patch statefulset`命令：

```sh
kubectl patch statefulset minio --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"<replace-with-new-minio-image>"}]'
```

成功更新后，您应该会看到下面的输出

```
statefulset "minio" patched
```

然后如下所示，逐一删除StatefulSet中的所有pod。 Kubernetes将使用新的镜像为您重新启动那些pod。

```sh
kubectl delete minio-0
```

### 分布式模式资源清理

你可以使用以下命令清理集群
```sh
kubectl delete statefulset minio \
&&  kubectl delete svc minio \
&& kubectl delete svc minio-service
```

## Minio GCS网关部署

以下部分介绍在Kubernetes上部署[Minio](https://minio.io/)GCS Gateway的过程。 部署使用Docker Hub的[官方Minio Docker映像](https://hub.docker.com/r/minio/minio/~/dockerfile/)。

此示例使用以下Kubernetes的核心组件：

- [_Secrets_](https://kubernetes.io/docs/concepts/configuration/secret/)
- [_Services_](https://kubernetes.io/docs/user-guide/services/)
- [_Deployments_](https://kubernetes.io/docs/user-guide/deployments/)

### GCS 网关快速入门

按照 [这里](https://github.com/minio/minio/blob/master/docs/gateway/gcs.md#create-service-account-key-for-gcs-and-get-the-credentials-file)描述的步骤创建Google云服务认证凭据文件。

使用上面生成的文件来创建一个Kubernetes`secret`。

```sh
kubectl create secret generic gcs-credentials --from-file=/path/to/gcloud/credentials/application_default_credentials.json
```

下载 `minio-gcs-gateway-deployment.yaml` 

```sh
wget https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-deployment.yaml?raw=true
```

使用你的GCS  project ID更新 `gcp_project_id`部分的内容，然后运行 

```sh
kubectl create -f minio-gcs-gateway-deployment.yaml
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-service.yaml?raw=true
```

### 创建GCS凭据

`凭据`旨在保存敏感信息，例如密码，OAuth令牌和ssh密钥。 将这些信息放在一个凭据中比将其逐字地放在pod定义或docker镜像中更安全，更灵活。

按照 [这里](https://github.com/minio/minio/blob/master/docs/gateway/gcs.md#create-service-account-key-for-gcs-and-get-the-credentials-file)描述的步骤创建Google云服务认证凭据文件。

使用上面生成的文件来创建一个Kubernetes`secret`。

```sh
kubectl create secret generic gcs-credentials --from-file=/path/to/gcloud/credentials/application_default_credentials.json
```

### 创建Minio GCS Gateway部署

部署封装了副本集和pod - 因此，如果pod掉线，复制控制器会确保另一个pod自动出现。 这样，您就不必担心pod失败，并且可以提供稳定的Minio服务。

Minio Gateway使用GCS作为其存储后端，需要使用GCP“projectid”来识别您的凭据。 使用GCS项目ID更新“gcp_project_id”部分。 这是部署描述。

```sh
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  # This name uniquely identifies the Deployment
  name: minio-deployment
spec:
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        # Label is used as selector in the service.
        app: minio
    spec:
      # Refer to the secret created earlier
      volumes:
      - name: gcs-credentials
        secret:
          # Name of the Secret created earlier
          secretName: gcs-credentials
      containers:
      - name: minio
        # Pulls the default Minio image from Docker Hub
        image: minio/minio:RELEASE.2017-08-05T00-00-53Z
        args:
        - gateway
        - gcs
        - gcp_project_id
        env:
        # Minio access key and secret key
        - name: MINIO_ACCESS_KEY
          value: "minio"
        - name: MINIO_SECRET_KEY
          value: "minio123"
        # Google Cloud Service uses this variable
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: "/etc/credentials/application_default_credentials.json"
        ports:
        - containerPort: 9000
        # Mount the volume into the pod
        volumeMounts:
        - name: gcs-credentials
          mountPath: "/etc/credentials"
          readOnly: true
```

创建部署

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### 创建Minio LoadBalancer服务

现在您正在运行Minio，您可能希望在内部（集群内）访问它，或者将其作为服务暴露在外部（集群外部，也可能是公共Internet）IP地址，具体取决于用例。 您可以使用服务来实现此目的。 有三种主要的服务类型 - 默认类型是ClusterIP，它将集群内部的连接暴露给服务。 NodePort和LoadBalancer是向外部流量提供服务的两种类型。

在此示例中，我们通过创建LoadBalancer服务来暴露Minio。 这是服务描述。

```sh
apiVersion: v1
kind: Service
metadata:
  name: minio-service
spec:
  type: LoadBalancer
  ports:
    - port: 9000
      targetPort: 9000
      protocol: TCP
  selector:
    app: minio
```
创建Minio服务

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-service.yaml?raw=true
service "minio-service" created
```

`LoadBalancer`服务需要几分钟才能启动。 要检查服务是否已成功创建，请运行命令

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### 更新现有的Minio GCS部署

您可以更新现有的Minio部署以使用较新的Minio版本。 为此，请使用`kubectl set image`命令：

```sh
kubectl set image deployment/minio-deployment minio=<replace-with-new-minio-image>
```

Kubernetes将重新启动部署以更新镜像。 成功更新后，您将收到以下消息：
```
deployment "minio-deployment" image updated
```

### GCS网关资源清理

你可以使用下面的命令清理集群

```sh
kubectl delete deployment minio-deployment \
&&  kubectl delete secret gcs-credentials 
```