# Deploy Minio on Kubernetes [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Kubernetes constructs like Deployments and StatefulSets provide perfect platform to deploy Minio server in standalone, distributed or shared mode. In addition, using Minio [Helm](https://helm.sh) Chart, you can deploy Minio server with a single command on your cluster. 

Minio Helm Chart offers great deal of [customizability](#configuration), still if you'd rather like to deploy Minio using custom config files, you can do that as well. This [blog post](https://blog.minio.io/build-aws-s3-compatible-cloud-storage-on-gcp-with-minio-and-kubernetes-159cc99caea8#.8zesfh6tc) offers an introduction to running Minio on Kubernetes using .yaml configuration files.

## 1. Prerequisites

* Kubernetes 1.4+ with Beta APIs enabled for default standalone mode.
* Kubernetes 1.5+ with Beta APIs enabled to run Minio in [distributed mode](#distributed-minio).
* PV provisioner support in the underlying infrastructure. 
* Helm package manager [installed](https://github.com/kubernetes/helm#install) on your Kubernetes cluster. 

## 2. Deploy Minio using Helm Chart

Install Minio chart by

```bash
$ helm install stable/minio
```
Above command deploys Minio on the Kubernetes cluster in the default configuration. Below section lists all the configurable parameters of the Minio chart and their default values.

### Configuration

| Parameter                  | Description                         | Default                                                 |
|----------------------------|-------------------------------------|---------------------------------------------------------|
| `image`                    | Minio image name                    | `minio/minio`                                           |
| `imageTag`                 | Minio image tag. Possible values listed [here](https://hub.docker.com/r/minio/minio/tags/).| `RELEASE.2017-01-25T03-14-52Z`|
| `imagePullPolicy`          | Image pull policy                   | `Always`                                                |
| `mode`                     | Minio server mode (`standalone`, `shared` or `distributed`)| `standalone`                     |
| `numberOfNodes`            | Number of nodes (applicable only for Minio distributed mode). Should be 4 <= x <= 16 | `4`    |
| `accessKey`                | Default access key                  | `AKIAIOSFODNN7EXAMPLE`                                  |
| `secretKey`                | Default secret key                  | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`              |
| `configPath`               | Default config file location        | `~/.minio`                                              |
| `mountPath`                | Default mount location for persistent drive| `/export`                                        |
| `serviceType`              | Kubernetes service type             | `LoadBalancer`                                          |
| `servicePort`              | Kubernetes port where service is exposed| `9000`                                              |
| `persistence.enabled`      | Use persistent volume to store data | `true`                                                  |
| `persistence.size`         | Size of persistent volume claim     | `10Gi`                                                  |
| `persistence.storageClass` | Type of persistent volume claim     | `generic`                                               |
| `persistence.accessMode`   | ReadWriteOnce or ReadOnly           | `ReadWriteOnce`                                         |
| `resources`                | CPU/Memory resource requests/limits | Memory: `256Mi`, CPU: `100m`                            |

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name my-release \
  --set persistence.size=100Gi \
    stable/minio
```

The above command deploys Minio server with a 100Gi backing persistent volume.

Alternately, you can provide a YAML file that specifies parameter values while installing the chart. For example,

```bash
$ helm install --name my-release -f values.yaml stable/minio
```

### Distributed Minio

This chart provisions a Minio server in standalone mode, by default. To provision Minio server in [distributed mode](https://docs.minio.io/docs/distributed-minio-quickstart-guide), set the `mode` field to `distributed`,

```bash
$ helm install --set mode=distributed stable/minio
```

This provisions Minio server in distributed mode with 4 nodes. To change the number of nodes in your distributed Minio server, set the `numberOfNodes` field,

```bash
$ helm install --set mode=distributed,numberOfNodes=8 stable/minio
```

This provisions Minio server in distributed mode with 8 nodes. Note that the `numberOfNodes` value should be an integer between 4 and 16 (inclusive).

#### StatefulSet [limitations](http://kubernetes.io/docs/concepts/abstractions/controllers/statefulsets/#limitations) applicable to distributed Minio

* StatefulSets need persistent storage, so the `persistence.enabled` flag is ignored when `mode` is set to `distributed`.
* When uninstalling a distributed Minio release, you'll need to manually delete volumes associated with the StatefulSet.

### Shared Minio

To provision Minio servers in [shared mode](https://github.com/minio/minio/blob/master/docs/shared-backend/README.md), set the `mode` field to `shared`,

```bash
$ helm install --set mode=shared stable/minio
```

This provisions 4 Minio server nodes backed by single storage. To change the number of nodes in your shared Minio deployment, set the `numberOfNodes` field,

```bash
$ helm install --set mode=shared,numberOfNodes=8 stable/minio
```

This provisions Minio server in shared mode with 8 nodes.

### Persistence

This chart provisions a PersistentVolumeClaim and mounts corresponding persistent volume to default location `/export`. You'll need physical storage available in the Kubernetes cluster for this to work. If you'd rather use `emptyDir`, disable PersistentVolumeClaim by:

```bash
$ helm install --set persistence.enabled=false stable/minio
```

> *"An emptyDir volume is first created when a Pod is assigned to a Node, and exists as long as that Pod is running on that node. When a Pod is removed from a node for any reason, the data in the emptyDir is deleted forever."*

## 3. Uninstalling the Chart

Assuming your release is named as `my-release`, delete it using the command:

```bash
$ helm delete my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

### Notes

* An instance of a chart running in a Kubernetes cluster is called a release. Helm automatically assigns a unique release name after installing the chart. You can also set your preferred name by:

```bash
$ helm install --name my-release stable/minio
```

* To override the default keys, pass the access and secret keys as arguments to helm install.

```bash
$ helm install --set accessKey=myaccesskey,secretKey=mysecretkey \
    stable/minio
```

### Explore Further
- [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
