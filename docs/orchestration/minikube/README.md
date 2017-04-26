# Deploy distributed Minio locally with minikube [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minikube runs a single-node Kubernetes cluster inside a VM on your computer. This makes it easy to deploy distributed Minio server on
Kubernetes running locally on your computer.

## 1. Prerequisites

[Minikube](https://github.com/kubernetes/minikube/blob/master/README.md#installation) and [`kubectl`](https://kubernetes.io/docs/user-guide/prereqs/)
installed on your system.

## 2. Steps

* Download `minio_distributed.sh` and `statefulset.yaml`

```sh
wget https://raw.githubusercontent.com/minio/minio/master/docs/orchestration/minikube/minio_distributed.sh  
wget https://raw.githubusercontent.com/minio/minio/master/docs/orchestration/minikube/statefulset.yaml
```

* Execute the `minio_distributed.sh` script in command prompt.

```sh
./minio_distributed.sh
```

After the script is executed successfully, you should get an output like this

```sh
service "minio-public" created
service "minio" created
statefulset "minio" created
```
This means Minio is deployed on your local Minikube installation.

Note that the service `minio-public` is a [clusterIP](https://kubernetes.io/docs/user-guide/services/#publishing-services---service-types) service. It exposes the service on a cluster-internal IP. To connect to your Minio instances via `kubectl port-forward` command, execute

```
kubectl port-forward minio-0 9000:9000
```

Minio server can now be accessed at `http://localhost:9000`, with accessKey and secretKey as mentioned in the `statefulset.yaml` file.

## 3. Notes

Minikube currently does not support dynamic provisioning, so we manually create PersistentVolumes(PV) and PersistentVolumeClaims(PVC). Once the PVs and PVCs are created, we call the `statefulset.yaml` configuration file to create the distributed Minio setup.

This setup runs on a laptop/computer. Hence only one disk is used as the backend for all the minio instance PVs. Minio sees these PVs as separate disks and reports the available storage incorrectly.
