# Deploy MinIO on Kubernetes [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Kubernetes concepts like Deployments and StatefulSets provide perfect platform to deploy MinIO server in standalone, distributed or gateway mode. There are multiple options to deploy MinIO on Kubernetes, you can choose the one that best suits your requirements.

- Helm Chart: MinIO Helm Chart offers customizable and easy MinIO deployment with a single command. Refer [MinIO Helm Chart repository documentation](https://github.com/helm/charts/tree/master/stable/minio) for more details.

- YAML File: MinIO can be deployed with `yaml` files via `kubectl`. This document outlines steps required to deploy MinIO using `yaml` files.

## Table of Contents

- [Prerequisites](#Prerequisites)
- [MinIO Standalone Server Deployment](#minio-standalone-server-deployment)
- [MinIO Distributed Server Deployment](#minio-distributed-server-deployment)
- [MinIO GCS Gateway Deployment](#minio-gcs-gateway-deployment)
- [Monitoring MinIO in Kubernetes](#monitoring-minio)

<a name="Prerequisites"></a>

## Prerequisites

To run this example, you need Kubernetes version >=1.4 cluster installed and running, and that you have installed the [`kubectl`](https://kubernetes.io/docs/tasks/kubectl/install/) command line tool in your path. Please see the [getting started guides](https://kubernetes.io/docs/getting-started-guides/) for installation instructions for your platform.

<a name="minio-standalone-server-deployment"></a>

## MinIO Standalone Server Deployment

The following section describes the process to deploy standalone [MinIO](https://min.io/) server on Kubernetes. The deployment uses the [official MinIO Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

This section uses following core components of Kubernetes:

- [_Pods_](https://kubernetes.io/docs/user-guide/pods/)
- [_Services_](https://kubernetes.io/docs/user-guide/services/)
- [_Deployments_](https://kubernetes.io/docs/user-guide/deployments/)
- [_Persistent Volume Claims_](https://kubernetes.io/docs/user-guide/persistent-volumes/#persistentvolumeclaims)

### Standalone Quickstart

Run the below commands to get started quickly

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-pvc.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-deployment.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-service.yaml?raw=true
```

### Create Persistent Volume Claim

MinIO needs persistent storage to store objects. If there is no persistent storage, the data stored in MinIO instance will be stored in the container file system and will be wiped off as soon as the container restarts.

Create a persistent volume claim (PVC) to request storage for the MinIO instance. Kubernetes looks out for PVs matching the PVC request in the cluster and binds it to the PVC automatically. Create the PersistentVolumeClaim 


```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-pvc.yaml?raw=true
persistentvolumeclaim "minio-pv-claim" created
```

### Create MinIO Deployment

A deployment encapsulates replica sets and pods. If a pod goes down, replication controller makes sure another pod comes up automatically. This way you won’t need to bother about pod failures and will have a stable MinIO service available. Create the Deployment using the following command

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### Create MinIO Service

Now that you have a MinIO deployment running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic.

In this example, we expose the MinIO Deployment by creating a LoadBalancer service. Create the MinIO service using the following command

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update existing MinIO Deployment

You can update an existing MinIO deployment to use a newer MinIO release. To do this, use the `kubectl set image` command:

```sh
kubectl set image deployment/minio-deployment minio=<replace-with-new-minio-image>
```

Kubernetes will restart the deployment to update the image. You will get a message as shown below, on successful update:

```
deployment "minio-deployment" image updated
```

### Standalone Resource cleanup

You can cleanup the cluster using

```sh
kubectl delete deployment minio \
&& kubectl delete pvc minio-pv-claim \
&& kubectl delete svc minio-service
```

<a name="minio-distributed-server-deployment"></a>

## MinIO Distributed Server Deployment

The following document describes the process to deploy [distributed MinIO](https://docs.min.io/docs/distributed-minio-quickstart-guide) server on Kubernetes. This example uses the [official MinIO Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

This example uses following core components of Kubernetes:

- [_Pods_](https://kubernetes.io/docs/concepts/workloads/pods/pod/)
- [_Services_](https://kubernetes.io/docs/concepts/services-networking/service/)
- [_Statefulsets_](https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/)

### Distributed Quickstart

Run the below commands to get started quickly

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-headless-service.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-statefulset.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-service.yaml?raw=true
```

### Create MinIO Headless Service

Headless Service controls the domain within which StatefulSets are created. The domain managed by this Service takes the form: `$(service name).$(namespace).svc.cluster.local` (where “cluster.local” is the cluster domain), and the pods in this domain take the form: `$(pod-name-{i}).$(service name).$(namespace).svc.cluster.local`. This is required to get a DNS resolvable URL for each of the pods created within the Statefulset. Create the Headless Service using the following command

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-headless-service.yaml?raw=true
service "minio" created
```

### Create MinIO Statefulset

A StatefulSet provides a deterministic name and a unique identity to each pod, making it easy to deploy stateful distributed applications. To launch distributed MinIO you need to pass drive locations as parameters to the minio server command. Then, you’ll need to run the same command on all the participating pods. StatefulSets offer a perfect way to handle this requirement. Create the Statefulset using the following command

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-statefulset.yaml?raw=true
statefulset "minio" created
```

### Create MinIO Service

Now that you have a MinIO statefulset running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic. Create the MinIO service using the following command

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
$ kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update existing MinIO StatefulSet

You can update an existing MinIO StatefulSet to use a newer MinIO release. To do this, use the `kubectl patch statefulset` command:

```sh
kubectl patch statefulset minio --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"<replace-with-new-minio-image>"}]'
```

On successful update, you should see the output below

```
statefulset "minio" patched
```

Then delete all the pods in your StatefulSet one by one as shown below. Kubernetes will restart those pods for you, using the new image.

```sh
kubectl delete minio-0
```

### Resource cleanup

You can cleanup the cluster using
```sh
kubectl delete statefulset minio \
&& kubectl delete svc minio \
&& kubectl delete svc minio-service
```

### Deploying on cluster nodes with local host path

If your cluster does not have a storage solution or PV abstraction, you must explicitly define what nodes you wish to run MinIO on, and define a homogeneous path to a local fast block device available on every host.

This must be changed in the example daemonset: [minio-distributed-daemonset.yaml](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-distributed-daemonset.yaml)

Specifically the hostpath:
```yaml
        hostPath:
          path: /data/minio/
```

And the list of hosts:
```yaml
        - http://hostname1:9000/data/minio
        - http://hostname2:9000/data/minio
        - http://hostname3:9000/data/minio
        - http://hostname4:9000/data/minio
```

Once deployed, tag the defined host with the `minio-server=true` label:

```bash
kubectl label node hostname1  -l minio-server=true
kubectl label node hostname2  -l minio-server=true
kubectl label node hostname3  -l minio-server=true
kubectl label node hostname4  -l minio-server=true
```

<a name="minio-gcs-gateway-deployment"></a>

## MinIO GCS Gateway Deployment

The following section describes the process to deploy [MinIO](https://min.io/) GCS Gateway on Kubernetes. The deployment uses the [official MinIO Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

This section uses following core components of Kubernetes:

- [_Secrets_](https://kubernetes.io/docs/concepts/configuration/secret/)
- [_Services_](https://kubernetes.io/docs/user-guide/services/)
- [_Deployments_](https://kubernetes.io/docs/user-guide/deployments/)

### GCS Gateway Quickstart

Create the Google Cloud Service credentials file using the steps mentioned [here](https://github.com/minio/minio/blob/master/docs/gateway/gcs.md#create-service-account-key-for-gcs-and-get-the-credentials-file). 

Use the path of file generated above to create a Kubernetes `secret`.

```sh
kubectl create secret generic gcs-credentials --from-file=/path/to/gcloud/credentials/application_default_credentials.json
```

Then download the `minio-gcs-gateway-deployment.yaml` file

```sh
wget https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-gcs-gateway-deployment.yaml?raw=true
```

Update the section `gcp_project_id` with your GCS project ID. Then run

```sh
kubectl create -f minio-gcs-gateway-deployment.yaml
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-gcs-gateway-service.yaml?raw=true
```

### Create GCS Credentials Secret

A `secret` is intended to hold sensitive information, such as passwords, OAuth tokens, and ssh keys. Putting this information in a secret is safer and more flexible than putting it verbatim in a pod definition or in a docker image.

Create the Google Cloud Service credentials file using the steps mentioned [here](https://github.com/minio/minio/blob/master/docs/gateway/gcs.md#create-service-account-key-for-gcs-and-get-the-credentials-file). 

Use the path of file generated above to create a Kubernetes `secret`.

```sh
kubectl create secret generic gcs-credentials --from-file=/path/to/gcloud/credentials/application_default_credentials.json
```

### Create MinIO GCS Gateway Deployment

A deployment encapsulates replica sets and pods — so, if a pod goes down, replication controller makes sure another pod comes up automatically. This way you won’t need to bother about pod failures and will have a stable MinIO service available.

MinIO Gateway uses GCS as its storage backend and need to use a GCP `projectid` to identify your credentials. Update the section `gcp_project_id` with your
GCS project ID. Create the Deployment using the following command

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-gcs-gateway-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### Create MinIO LoadBalancer Service

Now that you have a MinIO deployment running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic. Create the MinIO service using the following command

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-gcs-gateway-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update Existing MinIO GCS Deployment

You can update an existing MinIO deployment to use a newer MinIO release. To do this, use the `kubectl set image` command:

```sh
kubectl set image deployment/minio-deployment minio=<replace-with-new-minio-image>
```

Kubernetes will restart the deployment to update the image. You will get a message as shown below, on successful update:

```
deployment "minio-deployment" image updated
```

### GCS Gateway Resource Cleanup

You can cleanup the cluster using

```sh
kubectl delete deployment minio-deployment \
&&  kubectl delete secret gcs-credentials 
```

<a name="monitoring-minio"></a>

## Monitoring MinIO in Kubernetes

MinIO server exposes un-authenticated readiness and liveness endpoints so Kubernetes can natively identify unhealthy MinIO containers. MinIO also exposes Prometheus compatible data on a different endpoint to enable Prometheus users to natively monitor their MinIO deployments.

_Note_ : Readiness check is not allowed in distributed MinIO deployment. This is because Kubernetes doesn't allow any traffic to containers whose Readiness checks fail, and in a distributed setup, MinIO server can't respond to Readiness checks until all the nodes are reachable. So, Liveness checks are recommended native Kubernetes monitoring approach for distributed MinIO StatefulSets. Read more about Kubernetes recommendations for [container probes](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#container-probes).

## Explore Further
- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Helm package manager for kubernetes](https://helm.sh/)
