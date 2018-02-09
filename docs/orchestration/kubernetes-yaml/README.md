# Cloud Native Deployment of Minio on Kubernetes [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## Table of Contents

- [Prerequisites](#prerequisites)
- [Minio Standalone Server Deployment](#minio-standalone-server-deployment)
    - [Standalone Quickstart](#standalone-quickstart)
    - [Create Persistent Volume Claim](#create-persistent-volume-claim)
    - [Create Deployment](#create-minio-deployment)
    - [Create LoadBalancer Service](#create-minio-service)
  - [Update existing Minio Deployment](#update-existing-minio-deployment)
  - [Resource cleanup](#standalone-resource-cleanup)

- [Minio Distributed Server Deployment](#minio-distributed-server-deployment)
    - [Distributed Quickstart](#distributed-quickstart)
    - [Create Minio Headless Service](#create-minio-headless-service)
    - [Create Minio Statefulset](#create-minio-statefulset)
    - [Create LoadBalancer Service](#create-minio-service)
  - [Update existing Minio StatefulSet](#update-existing-minio-statefulset)
  - [Resource cleanup](#distributed-resource-cleanup)

- [Minio GCS Gateway Deployment](#minio-gcs-gateway-deployment)
    - [GCS Gateway Quickstart](#gcs-gateway-quickstart)
    - [Create GCS Credentials Secret](#create-gcs-credentials-secret)
    - [Create Minio GCS Gateway Deployment](#create-minio-gcs-gateway-deployment)
    - [Create Minio LoadBalancer Service](#create-minio-loadbalancer-service)
    - [Update Existing Minio GCS Deployment](#update-existing-minio-gcs-deployment)
  - [Resource cleanup](#gcs-gateway-resource-cleanup) 

## Prerequisites

To run this example, you need Kubernetes version >=1.4 cluster installed and running, and that you have installed the [`kubectl`](https://kubernetes.io/docs/tasks/kubectl/install/) command line tool in your path. Please see the [getting started guides](https://kubernetes.io/docs/getting-started-guides/) for installation instructions for your platform.

## Minio Standalone Server Deployment

The following section describes the process to deploy standalone [Minio](https://minio.io/) server on Kubernetes. The deployment uses the [official Minio Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

This section uses following core components of Kubernetes:

- [_Pods_](https://kubernetes.io/docs/user-guide/pods/)
- [_Services_](https://kubernetes.io/docs/user-guide/services/)
- [_Deployments_](https://kubernetes.io/docs/user-guide/deployments/)
- [_Persistent Volume Claims_](https://kubernetes.io/docs/user-guide/persistent-volumes/#persistentvolumeclaims)

### Standalone Quickstart

Run the below commands to get started quickly

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-pvc.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-deployment.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-service.yaml?raw=true
```

### Create Persistent Volume Claim

Minio needs persistent storage to store objects. If there is no
persistent storage, the data stored in Minio instance will be stored in the container file system and will be wiped off as soon as the container restarts.

Create a persistent volume claim (PVC) to request storage for the Minio instance. Kubernetes looks out for PVs matching the PVC request in the cluster and binds it to the PVC automatically.

This is the PVC description.

```sh
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  # This name uniquely identifies the PVC. This is used in deployment.
  name: minio-pv-claim
spec:
  # Read more about access modes here: http://kubernetes.io/docs/user-guide/persistent-volumes/#access-modes
  accessModes:
    # The volume is mounted as read-write by a single node
    - ReadWriteOnce
  resources:
    # This is the request for storage. Should be available in the cluster.
    requests:
      storage: 10Gi
```

Create the PersistentVolumeClaim

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-pvc.yaml?raw=true
persistentvolumeclaim "minio-pv-claim" created
```

### Create Minio Deployment

A deployment encapsulates replica sets and pods — so, if a pod goes down, replication controller makes sure another pod comes up automatically. This way you won’t need to bother about pod failures and will have a stable Minio service available.

This is the deployment description.

```sh
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  # This name uniquely identifies the Deployment
  name: minio
spec:
  strategy:
    # Specifies the strategy used to replace old Pods by new ones
    # Refer: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy
    type: Recreate
  template:
    metadata:
      labels:
        # This label is used as a selector in Service definition
        app: minio
    spec:
      # Volumes used by this deployment
      volumes:
      - name: data
        # This volume is based on PVC
        persistentVolumeClaim:
          # Name of the PVC created earlier
          claimName: minio-pv-claim
      containers:
      - name: minio
        # Volume mounts for this container
        volumeMounts:
        # Volume 'data' is mounted to path '/data'
        - name: data 
          mountPath: "/data"
        # Pulls the lastest Minio image from Docker Hub
        image: minio/minio:RELEASE.2018-02-09T22-40-05Z
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
          hostPort: 9000
```

Create the Deployment

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### Create Minio Service

Now that you have a Minio deployment running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic.

In this example, we expose the Minio Deployment by creating a LoadBalancer service. This is the service description.

```sh
apiVersion: v1
kind: Service
metadata:
  # This name uniquely identifies the service
  name: minio-service
spec:
  type: LoadBalancer
  ports:
    - port: 9000
      targetPort: 9000
      protocol: TCP
  selector:
    # Looks for labels `app:minio` in the namespace and applies the spec
    app: minio
```
Create the Minio service

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update existing Minio Deployment

You can update an existing Minio deployment to use a newer Minio release. To do this, use the `kubectl set image` command:

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
&&  kubectl delete pvc minio-pv-claim \
&& kubectl delete svc minio-service
```

## Minio Distributed Server Deployment

The following document describes the process to deploy [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide) server on Kubernetes. This example uses the [official Minio Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

This example uses following core components of Kubernetes:

- [_Pods_](https://kubernetes.io/docs/concepts/workloads/pods/pod/)
- [_Services_](https://kubernetes.io/docs/concepts/services-networking/service/)
- [_Statefulsets_](https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/)

### Distributed Quickstart

Run the below commands to get started quickly

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-headless-service.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-statefulset.yaml?raw=true
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-service.yaml?raw=true
```

### Create Minio Headless Service

Headless Service controls the domain within which StatefulSets are created. The domain managed by this Service takes the form: `$(service name).$(namespace).svc.cluster.local` (where “cluster.local” is the cluster domain), and the pods in this domain take the form: `$(pod-name-{i}).$(service name).$(namespace).svc.cluster.local`. This is required to get a DNS resolvable URL for each of the pods created within the Statefulset.

This is the Headless service description.

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

Create the Headless Service

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-headless-service.yaml?raw=true
service "minio" created
```

### Create Minio Statefulset

A StatefulSet provides a deterministic name and a unique identity to each pod, making it easy to deploy stateful distributed applications. To launch distributed Minio you need to pass drive locations as parameters to the minio server command. Then, you’ll need to run the same command on all the participating pods. StatefulSets offer a perfect way to handle this requirement.

This is the Statefulset description.

```sh
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  # This name uniquely identifies the StatefulSet
  name: minio
spec:
  serviceName: minio
  replicas: 4
  selector:
    matchLabels:
      app: minio # has to match .spec.template.metadata.labels
  template:
    metadata:
      labels:
        app: minio # has to match .spec.selector.matchLabels
    spec:
      containers:
      - name: minio
        env:
        - name: MINIO_ACCESS_KEY
          value: "minio"
        - name: MINIO_SECRET_KEY
          value: "minio123"
        image: minio/minio:RELEASE.2018-02-09T22-40-05Z
        args:
        - server
        - http://minio-0.minio.default.svc.cluster.local/data
        - http://minio-1.minio.default.svc.cluster.local/data
        - http://minio-2.minio.default.svc.cluster.local/data
        - http://minio-3.minio.default.svc.cluster.local/data
        ports:
        - containerPort: 9000
          hostPort: 9000
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
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 10Gi
```

Create the Statefulset

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-statefulset.yaml?raw=true
statefulset "minio" created
```

### Create Minio Service

Now that you have a Minio statefulset running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic.

In this example, we expose the Minio Deployment by creating a LoadBalancer service. This is the service description.

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
Create the Minio service

```sh
$ kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-distributed-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
$ kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update existing Minio StatefulSet
You can update an existing Minio StatefulSet to use a newer Minio release. To do this, use the `kubectl patch statefulset` command:

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
&&  kubectl delete svc minio \
&& kubectl delete svc minio-service
```

## Minio GCS Gateway Deployment

The following section describes the process to deploy [Minio](https://minio.io/) GCS Gateway on Kubernetes. The deployment uses the [official Minio Docker image](https://hub.docker.com/r/minio/minio/~/dockerfile/) from Docker Hub.

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
wget https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-deployment.yaml?raw=true
```

Update the section `gcp_project_id` with your GCS project ID. Then run

```sh
kubectl create -f minio-gcs-gateway-deployment.yaml
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-service.yaml?raw=true
```

### Create GCS Credentials Secret

A `secret` is intended to hold sensitive information, such as passwords, OAuth tokens, and ssh keys. Putting this information in a secret is safer and more flexible than putting it verbatim in a pod definition or in a docker image.

Create the Google Cloud Service credentials file using the steps mentioned [here](https://github.com/minio/minio/blob/master/docs/gateway/gcs.md#create-service-account-key-for-gcs-and-get-the-credentials-file). 

Use the path of file generated above to create a Kubernetes `secret`.

```sh
kubectl create secret generic gcs-credentials --from-file=/path/to/gcloud/credentials/application_default_credentials.json
```

### Create Minio GCS Gateway Deployment

A deployment encapsulates replica sets and pods — so, if a pod goes down, replication controller makes sure another pod comes up automatically. This way you won’t need to bother about pod failures and will have a stable Minio service available.

Minio Gateway uses GCS as its storage backend and need to use a GCP `projectid` to identify your credentials. Update the section `gcp_project_id` with your
GCS project ID. This is the deployment description.

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
        image: minio/minio:RELEASE.2018-02-09T22-40-05Z
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
          hostPort: 9000
        # Mount the volume into the pod
        volumeMounts:
        - name: gcs-credentials
          mountPath: "/etc/credentials"
          readOnly: true
```

Create the Deployment

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-deployment.yaml?raw=true
deployment "minio-deployment" created
```

### Create Minio LoadBalancer Service

Now that you have a Minio deployment running, you may either want to access it internally (within the cluster) or expose it as a Service onto an external (outside of your cluster, maybe public internet) IP address, depending on your use case. You can achieve this using Services. There are 3 major service types — default type is ClusterIP, which exposes a service to connection from inside the cluster. NodePort and LoadBalancer are two types that expose services to external traffic.

In this example, we expose the Minio Deployment by creating a LoadBalancer service. This is the service description.

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
Create the Minio service

```sh
kubectl create -f https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-gcs-gateway-service.yaml?raw=true
service "minio-service" created
```

The `LoadBalancer` service takes couple of minutes to launch. To check if the service was created successfully, run the command

```sh
kubectl get svc minio-service
NAME            CLUSTER-IP     EXTERNAL-IP       PORT(S)          AGE
minio-service   10.55.248.23   104.199.249.165   9000:31852/TCP   1m
```

### Update Existing Minio GCS Deployment

You can update an existing Minio deployment to use a newer Minio release. To do this, use the `kubectl set image` command:

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