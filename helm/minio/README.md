# MinIO Helm Chart
[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![license](https://img.shields.io/badge/license-AGPL%20V3-blue)](https://github.com/minio/minio/blob/master/LICENSE)

MinIO is a High Performance Object Storage released under GNU Affero General Public License v3.0. It is API compatible with Amazon S3 cloud storage service. Use MinIO to build high performance infrastructure for machine learning, analytics and application data workloads.

For more detailed documentation please visit [here](https://docs.minio.io/)

Introduction
------------

This chart bootstraps MinIO Cluster on [Kubernetes](http://kubernetes.io) using the [Helm](https://helm.sh) package manager.

Prerequisites
-------------

- Helm cli with Kubernetes cluster configured.
- PV provisioner support in the underlying infrastructure. (We recommend using https://github.com/minio/direct-csi)
- Use Kubernetes version v1.19 and later for best experience.

Configure MinIO Helm repo
--------------------
```bash
helm repo add minio https://charts.min.io/
```

Installing the Chart
--------------------

Install this chart using:

```bash
helm install --namespace minio --set rootUser=rootuser,rootPassword=rootpass123 --generate-name minio/minio
```

The command deploys MinIO on the Kubernetes cluster in the default configuration. The [configuration](#configuration) section lists the parameters that can be configured during installation.

Upgrading the Chart
-------------------

You can use Helm to update MinIO version in a live release. Assuming your release is named as `my-release`, get the values using the command:

```bash
helm get values my-release > old_values.yaml
```

Then change the field `image.tag` in `old_values.yaml` file with MinIO image tag you want to use. Now update the chart using

```bash
helm upgrade -f old_values.yaml my-release minio/minio
```

Default upgrade strategies are specified in the `values.yaml` file. Update these fields if you'd like to use a different strategy.

Configuration
-------------

Refer the [Values file](./values.yaml) for all the possible config fields.

You can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
helm install --name my-release --set persistence.size=1Ti minio/minio
```

The above command deploys MinIO server with a 1Ti backing persistent volume.

Alternately, you can provide a YAML file that specifies parameter values while installing the chart. For example,

```bash
helm install --name my-release -f values.yaml minio/minio
```

Persistence
-----------

This chart provisions a PersistentVolumeClaim and mounts corresponding persistent volume to default location `/export`. You'll need physical storage available in the Kubernetes cluster for this to work. If you'd rather use `emptyDir`, disable PersistentVolumeClaim by:

```bash
helm install --set persistence.enabled=false minio/minio
```

> *"An emptyDir volume is first created when a Pod is assigned to a Node, and exists as long as that Pod is running on that node. When a Pod is removed from a node for any reason, the data in the emptyDir is deleted forever."*

Existing PersistentVolumeClaim
------------------------------

If a Persistent Volume Claim already exists, specify it during installation.

1. Create the PersistentVolume
2. Create the PersistentVolumeClaim
3. Install the chart

```bash
helm install --set persistence.existingClaim=PVC_NAME minio/minio
```

NetworkPolicy
-------------

To enable network policy for MinIO,
install [a networking plugin that implements the Kubernetes
NetworkPolicy spec](https://kubernetes.io/docs/tasks/administer-cluster/declare-network-policy#before-you-begin),
and set `networkPolicy.enabled` to `true`.

For Kubernetes v1.5 & v1.6, you must also turn on NetworkPolicy by setting
the DefaultDeny namespace annotation. Note: this will enforce policy for _all_ pods in the namespace:

```
kubectl annotate namespace default "net.beta.kubernetes.io/network-policy={\"ingress\":{\"isolation\":\"DefaultDeny\"}}"
```

With NetworkPolicy enabled, traffic will be limited to just port 9000.

For more precise policy, set `networkPolicy.allowExternal=true`. This will
only allow pods with the generated client label to connect to MinIO.
This label will be displayed in the output of a successful install.

Existing secret
---------------

Instead of having this chart create the secret for you, you can supply a preexisting secret, much
like an existing PersistentVolumeClaim.

First, create the secret:
```bash
kubectl create secret generic my-minio-secret --from-literal=rootUser=foobarbaz --from-literal=rootPassword=foobarbazqux
```

Then install the chart, specifying that you want to use an existing secret:
```bash
helm install --set existingSecret=my-minio-secret minio/minio
```

The following fields are expected in the secret:

| .data.<key> in Secret | Corresponding variable | Description    | Required |
|:----------------------|:-----------------------|:---------------|:---------|
| `rootUser`            | `rootUser`             | Root user.     | yes      |
| `rootPassword`        | `rootPassword`         | Root password. | yes      |

All corresponding variables will be ignored in values file.

Configure TLS
-------------

To enable TLS for MinIO containers, acquire TLS certificates from a CA or create self-signed certificates. While creating / acquiring certificates ensure the corresponding domain names are set as per the standard [DNS naming conventions](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-identity) in a Kubernetes StatefulSet (for a distributed MinIO setup). Then create a secret using

```bash
kubectl create secret generic tls-ssl-minio --from-file=path/to/private.key --from-file=path/to/public.crt
```

Then install the chart, specifying that you want to use the TLS secret:

```bash
helm install --set tls.enabled=true,tls.certSecret=tls-ssl-minio minio/minio
```

### Installing certificates from third party CAs

MinIO can connect to other servers, including MinIO nodes or other server types such as NATs and Redis. If these servers use certificates that were not registered with a known CA, add trust for these certificates to MinIO Server by bundling these certificates into a Kubernetes secret and providing it to Helm via the `trustedCertsSecret` value. If `.Values.tls.enabled` is `true` and you're installing certificates for third party CAs, remember to include Minio's own certificate with key `public.crt`, if it also needs to be trusted.

For instance, given that TLS is enabled and you need to add trust for Minio's own CA and for the CA of a Keycloak server, a Kubernetes secret can be created from the certificate files using `kubectl`:

```
kubectl -n minio create secret generic minio-trusted-certs --from-file=public.crt --from-file=keycloak.crt
```

If TLS is not enabled, you would need only the third party CA:

```
kubectl -n minio create secret generic minio-trusted-certs --from-file=keycloak.crt
```

The name of the generated secret can then be passed to Helm using a values file or the `--set` parameter:

```
trustedCertsSecret: "minio-trusted-certs"

or

--set trustedCertsSecret=minio-trusted-certs
```

Create buckets after install
---------------------------

Install the chart, specifying the buckets you want to create after install:

```bash
helm install --set buckets[0].name=bucket1,buckets[0].policy=none,buckets[0].purge=false minio/minio
```

Description of the configuration parameters used above -

- `buckets[].name` - name of the bucket to create, must be a string with length > 0
- `buckets[].policy` - can be one of none|download|upload|public
- `buckets[].purge` - purge if bucket exists already

Uninstalling the Chart
----------------------

Assuming your release is named as `my-release`, delete it using the command:

```bash
helm delete my-release
```

or

```bash
helm uninstall my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.
