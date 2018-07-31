# How to secure access to Minio on Kubernetes with TLS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This document explains how to configure Minio server with TLS certificates on Kubernetes.

## 1. Prerequisites

- Familiarity with [Minio deployment process on Kubernetes](https://docs.minio.io/docs/deploy-minio-on-kubernetes).

- Kubernetes cluster with `kubectl` configured.

- Acquire TLS certificates, either from a CA or [create self-signed certificates](https://docs.minio.io/docs/how-to-secure-access-to-minio-server-with-tls).

For a [distributed Minio setup](https://docs.minio.io/docs/distributed-minio-quickstart-guide), where there are multiple pods with different domain names expected to run, you will either need wildcard certificates valid for all the domains or have specific certificates for each domain. If you are going to use specific certificates, make sure to create Kubernetes secrets accordingly.

For testing purposes, here is [how to create self-signed certificates](https://github.com/minio/minio/tree/master/docs/tls#3-generate-self-signed-certificates).

## 2. Create Kubernetes secret

[Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret) are intended to hold sensitive information. 
We'll use secrets to hold the TLS certificate and key. To create a secret, update the paths to `private.key` and `public.crt` 
below.

Then type

```sh
kubectl create secret generic tls-ssl-minio --from-file=path/to/private.key --from-file=path/to/public.crt
```

Cross check if the secret is created successfully using 

```sh
kubectl get secrets
```

You should see a secret named `tls-ssl-minio`.

## 3. Update deployment yaml file

Whether you are planning to use Kubernetes StatefulSet or Kubernetes Deployment, the steps remain the same.

If you're using certificates provided by a CA, add the below section in your yaml file under `spec.volumes[]`

```yaml
    volumes:
      - name: secret-volume
        secret:
          secretName: tls-ssl-minio
          items:
          - key: public.crt
            path: public.crt
          - key: private.key
            path: private.key
          - key: public.crt
            path: CAs/public.crt
```

Note that the `secretName` should be same as the secret name created in previous step. Then add the below section under
`spec.containers[].volumeMounts[]`

```yaml
    volumeMounts:
        - name: secret-volume
          mountPath: /<user-running-minio>/.minio/certs
```

Here the name of `volumeMount` should match the name of `volume` created previously. Also `mountPath` must be set to the path of
the Minio server's config sub-directory that is used to store certificates. By default, the location is
`/<user-running-minio>/.minio/certs`.

*Tip*: In a standard Kubernetes configuration, this will be `/root/.minio/certs`. Kubernetes will mount the secrets volume read-only, 
so avoid setting `mountPath` to a path that Minio server expects to write to.
