# minio-gateway

minio-gateway fork of [MinIO](https://github.com/minio/minio) with the s3 gateway feature. 

It follows the MinIO teams recommandation to fork the project and maintain it as a fork if the gateway feature is needed.


## Why is it still needed ?

1. Because you not always want your data provider to have access to your keys.

2. Many solution using s3 do not support sse-c. With the s3-gateway you get that without extra configuration, e.g. Gitlab does not support sse-c.

3. Because you don't want to pay S3 [data transfer charges](https://aws.amazon.com/s3/pricing/) to get your data back.


## About encryption

MinIO recommend using a KMS based solution for encryption, but it is not always possible or might be overkill to use KES.

The s3-gateway explicitly support full encryption both the MINIO_KMS_SECRET_KEY and MINIO_KMS_AUTO_ENCRYPTION environment variables.

Example:

```bash
MINIO_KMS_SECRET_KEY="my-minio-key:$(cat /dev/urandom | head -c 32 | base64 -)"
MINIO_KMS_AUTO_ENCRYPTION=on

```

We do not recommend using it, but it is there for the ones that need it as a simple solution to encrypt their data.

## Why the AGPL license?

Because it is a MinIO fork and want to keep last security patches applicable without license breakage.


## Is it still feature freeze?

Except for the s3 gateway (the lasting NAS one will be soon removed) it mostly still is. 

We might add some at some point some features, like a Kubernetes IAM backend so that we don't have to deploy a proper etcd cluster to support distributed IAM, but other than that, it will then be open to issues/contributions.

<!-- ## How to use it? -->

## Getting started

... TODO ... (as soon as we have a proper release)
