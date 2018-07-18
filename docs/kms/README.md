# KMS Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

KMS feature allows you to use Vault to generate and manages keys which are used by the minio server to encrypt objects.This document explains how to configure Minio with Vault as KMS.

## Get started

### 1. Prerequisites
Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

### 2. Configure Vault
Vault as Key Management System requires following to be configured in Vault

- transit backend configured with a named encryption key-ring
- AppRole based authentication with read/update policy for transit backend

### Environment variables

You'll need the Vault endpoint, AppRole ID, AppRole SecretID, encryption key-ring name before starting Minio server with Vault as KMS

```sh
export MINIO_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_VAULT_KEY_NAME=my-minio-key
minio server ~/export
```

### 4. Test your setup

To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the all the Minio endpoints.

# Explore Further

- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
