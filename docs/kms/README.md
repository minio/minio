# KMS Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

KMS feature allows you to use Vault to generate and manages keys which are used by the minio server to encrypt objects.This document explains how to configure Minio with Vault as KMS.

## Get started

### 1. Prerequisites
Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

### 2. Configure Vault
Vault as Key Management System requires following to be configured in Vault

- transit backend configured with a named encryption key-ring
- AppRole based authentication with read/update policy for transit backend. In particular, read and update policy
  are required for the generate data key endpoint and decrypt key endpoint.

### 3. Environment variables

You'll need the Vault endpoint, AppRole ID, AppRole SecretID, encryption key-ring name before starting Minio server with Vault as KMS

```sh
export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
minio server ~/export
```

Optionally set `MINIO_SSE_VAULT_CAPATH` is the path to a directory of PEM-encoded CA cert files to verify the Vault server SSL certificate.
```
export MINIO_SSE_VAULT_CAPATH=/home/user/custom-pems
```

Gateway encryption options can be specified by setting MINIO_GW_SSE and MINI_GW_SSE_MODE.

To specify encryption type at gateway MINIO_GW_SSE environment variable needs to be set to "s3" for sse-s3
and "c" for sse-c encryption at gateway. More than one encryption option can be set, delimited by ";". If MINIO_GW_SSE is not set, any SSE headers will be passed to S3 backend.

In gateway mode, encryption can be set to pass-through to backend, do encryption at the gateway or double encryption at both gateway and backend by setting environment variable MINIO_GW_SSE_MODE.  More than one encryption mode can be set, delimited by ";". If MINIO_GW_SSE_MODE is not set,SSE options will not be honored.
Valid settings are "backend" to enable encryption at the backend, and "gateway" to enable encryption at the gateway. Both can be specified to turn on double
encryption.

```sh
export MINIO_GW_SSE="s3;c"
export MINIO_GW_SSE_MODE="backend;gateway"
export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
minio gateway s3
```
### 4. Test your setup

To test this setup, access the Minio server via browser or [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). You’ll see the uploaded files are accessible from the all the Minio endpoints.

# Explore Further

- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
