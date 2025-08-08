# KMS Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO uses a key-management-system (KMS) to support SSE-S3. If a client requests SSE-S3, or auto-encryption is enabled, the MinIO server encrypts each object with a unique object key which is protected by a master key managed by the KMS.

## Quick Start

MinIO supports multiple KMS implementations via our [KES](https://github.com/minio/kes#kes) project. We run a KES instance at `https://play.min.io:7373` for you to experiment and quickly get started. To run MinIO with a KMS just fetch the root identity, set the following environment variables and then start your MinIO server. If you haven't installed MinIO, yet, then follow the MinIO [install instructions](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html) first.

### 1. Fetch the root identity

As the initial step, fetch the private key and certificate of the root identity:

```sh
curl -sSL --tlsv1.2 \
     -O 'https://raw.githubusercontent.com/minio/kes/master/root.key' \
     -O 'https://raw.githubusercontent.com/minio/kes/master/root.cert'
```

### 2. Set the MinIO-KES configuration

```sh
export MINIO_KMS_KES_ENDPOINT=https://play.min.io:7373
export MINIO_KMS_KES_KEY_FILE=root.key
export MINIO_KMS_KES_CERT_FILE=root.cert
export MINIO_KMS_KES_KEY_NAME=my-minio-key
```

### 3. Start the MinIO Server

```sh
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
minio server ~/export
```

> The KES instance at `https://play.min.io:7373` is meant to experiment and provides a way to get started quickly.
> Note that anyone can access or delete master keys at `https://play.min.io:7373`. You should run your own KES
> instance in production.

## Configuration Guides

A typical MinIO deployment that uses a KMS for SSE-S3 looks like this:

```
    ┌────────────┐
    │ ┌──────────┴─┬─────╮          ┌────────────┐
    └─┤ ┌──────────┴─┬───┴──────────┤ ┌──────────┴─┬─────────────────╮
      └─┤ ┌──────────┴─┬─────┬──────┴─┤ KES Server ├─────────────────┤
        └─┤   MinIO    ├─────╯        └────────────┘            ┌────┴────┐
          └────────────┘                                        │   KMS   │
                                                                └─────────┘
```

In a given setup, there are `n` MinIO instances talking to `m` KES servers but only `1` central KMS. The most simple setup consists of `1` MinIO server or cluster talking to `1` KMS via `1` KES server.

The main difference between various MinIO-KMS deployments is the KMS implementation. The following table helps you select the right option for your use case:

| KMS                                                                                          | Purpose                                                           |
|:---------------------------------------------------------------------------------------------|:------------------------------------------------------------------|
| [Hashicorp Vault](https://github.com/minio/kes/wiki/Hashicorp-Vault-Keystore)                | Local KMS. MinIO and KMS on-prem (**Recommended**)                |
| [AWS-KMS + SecretsManager](https://github.com/minio/kes/wiki/AWS-SecretsManager)             | Cloud KMS. MinIO in combination with a managed KMS installation   |
| [Gemalto KeySecure /Thales CipherTrust](https://github.com/minio/kes/wiki/Gemalto-KeySecure) | Local KMS. MinIO and KMS On-Premises.                             |
| [Google Cloud Platform SecretManager](https://github.com/minio/kes/wiki/GCP-SecretManager)   | Cloud KMS. MinIO in combination with a managed KMS installation   |
| [FS](https://github.com/minio/kes/wiki/Filesystem-Keystore)                                  | Local testing or development (**Not recommended for production**) |

The MinIO-KES configuration is always the same - regardless of the underlying KMS implementation. Checkout the MinIO-KES [configuration example](https://github.com/minio/kes/wiki/MinIO-Object-Storage).

### Further references

- [Run MinIO with TLS / HTTPS](https://docs.min.io/community/minio-object-store/operations/network-encryption.html)
- [Tweak the KES server configuration](https://github.com/minio/kes/wiki/Configuration)
- [Run a load balancer in front of KES](https://github.com/minio/kes/wiki/TLS-Proxy)
- [Understand the KES server concepts](https://github.com/minio/kes/wiki/Concepts)

## Auto Encryption

Auto-Encryption is useful when MinIO administrator wants to ensure that all data stored on MinIO is encrypted at rest.

### Using `mc encrypt` (recommended)

MinIO automatically encrypts all objects on buckets if KMS is successfully configured and bucket encryption configuration is enabled for each bucket as shown below:

```
mc encrypt set sse-s3 myminio/bucket/
```

Verify if MinIO has `sse-s3` enabled

```
mc encrypt info myminio/bucket/
Auto encryption 'sse-s3' is enabled
```

### Using environment (not-recommended)

MinIO automatically encrypts all objects on buckets if KMS is successfully configured and following ENV is enabled:

```
export MINIO_KMS_AUTO_ENCRYPTION=on
```

### Verify auto-encryption

> Note that auto-encryption only affects requests without S3 encryption headers. So, if a S3 client sends
> e.g. SSE-C headers, MinIO will encrypt the object with the key sent by the client and won't reach out to
> the configured KMS.

To verify auto-encryption, use the following `mc` command:

```
mc cp test.file myminio/bucket/
test.file:              5 B / 5 B  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100.00% 337 B/s 0s
```

```
mc stat myminio/bucket/test.file
Name      : test.file
...
Encrypted :
  X-Amz-Server-Side-Encryption: AES256
```

## Encrypted Private Key

MinIO supports encrypted KES client private keys. Therefore, you can use
an password-protected private keys for `MINIO_KMS_KES_KEY_FILE`.

When using password-protected private keys for accessing KES you need to
provide the password via:

```
export MINIO_KMS_KES_KEY_PASSWORD=<your-password>
```

Note that MinIO only supports encrypted private keys - not encrypted certificates.
Certificates are no secrets and sent in plaintext as part of the TLS handshake.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/community/minio-object-store/reference/minio-mc.html)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/community/minio-object-store/integrations/aws-cli-with-minio.html)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/community/minio-object-store/developers/go/minio-go.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
