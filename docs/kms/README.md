# KMS Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO uses a key-management-system (KMS) to support SSE-S3. If a client requests SSE-S3, or auto-encryption
is enabled, the MinIO server encrypts each object with an unique object key which is protected by a master key
managed by the KMS.

## Quick Start

MinIO supports multiple KMS implementations via our [KES](https://github.com/minio/kes#kes) project. We run
a KES instance at `https://play.min.io:7373` for you to experiment and quickly get started. To run MinIO with
a KMS just fetch the root identity, set the following environment variables and then start your MinIO server.
If you havn't installed MinIO, yet, then follow the MinIO [install instructions](https://docs.min.io/docs/minio-quickstart-guide)
first.

#### 1. Fetch the root identity
As the initial step, fetch the private key and certificate of the root identity:

```sh
curl -sSL --tlsv1.2 \
     -O 'https://raw.githubusercontent.com/minio/kes/master/root.key' \
     -O 'https://raw.githubusercontent.com/minio/kes/master/root.cert'
```

#### 2. Set the MinIO-KES configuration

```sh
export MINIO_KMS_KES_ENDPOINT=https://play.min.io:7373
export MINIO_KMS_KES_KEY_FILE=root.key
export MINIO_KMS_KES_CERT_FILE=root.cert
export MINIO_KMS_KES_KEY_NAME=my-minio-key
```

#### 3. Start the MinIO Server

```sh
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
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

In a given setup, there are `n` MinIO instances talking to `m` KES servers but only `1` central KMS. The most simple
setup consists of `1` MinIO server or cluster talking to `1` KMS via `1` KES server.

The main difference between various MinIO-KMS deployments is the KMS implementation. The following table
helps you select the right option for your use case:

|                                        KMS                                       | Purpose                                                           |
|:---------------------------------------------------------------------------------|:------------------------------------------------------------------|
| [Hashicorp Vault](https://github.com/minio/kes/wiki/Hashicorp-Vault-Keystore)    | Local KMS. MinIO and KMS on-prem (**Recommended**)                |
| [AWS-KMS + SecretsManager](https://github.com/minio/kes/wiki/AWS-SecretsManager) | Cloud KMS. MinIO in combination with a managed KMS installation   |  
| [FS](https://github.com/minio/kes/wiki/Filesystem-Keystore)                      | Local testing or development (**Not recommended for production**) |
 
The MinIO-KES configuration is always the same - regardless of the underlying KMS implementation.
Checkout the MinIO-KES [configuration example](https://github.com/minio/kes/wiki/MinIO-Object-Storage).

### Further references

 - [Run MinIO with TLS / HTTPS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls.html)
 - [Tweak the KES server configuration](https://github.com/minio/kes/wiki/Configuration)
 - [Run a load balancer infront of KES](https://github.com/minio/kes/wiki/TLS-Proxy)
 - [Understand the KES server concepts](https://github.com/minio/kes/wiki/Concepts) 

## Auto Encryption

Optionally, you can instruct the MinIO server to automatically encrypt all objects with keys from the KES
server - even if the client does not specify any encryption headers during the S3 PUT operation.

Auto-Encryption is especially useful when the MinIO operator wants to ensure that all data stored on MinIO
gets encrypted before it's written to the storage backend.

To enable auto-encryption set the environment variable to `on`:
```
export MINIO_KMS_AUTO_ENCRYPTION=on
```

> Note that auto-encryption only affects requests without S3 encryption headers. So, if a S3 client sends
> e.g. SSE-C headers, MinIO will encrypt the object with the key sent by the client and won't reach out to
> the KMS.

To verify auto-encryption, use the `mc` command:

```
mc cp test.file myminio/bucket/
test.file:              5 B / 5 B  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100.00% 337 B/s 0s

mc stat myminio/bucket/test.file
Name      : test.file
...
Encrypted :
  X-Amz-Server-Side-Encryption: AES256
```

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
