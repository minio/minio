# KMS Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio uses a key-management-system (KMS) to support SSE-S3. If a client requests SSE-S3, or auto-encryption
is enabled, the Minio server encrypts each object with an unique object key which is protected by a master key
managed by the KMS. Usually all object keys are protected by a single master key.

Minio supports two different KMS concepts:
 - External KMS:
   Minio can be configured to use an external KMS i.e. [Hashicorp Vault](https://www.vaultproject.io/).
   An external KMS decouples Minio as storage system from key-management. An external KMS can
   be managed by a dedicated security team and allows you to grant/deny access to (certain) objects
   by enabling or disabling the corresponding master keys on demand.

- Direct KMS master keys:
   Minio can also be configured to directly use a master key specified by the environment variable `MINIO_SSE_MASTER_KEY`.
   Direct master keys are useful if the storage backend is not on the same machine as the Minio server, e.g.,
   if network drives or Minio gateway is used and an external KMS would cause too much management overhead.  
   
   Note: KMS master keys are mainly for testing purposes. It's not recommended to use them for production deployments.
   Further if the Minio server machine is ever compromised, then the master key must also be treated as compromised.

**Important:**  
If multiple Minio servers are configured as [gateways](https://github.com/minio/minio/blob/master/docs/gateway/README.md)
pointing to the *same* backend - for example the same NAS storage - then the KMS configuration **must** be the same for
all gateways. Otherwise one gateway may not be able to decrypt objects created by another gateway. It is the operators' 
responsibility to ensure consistency.

## Get started

### 1. Prerequisites
Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

### 2. Setup a KMS

Either use Hashicorp Vault as external KMS or specify a master key directly depending on your use case.

#### 2.1 Setup Hashicorp Vault

Here is a sample quick start for configuring vault with a transit backend and Approle with correct policy 

Minio requires the following Vault setup:
- The [transit backend](https://www.vaultproject.io/api/secret/transit/index.html) configured with a named encryption key-ring
- [AppRole](https://www.vaultproject.io/docs/auth/approle.html) based authentication with read/update policy for transit backend. In particular, read and update policy are required for the [Generate Data Key](https://www.vaultproject.io/api/secret/transit/index.html#generate-data-key) endpoint and [Decrypt Data](https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data) endpoint.

**2.1.1 Start Vault server in dev mode**

In dev mode, Vault server runs in-memory and starts unsealed. Note that running Vault in dev mode is insecure and any data stored in the Vault is lost upon restart.

```
vault server -dev
```

**2.1.2 Set up vault transit backend and create an app role**

```
cat > vaultpolicy.hcl <<EOF
path "transit/datakey/plaintext/my-minio-key" { 
  capabilities = [ "read", "update"]
}
path "transit/decrypt/my-minio-key" { 
  capabilities = [ "read", "update"]
}
path "transit/encrypt/my-minio-key" { 
  capabilities = [ "read", "update"]
}

EOF

export VAULT_ADDR='http://127.0.0.1:8200'
vault auth enable approle    # enable approle style auth
vault secrets enable transit  # enable transit secrets engine
vault write -f  transit/keys/my-minio-key  #define a encryption key-ring for the transit path
vault policy write minio-policy ./vaultpolicy.hcl  #define a policy for AppRole to access transit path
vault write auth/approle/role/my-role token_num_uses=0  secret_id_num_uses=0  period=60s # period indicates it is renewable if token is renewed before the period is over
# define an AppRole
vault write auth/approle/role/my-role policies=minio-policy # apply policy to role
vault read auth/approle/role/my-role/role-id  # get Approle ID
vault write -f auth/approle/role/my-role/secret-id

```

The AppRole ID, AppRole Secret Id, Vault endpoint and Vault key name can now be used to start minio server with Vault as KMS.

**2.1.3 Vault Environment variables**

You'll need the Vault endpoint, AppRole ID, AppRole SecretID and encryption key-ring name defined in step 2.1.2

```sh
export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
export MINIO_SSE_VAULT_AUTH_TYPE=approle
minio server ~/export
```

Optionally, set `MINIO_SSE_VAULT_CAPATH` to a directory of PEM-encoded CA cert files to use mTLS for client-server authentication.

```
export MINIO_SSE_VAULT_CAPATH=/home/user/custom-certs
```

An additional option is to set `MINIO_SSE_VAULT_NAMESPACE` if AppRole and Transit Secrets engine have been scoped to Vault Namespace

```
export MINIO_SSE_VAULT_NAMESPACE=ns1
```

Note: If [Vault Namespaces](https://learn.hashicorp.com/vault/operations/namespaces) are in use, MINIO_SSE_VAULT_NAMESPACE variable needs to be set before setting approle and transit secrets engine.



#### 2.2 Specify a master key

A KMS master key consists of a master-key ID (CMK) and the 256 bit master key encoded as HEX value separated by a `:`.
A KMS master key can be specified directly using:

```sh
export MINIO_SSE_MASTER_KEY=my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574
```

Please use your own master key. A random master key can be generated using e.g. this command on Linux/Mac/BSD* systems:

```sh
head -c 32 /dev/urandom | xxd -c 32 -ps
```

### 3. Test your setup

To test this setup, start minio server with environment variables set in Step 3, and server is ready to handle SSE-S3 requests.

### Auto-Encryption

Minio can also enable auto-encryption **if** a valid KMS configuration is specified and the storage backend supports
encrypted objects. Auto-Encryption, if enabled, ensures that all uploaded objects are encrypted using the specified
KMS configuration.

Auto-Encryption is useful especially if the Minio operator wants to ensure that objects are **never** stored in 
plaintext - for example if sensitive data is stored on public cloud storage.

To enable auto-encryption set the environment variable to `on`:

```sh
export MINIO_SSE_AUTO_ENCRYPTION=on
```

To verify auto-encryption, use the `mc` command:

```sh
mc cp test.file myminio/crypt/
test.file:              5 B / 5 B  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100.00% 337 B/s 0s
mc stat myminio/crypt/test.file
Name      : test.file
...
Encrypted :
  X-Amz-Server-Side-Encryption: AES256
```

Note: Auto-Encryption only affects non-SSE-C requests since objects uploaded using SSE-C are already encrypted
and S3 only allows either SSE-S3 or SSE-C but not both for the same object.   


# Explore Further

- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
