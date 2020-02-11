# KMS Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO uses a key-management-system (KMS) to support SSE-S3. If a client requests SSE-S3, or auto-encryption
is enabled, the MinIO server encrypts each object with an unique object key which is protected by a master key
managed by the KMS. Usually all object keys are protected by a single master key.

MinIO supports two different KMS concepts:
 - External KMS:
   MinIO can be configured to use an external KMS i.e. [Hashicorp Vault](https://www.vaultproject.io/).
   An external KMS decouples MinIO as storage system from key-management. An external KMS can
   be managed by a dedicated security team and allows you to grant/deny access to (certain) objects
   by enabling or disabling the corresponding master keys on demand.

- Direct KMS master keys:
   MinIO can also be configured to directly use a master key specified by the environment variable `MINIO_KMS_MASTER_KEY` or with a docker secret key.
   Direct master keys are useful if the storage backend is not on the same machine as the MinIO server, e.g.,
   if network drives or MinIO gateway is used and an external KMS would cause too much management overhead.

   Note: KMS master keys are mainly for testing purposes. It's not recommended to use them for production deployments.
   Further if the MinIO server machine is ever compromised, then the master key must also be treated as compromised.

**Important:**
If multiple MinIO servers are configured as [gateways](https://github.com/minio/minio/blob/master/docs/gateway/README.md) pointing to the *same* backend - for example the same NAS storage - then the KMS configuration **must** be the same for all gateways. Otherwise one gateway may not be able to decrypt objects created by another gateway. It is the operator responsibility to ensure consistency.

## Get started

### 1. Prerequisites
Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Setup a KMS

Either use Hashicorp Vault as external KMS or specify a master key directly depending on your use case.

#### 2.1 Setup Hashicorp Vault

Here is a sample quick start for configuring vault with a transit backend and Approle with correct policy

MinIO requires the following Vault setup:
- The [transit backend](https://www.vaultproject.io/api/secret/transit/index.html) configured with a named encryption key-ring
- [AppRole](https://www.vaultproject.io/docs/auth/approle.html) based authentication with read/update policy for transit backend. In particular, read and update policy are required for the [Generate Data Key](https://www.vaultproject.io/api/secret/transit/index.html#generate-data-key) endpoint and [Decrypt Data](https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data) endpoint.

**2.1.1 Start Vault server**

Vault requires access to `mlock` syscall by default. Use `setcap` to give the Vault executable the ability to use the `mlock` syscall without running the process as root.  To do so run:
```
sudo setcap cap_ipc_lock=+ep $(readlink -f $(which vault))
```

Create `vault-config.json` to use file backend and start the server.
```
cat > vault-config.json <<EOF
{
  "api_addr": "http://127.0.0.1:8200",
  "backend": {
    "file": {
      "path": "vault/file"
    }
  },
  "default_lease_ttl": "168h",
  "max_lease_ttl": "720h",
  "listener": {
    "tcp": {
      "address": "0.0.0.0:8200",
      "tls_disable": true
    }
  },
  "ui": true
}
EOF

vault server -config vault-config.json
```

> NOTE: In this example we use `"tls_disable": true` for demonstration purposes only,
> in production setup you should generate proper TLS certificates by specifying
> - [`tls_cert_file`](https://www.vaultproject.io/docs/configuration/listener/tcp.html#tls_cert_file)
> - [`tls_key_file`](https://www.vaultproject.io/docs/configuration/listener/tcp.html#tls_key_file)


**2.1.2 Initialize vault and unseal it**

```
export VAULT_ADDR='http://127.0.0.1:8200'
vault operator init
Unseal Key 1: eyW/+8ZtsgT81Cb0e8OVxzJAQP5lY7Dcamnze+JnWEDT
Unseal Key 2: 0tZn+7QQCxphpHwTm6/dC3LpP5JGIbYl6PK8Sy79R+P2
Unseal Key 3: cmhs+AUMXUuB6Lzsvgcbp3bRT6VDGQjgCBwB2xm0ANeF
Unseal Key 4: /fTPpec5fWpGqWHK+uhnnTNMQyAbl5alUi4iq2yNgyqj
Unseal Key 5: UPdDVPto+H6ko+20NKmagK40MOskqOBw4y/S51WpgVy/

Initial Root Token: s.zaU4Gbcu0Wh46uj2V3VuUde0

Vault is initialized with 5 key shares and a key threshold of 3. Please securely
distribute the key shares printed above. When the Vault is re-sealed,
restarted, or stopped, you must supply at least 3 of these keys to unseal it
before it can start servicing requests.

Vault does not store the generated master key. Without at least 3 key to
reconstruct the master key, Vault will remain permanently sealed!

It is possible to generate new unseal keys, provided you have a quorum of
existing unseal keys shares. See "vault operator rekey" for more information.
```

Use any of the previously generated keys to unseal the vault
```
vault operator unseal eyW/+8ZtsgT81Cb0e8OVxzJAQP5lY7Dcamnze+JnWEDT
vault operator unseal 0tZn+7QQCxphpHwTm6/dC3LpP5JGIbYl6PK8Sy79R+P2
vault operator unseal cmhs+AUMXUuB6Lzsvgcbp3bRT6VDGQjgCBwB2xm0ANeF
Key             Value
---             -----
Seal Type       shamir
Initialized     true
Sealed          false  ---> NOTE: vault is unsealed
Total Shares    5
Threshold       3
Version         1.1.3
Cluster Name    vault-cluster-3f084948
Cluster ID      8c92e999-7062-4da6-4434-0fc05f34824d
HA Enabled      false
```

Obtain root token from the `vault operator init` output above. It is displayed as `Initial Root Token: s.zaU4Gbcu0Wh46uj2V3VuUde0`

**2.1.3 Set up vault transit backend and create an app role**
```
export VAULT_TOKEN=s.zaU4Gbcu0Wh46uj2V3VuUde0

vault auth enable approle    # enable approle style auth
vault secrets enable transit  # enable transit secrets engine
# define an encryption key-ring for the transit path
vault write -f  transit/keys/my-minio-key

cat > vaultpolicy.hcl <<EOF
path "transit/datakey/plaintext/my-minio-key" {
  capabilities = [ "read", "update"]
}
path "transit/decrypt/my-minio-key" {
  capabilities = [ "read", "update"]
}
path "transit/rewrap/my-minio-key" {
  capabilities = ["update"]
}

EOF

# define a policy for AppRole to access transit path
vault policy write minio-policy ./vaultpolicy.hcl

# period indicates it is renewable if token is renewed before the period is over
vault write auth/approle/role/my-role token_num_uses=0  secret_id_num_uses=0  period=5m

# define an AppRole
vault write auth/approle/role/my-role policies=minio-policy # apply policy to role
vault read auth/approle/role/my-role/role-id  # get Approle ID
Key        Value
---        -----
role_id    8c03926c-6c51-7a1d-cf7d-62e48ab8d6d7

vault write -f auth/approle/role/my-role/secret-id
Key                   Value
---                   -----
secret_id             edd8738c-6efe-c226-74f9-ef5b66e119d7
secret_id_accessor    57d1db64-6350-c321-4a3e-fc6aeb7d00b6
```

The AppRole ID, AppRole Secret Id, Vault endpoint and Vault key name can now be used to start minio server with Vault as KMS.

**2.1.3 Vault Environment variables**

You'll need the Vault endpoint, AppRole ID, AppRole SecretID and encryption key-ring name defined in step 2.1.2

```
export MINIO_KMS_VAULT_APPROLE_ID=8c03926c-6c51-7a1d-cf7d-62e48ab8d6d7
export MINIO_KMS_VAULT_APPROLE_SECRET=edd8738c-6efe-c226-74f9-ef5b66e119d7
export MINIO_KMS_VAULT_ENDPOINT=http://vault-endpoint-ip:8200
export MINIO_KMS_VAULT_KEY_NAME=my-minio-key
export MINIO_KMS_VAULT_AUTH_TYPE=approle
minio server ~/export
```

Optionally, set `MINIO_KMS_VAULT_CAPATH` to a directory of PEM-encoded CA cert files to use mTLS for client-server authentication.

```
export MINIO_KMS_VAULT_CAPATH=/home/user/custom-certs
```

An additional option is to set `MINIO_KMS_VAULT_NAMESPACE` if AppRole and Transit Secrets engine have been scoped to Vault Namespace

```
export MINIO_KMS_VAULT_NAMESPACE=ns1
```

Note: If [Vault Namespaces](https://learn.hashicorp.com/vault/operations/namespaces) are in use, MINIO_KMS_VAULT_VAULT_NAMESPACE variable needs to be set before setting approle and transit secrets engine.

#### 2.2 Specify a master key

**2.2.1 KMS master key from environment variables**

A KMS master key consists of a master-key ID (CMK) and the 256 bit master key encoded as HEX value separated by a `:`.
A KMS master key can be specified directly using:

```
export MINIO_KMS_MASTER_KEY=my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574
```

Please use your own master key. A random master key can be generated using e.g. this command on Linux/Mac/BSD* systems:

```
head -c 32 /dev/urandom | xxd -c 32 -ps
```

**2.2.2 KMS master key from docker secret**

Alternatively, you may pass a master key as a [Docker secret](https://docs.docker.com/engine/swarm/secrets/).

```bash
echo "my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574" | docker secret create kms_master_key
```

Obviously, do not use this demo key for anything real!

To use another secret name, follow the instructions above and replace kms_master_key with your custom names (e.g. my_kms_master_key). Then, set the MINIO_KMS_MASTER_KEY_FILE environment variable to your secret name:

```bash
export MINIO_KMS_MASTER_KEY_FILE=my_kms_master_key
```

### 3. Test your setup
To test this setup, start minio server with environment variables set in Step 3, and server is ready to handle SSE-S3 requests.

### Auto-Encryption

MinIO can also enable auto-encryption **if** a valid KMS configuration is specified and the storage backend supports
encrypted objects. Auto-Encryption, if enabled, ensures that all uploaded objects are encrypted using the specified
KMS configuration.

Auto-Encryption is useful especially if the MinIO operator wants to ensure that objects are **never** stored in
plaintext - for example if sensitive data is stored on public cloud storage.

To enable auto-encryption set the environment variable to `on`:

```
export MINIO_KMS_AUTO_ENCRYPTION=on
```

To verify auto-encryption, use the `mc` command:

```
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

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)

