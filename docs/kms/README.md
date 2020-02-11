# KMS Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO uses a key-management-system (KMS) to support SSE-S3. If a client requests SSE-S3, or auto-encryption
is enabled, the MinIO server encrypts each object with an unique object key which is protected by a master key
managed by the KMS.

> MinIO still provides native Hashicorp Vault support. However, this is feature is **deprecated** and may be
> removed in the future. Therefore, we strongly recommend to use the architecture and KMS Guide below.
> If you have to maintain a legacy MinIO-Vault deployment you can find the legacy documentation [here](https://docs.min.io/docs/minio-vault-legacy.html).

## Architecture and Concepts

The KMS decouples MinIO as an application-facing storage system from the secure key storage and
may be managed by a dedicated security team. In general, the MinIO-KMS infrastructure looks like this:
```
 +-------+       +-----+
 | MinIO +-------+ KMS |
 +-------+       +-----+
```

If you scale your storage infrastructure to multiple MinIO clusters your architecture should look like this:
```
 +-------+                       +-------+
 | MinIO +----+             +----+ MinIO |
 +-------+    |   +-----+   |    +-------+
              +---+ KMS +---+
 +-------+    |   +-----+   |    +-------+
 | MinIO +----+             +----+ MinIO |
 +-------+                       +-------+
``` 

MinIO supports commonly-used KMS implementations, like [AWS-KMS](https://aws.amazon.com/kms/) or
[Hashicorp Vault](https://www.vaultproject.io/) via our [KES project](https://github.com/minio/kes/wiki).
KES makes it possible to scale your KMS horizontally with your storage infrastructure (MinIO clusters).
Therefore, it wraps around the KMS implementation like this:
```
       +-------+                 +-------+
       | MinIO |                 | MinIO |
       +---+---+                 +---+---+
           |                         |
      +----+-------------------------+----+---- KMS
      |    |                         |    |
      | +--+--+                   +--+--+ |
      | | KES +--+             +--+ KES | |
      | +-----+  |  +-------+  |  +-----+ |
      |          +--+ Vault +--+          |
      | +-----+  |  +-------+  |  +-----+ |
      | | KES +--+             +--+ KES | |
      | +--+--+                   +--+--+ |
      |    |                         |    |
      +----+-------------------------+----+---- KMS
           |                         |
       +---+---+                 +---+---+
       | MinIO |                 | MinIO |
       +-------+                 +-------+
```
Observe that all MinIO clusters only have a connection to "their own" KES instance and no direct access to Vault (as one possible KMS implementation).
Each KES instance will handle all encrypton/decryption requests made by "its" MinIO cluster such that the central KMS implementation does not have to handle
a lot of traffic. Instead, each KES instance will use the central KMS implementation as secure key store and fetches the required master keys from it.

## Get Started Guide

In the subsequent sections this guide shows how to setup a MinIO-KMS deployment with Hashicorp Vault as KMS implementation.
Therefore, it shows how to setup and configure:
 - A Vault server as central key store.
 - A KES server instance as middleware between MinIO and Vault.
 - The MinIO instance itself.

> Please note that this guide uses self-signed certificates for simplicity. In a production deployment you should use
> X.509 certificates issued by a "public" (e.g. Let's Encrypt) or your organization-internal CA. 

This guide shows how to set up three different servers on the same machine:
 - The Vault server as `https://127.0.0.1:8200`
 - The KES server as `https://127.0.0.1:7373`
 - The MinIO server as `https://127.0.0.1:9000`

### 1 Prerequisites

Install MinIO, KES and Vault. For MinIO take a look at the [MinIO quickstart guide](https://docs.min.io/docs/minio-quickstart-guide).
Then download the [latest KES binary](https://github.com/minio/kes/releases/latest/) and the [latest Vault binary](https://github.com/hashicorp/vault/releases/latest/)
for your OS and platform.

### 2 Generate TLS certificates

Since KES sends object encryption keys to MinIO and Vault sends master keys (used to encrypt the object encryption keys) to KES we absolutely need
TLS connections between MinIO, KES and Vault. Therefore, we need to generate at least two TLS certificates.

#### 2.1 Generate a TLS certificate for Vault

To generate a new private key for Vault's certificate run the following openssl command:
```
openssl ecparam -genkey -name prime256v1 | openssl ec -out vault-tls.key
```

Then generate a new TLS certificate for the private/public key pair via:
```
openssl req -new -x509 -days 365 -key vault-tls.key -out vault-tls.crt -subj "/C=US/ST=state/L=location/O=organization/CN=localhost"
```
> You may want to adjust the X.509 subject (`-subj` parameter). Note that this is a self-signed certificate. For production deployments
> this certificate should be issued by a CA. 

#### 2.2 Generate a TLS certificate for KES

To generate a new private key for KES's certificate run the following openssl command:
```
openssl ecparam -genkey -name prime256v1 | openssl ec -out kes-tls.key
```

Then generate a new TLS certificate for the private/public key pair via:
```
openssl req -new -x509 -days 365 -key kes-tls.key -out kes-tls.crt -subj "/C=US/ST=state/L=location/O=organization/CN=localhost"
```
> You may want to adjust the X.509 subject (`-subj` parameter). Note that this is a self-signed certificate. For production deployments
> this certificate should be issued by a CA. 

#### 2.3 Generate a TLS certificate for MinIO (Optional)

This step is optional. However, we recommend to up/download your S3 objects via TLS - especially when they should be encrypted at the
storage backend with a KMS.

Checkout the [MinIO TLS guide](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls.html) for configuring MinIO and TLS.

### 3 Set up Vault

On unix-like systems, Vault uses the `mlock` syscall to prevent the OS from writing in-memory data
to disk (swapping). Therefore, you should give the Vault executable the ability to use the `mlock`
syscall without running the process as root. To do so run:
```
sudo setcap cap_ipc_lock=+ep $(readlink -f $(which vault))
```

Then create the Vault config file:
```
cat > vault-config.json <<EOF
{
  "api_addr": "https://127.0.0.1:8200",
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
      "tls_cert_file": "vault-tls.crt",
      "tls_key_file": "vault-tls.key",
      "tls_min_version": "tls12"
    }
  }
}
EOF
```
> Note that we run Vault with a file backend. For high-availability you may want to use a different
> backend - like [etcd](https://www.vaultproject.io/docs/configuration/storage/etcd/) or [consul](https://learn.hashicorp.com/vault/operations/ops-vault-ha-consul).

Finally, start the Vault server via:
```
vault server -config vault-config.json
``` 

#### 3.1 Initialize and unseal Vault

In a separate terminal window set the `VAULT_ADDR` env. variable to your Vault server:
```
export VAULT_ADDR='https://127.0.0.1:8200'
```

Further, you may want to run `export VAULT_SKIP_VERIFY=true` if Vault uses a self-signed TLS
certificate. When Vault serves a TLS certificate that has been issued by a CA that is trusted
by your machine - e.g. Let's Encrypt - then you don't need to run this command.

Then initialize Vault via:
```
vault operator init
``` 

Vault will print `n` (5 by default) unseal key shares of which at least `m` (3 by default)
are required to regenerate the actual unseal key to unseal Vault. Therefore, make sure to
remember them. In particular, keep those unseal key shares at a secure and durable location.

You should see some output similar to:
```
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

Now, set the env. variable `VAULT_TOKEN` to the root token printed by the command before:
```
export VAULT_TOKEN=s.zaU4Gbcu0Wh46uj2V3VuUde0
```

Then use any of the previously generated key shares to unseal Vault.
```
vault operator unseal eyW/+8ZtsgT81Cb0e8OVxzJAQP5lY7Dcamnze+JnWEDT
vault operator unseal 0tZn+7QQCxphpHwTm6/dC3LpP5JGIbYl6PK8Sy79R+P2
vault operator unseal cmhs+AUMXUuB6Lzsvgcbp3bRT6VDGQjgCBwB2xm0ANeF
```

Once you have submitted enough valid key shares Vault will become unsealed
and able to process requests.

#### 3.2 Enable Vault's K/V backend

The cryptographic master keys (but not the object encryption keys) will be stored
at Vault. Therefore, we need to enable Vault's K/V backend. To do so run:
```
vault secrets enable kv
```

#### 3.3 Enable AppRole authentication

Since we want connect one/multiple KES server to Vault later, we have to enable
AppRole authentication. To do so run:
```
vault auth enable approle
```

#### 3.4 Create an access policy for the K/V engine

The following policy determines how an application (i.e. KES server) can interact
with Vault. 
```
cat > minio-kes-policy.hcl <<EOF
path "kv/minio/*" {
  capabilities = [ "create", "read", "delete" ]
}

EOF
```
> Observe the path-prefix `minio` in `kv/minio/*`. This prefix ensures that the
> KES server can only read from and write to entries under `minio/*` - but not under
> `some-app/*`. How to separate domains on the K/V engine depends on your infrastructure
> and security requirements.

Then we upload the policy to Vault:
```
vault policy write minio-key-policy ./minio-kes-policy.hcl
```

#### 3.5 Create an new AppRole ID and bind it to a policy

Now, we need to create a new AppRole ID and grant that ID specific permissions.
The application (i.e. KES server) will authenticate to Vault via the AppRole role ID
and secret ID and is only allowed to perform operations granted by the specific policy.

So, we first create a new role for our KES server:
```
vault write auth/approle/role/kes-role token_num_uses=0  secret_id_num_uses=0  period=5m
```

Then we bind a policy to the role:
```
vault write auth/approle/role/kes-role policies=minio-key-policy
```

Finally, we request an AppRole role ID and secret ID from Vault.  
First, the role ID:
```
vault read auth/approle/role/kes-role/role-id 
```

Then the secret ID:
```
vault write -f auth/approle/role/kes-role/secret-id
```
> We are only interested in the `secret_id` - not in the `secret_id_accessor`.

### 4 Set up KES

Similar to Vault, KES uses the `mlock` syscall on linux systems to prevent the OS from writing in-memory
data to disk (swapping). Therefore, you should give the KES executable the ability to use the `mlock`
syscall without running the process as root. To do so run:
```
sudo setcap cap_ipc_lock=+ep $(readlink -f $(which kes))
```

#### 4.1 Create an identity for MinIO

Each user or application must present a valid X.509 certificate when connecting to the KES server (mTLS).
The KES server will accept/reject the connection attempt and applies policies based on the certificate.

Therefore, each MinIO cluster needs a X.509 TLS certificate for client authentication. You can create a
(self-signed) certificate by running:
```
kes tool identity new MinIO --key=minio.key --cert=minio.cert --time=8760h
```
> Note that *MinIO* is the [subject name](https://en.wikipedia.org/wiki/X.509#Structure_of_a_certificate).
> You may choose a more appropriate name for your deployment scenario. Also, for production deployments we
> recommend to get a TLS certificate for client authentication that has been issued by a CA.

To get the identity of a X.509 certificate run:
```
kes tool identity of minio.cert
``` 
> This command works with any (valid) X.509 certificate - regardless how it has been created - and 
> produces an output similar to:
<blockquote>
<p><code>Identity:  dd46485bedc9ad2909d2e8f9017216eec4413bc5c64b236d992f7ec19c843c5f</code></p>
</blockquote>

#### 4.2 Create the KES config file

Now, we can create the KES config file and start the KES server.
```
cat > kes-config.toml <<EOF
# The address:port of the kes server - i.e. on the local machine.
address = "127.0.0.1:7373"

[tls]
key  = "./kes-tls.key"
cert = "./kes-tls.crt" 

[policy.minio]
paths = [
          "/v1/key/create/minio-*",
          "/v1/key/generate/minio-*",
          "/v1/key/decrypt/minio-*"
        ]
identities = [ "dd46485bedc9ad2909d2e8f9017216eec4413bc5c64b236d992f7ec19c843c5f" ]

[cache.expiry]
all    = "5m" 
unused = "20s" 

[keystore.vault]
address   = "https://127.0.0.1:8200"  # The Vault endpoint - i.e. https://127.0.0.1:8200
name      = "minio"                   # The domain resp. prefix at Vault's K/V backend 

[keystore.vault.approle]
id     = ""    # Your AppRole Role ID 
secret = ""    # Your AppRole Secret ID
retry  = "15s" # Duration until the server tries to re-authenticate after connection loss.

[keystore.vault.tls]
ca = "./vault-tls.crt" # Since we use self-signed certificates

[keystore.vault.status]
ping = "10s"

EOF
```
> Please change the value of `identities` under `policy.minio` to the identity of your `minio.cert`.
> Also, insert the AppRole role ID and secret ID that you have created previously during the Vault setup.
> You can find a documented config file with all available parameters [here](https://github.com/minio/kes/blob/master/server-config.toml).

Finally, start the KES server via:
```
kes server --config=kes-config.toml --mlock --root=disabled --mtls-auth=ignore
```
> Note that we effectively disable the special *root* identity since we don't need it.
> For more information about KES identities checkout: [KES Identities](https://github.com/minio/kes/wiki#identities)
> Further, note that we pass `--mtls-auth=ignore` since the client X.509 certificate
> is a self-signed certificate.

#### 4.3 Create a new master key

Before we can proceed with the MinIO setup we need to create a new master key. Therefore we use the
MinIO identity and the KES CLI.

In a new terminal window become the MinIO identity via:
```
export KES_CLIENT_TLS_KEY_FILE=minio.key
export KES_CLIENT_TLS_CERT_FILE=minio.cert
```

Then create the master key by running:
```
kes key create minio-key-1 -k
```
> The `-k` flag is only required since we use self-signed certificates.
> Also, observe that based on the server config file the MinIO identity
> is only allowed to create/use master keys that start with `minio-`.
> So, trying to create a key e.g. `kes key create my-key-1 -k` will
> fail with a *prohibited by policy* error.

### 5 Set up the MinIO server

The MinIO server will need to know the KES server endpoint, its mTLS client certificate
for authentication and authorization and the default master key name. 

```
export MINIO_KMS_KES_ENDPOINT=https://localhost:7373
export MINIO_KMS_KES_KEY_FILE=minio.key
export MINIO_KMS_KES_CERT_FILE=minio.cert
export MINIO_KMS_KES_KEY_NAME=minio-key-1
export MINIO_KMS_KES_CAPATH=kes-tls.crt
```
> The `MINIO_KMS_KES_CAPATH` is only required since we use self-signed certificates.

Optionally, enable auto-encryption to encrypt uploaded objects automatically:
```
export MINIO_KMS_AUTO_ENCRYPTION=on
```
> For more information about auto-encryption see: [Appendix A](#appendix-a---auto-encryption)

Then start the MinIO server:

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
```

```
minio server ~/export
```

### Appendix A - Auto-Encryption

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
mc cp test.file myminio/crypt/
test.file:              5 B / 5 B  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100.00% 337 B/s 0s
mc stat myminio/crypt/test.file
Name      : test.file
...
Encrypted :
  X-Amz-Server-Side-Encryption: AES256
```

### Appendix B - Specify a master key

Instead of a proper KMS setup you can also **test** MinIO encryption using a KMS master key.
**A single master key via env. variable is for testing purposes only and not recommended for production deployments.**

A KMS master key consists of a master-key ID (CMK) and the 256 bit master key encoded as HEX value separated by a `:`.
A KMS master key can be specified directly using:

```
export MINIO_KMS_MASTER_KEY=minio-demo-key:6368616e676520746869732070617373776f726420746f206120736563726574
```

Please use your own master key. A random master key can be generated using e.g. this command on Linux/Mac/BSD systems:

```
head -c 32 /dev/urandom | xxd -c 32 -ps
```

***

Alternatively, you may pass a master key as a [Docker secret](https://docs.docker.com/engine/swarm/secrets/).

```bash
echo "my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574" | docker secret create kms_master_key
```

To use another secret name, follow the instructions above and replace `kms_master_key` with your custom names (e.g. `my_kms_master_key`).
Then, set the `MINIO_KMS_MASTER_KEY_FILE` environment variable to your secret name:

```bash
export MINIO_KMS_MASTER_KEY_FILE=my_kms_master_key
```

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
