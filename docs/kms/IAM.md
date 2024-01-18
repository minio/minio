# KMS IAM/Config Encryption

MinIO supports encrypting config, IAM assets with KMS provided keys. If the KMS is not enabled, MinIO will store the config, IAM data as plain text erasure coded in its backend.

## MinIO KMS Quick Start

MinIO supports two ways of encrypting IAM and configuration data.
You can either use KES - together with an external KMS - or, much simpler,
set the env. variable `MINIO_KMS_SECRET_KEY` and start/restart the MinIO server. For more details about KES and how
to set it up refer to our [KMS Guide](https://github.com/minio/minio/blob/master/docs/kms/README.md).

Instead of configuring an external KMS you can start with a single key by
setting the env. variable `MINIO_KMS_SECRET_KEY`. It expects the following
format:

```sh
MINIO_KMS_SECRET_KEY=<key-name>:<base64-value>
```

First generate a 256 bit random key via:

```sh
$ cat /dev/urandom | head -c 32 | base64 -
OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
```

Now, you can set `MINIO_KMS_SECRET_KEY` like this:

```sh
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
```

> You can choose an arbitrary name for the key - instead of `my-minio-key`.
> Please note that losing the `MINIO_KMS_SECRET_KEY` will cause data loss
> since you will not be able to decrypt the IAM/configuration data anymore.
For distributed MinIO deployments, specify the *same* `MINIO_KMS_SECRET_KEY` for each MinIO server process.

At any point in time you can switch from `MINIO_KMS_SECRET_KEY` to a full KMS
deployment. You just need to import the generated key into KES - for example via
the KES CLI once you have successfully setup KES:

```sh
kes key create my-minio-key OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
```

- For instructions on setting up KES, see the [KES Getting Started guide](https://github.com/minio/kes/wiki/Getting-Started)

- For instructions on using KES for encrypting the MinIO backend, follow the [KMS Quick Start](https://github.com/minio/minio/tree/master/docs/kms). The SSE-S3 configuration setup also supports MinIO KMS backend encryption.

## FAQ

> Why is this change needed?

Before, there were two separate mechanisms - S3 objects got encrypted using a KMS,
if present, and the IAM / configuration data got encrypted with the root credentials.
Now, MinIO encrypts IAM / configuration and S3 objects with a KMS, if present. This
change unified the key-management aspect within MinIO.

The unified KMS-based approach has several advantages:

- Key management is now centralized. There is one way to change or rotate encryption keys.
   There used to be two different mechanisms - one for regular S3 objects and one for IAM data.
- Reduced server startup time. For IAM encryption with the root credentials, MinIO had
   to use a memory-hard function (Argon2) that (on purpose) consumes a lot of memory and CPU.
   The new KMS-based approach can use a key derivation function that is orders of magnitudes
   cheaper w.r.t. memory and CPU.
- Root credentials can now be changed easily. Before, a two-step process was required to
   change the cluster root credentials since they were used to en/decrypt the IAM data.
   So, both - the old and new credentials - had to be present at the same time during a rotation
   and the old credentials had to be removed once the rotation completed. This process is now gone.
   The root credentials can now be changed easily.

> Does this mean I need an enterprise KMS setup to run MinIO (securely)?

No, MinIO does not depend on any third-party KMS provider. You have three options here:

- Run MinIO without a KMS. In this case all IAM data will be stored in plain-text.
- Run MinIO with a single secret key. MinIO supports a static cryptographic key
  that can act as minimal KMS. With this method all IAM data will be stored
  encrypted. The encryption key has to be passed as environment variable.
- Run MinIO with KES (minio/kes) in combination with any supported KMS as
  secure key store. For example, you can run MinIO + KES + Hashicorp Vault.

> What about an exiting MinIO deployment? Can I just upgrade my cluster?

Yes, MinIO will try to transparently migrate any existing IAM data and either stores
it in plaintext (no KMS) or re-encrypts using the KMS.

> Is this change backward compatible? Will it break my setup?

This change is not backward compatible for all setups. In particular, the native
Hashicorp Vault integration - which has been deprecated already - won't be
supported anymore. KES is now mandatory if a third-party KMS should be used.

Further, since the configuration data is encrypted with the KMS, the KMS
configuration itself can no longer be stored in the MinIO config file and
instead must be provided via environment variables. If you have set your KMS
configuration using e.g. the `mc admin config` commands you will need to adjust
your deployment.

Even though this change is backward compatible we do not expect that it affects
the vast majority of deployments in any negative way.

> Will an upgrade of an existing MinIO cluster impact the SLA of the cluster or will it even cause downtime?

No, an upgrade should not cause any downtime. However, on the first startup -
since MinIO will attempt to migrate any existing IAM data - the boot process may
take slightly longer, but may not be visibly noticeable. Once the migration has
completed, any subsequent restart should be as fast as before or even faster.
