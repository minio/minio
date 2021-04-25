# KMS IAM/Config Encryption

MinIO will soon release a change that re-works the encryption of IAM and
configuration data. Currently, MinIO encrypts IAM data (user/temp. credentials,
policies and other configuration data) with the cluster root credentials before
storing it on the backend disks. After release `RELEASE.2021-04-22T15-44-28Z`
onwards, MinIO will use the KMS provided keys to encrypt the IAM data instead
of the cluster root credentials. If the KMS is not enabled, MinIO will store
the IAM data as plain text in its backend.

### FAQ

> Why is this change needed? Was the previous encryption not secure (enough)?

The previous mechanism of encryption using the cluster root credentials itself
is secure. The purpose of this change is to improve the encryption key
management by delegating it to the KMS. Once the cluster root credentials are no
longer needed to decrypt persistent data at the backend, they can be rotated
much more easily. By using a KMS to protect the IAM data a potential security or
compliance team has much more control over how/when a MinIO cluster can be
started.

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
