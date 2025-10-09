# Automatic Site Replication

This feature allows multiple independent MinIO sites (or clusters) that are using the same external IDentity Provider (IDP) to be configured as replicas. In this situation the set of replica sites are referred to as peer sites or just sites. When site-replication is enabled on a set of sites, the following changes are replicated to all other sites:

- Creation and deletion of buckets and objects
- Creation and deletion of all IAM users, groups, policies and their mappings to users or groups
- Creation of STS credentials
- Creation and deletion of service accounts (except those owned by the root user)
- Changes to Bucket features such as:
  - Bucket Policies
  - Bucket Tags
  - Bucket Object-Lock configurations (including retention and legal hold configuration)
  - Bucket Encryption configuration

> NOTE: Bucket versioning is automatically enabled for all new and existing buckets on all replicated sites.

The following Bucket features will **not be replicated**, is designed to differ between sites:

- Bucket notification configuration
- Bucket lifecycle (ILM) configuration

## Pre-requisites

- Initially, only **one** of the sites added for replication may have data. After site-replication is successfully configured, this data is replicated to the other (initially empty) sites. Subsequently, objects may be written to any of the sites, and they will be replicated to all other sites.

- **Removing a site** is not allowed from a set of replicated sites once configured.
- All sites must be using the **same** external IDP(s) if any.
- For [SSE-S3 or SSE-KMS encryption via KMS](https://docs.min.io/community/minio-object-store/operations/server-side-encryption.html "MinIO KMS Guide"), all sites **must**  have access to a central KMS deployment. This can be achieved via a central KES server or multiple KES servers (say one per site) connected via a central KMS (Vault) server.

## Configuring Site Replication

- Configure an alias in `mc` for each of the sites. For example if you have three MinIO sites, you may run:

```sh
mc alias set minio1 https://minio1.example.com:9000 adminuser adminpassword
mc alias set minio2 https://minio2.example.com:9000 adminuser adminpassword
mc alias set minio3 https://minio3.example.com:9000 adminuser adminpassword
```

or

```sh
export MC_HOST_minio1=https://adminuser:adminpassword@minio1.example.com
export MC_HOST_minio2=https://adminuser:adminpassword@minio2.example.com
export MC_HOST_minio3=https://adminuser:adminpassword@minio3.example.com
```

- Add site replication configuration with:

```sh
mc admin replicate add minio1 minio2 minio3
```

- Once the above command returns success, you may query site replication configuration with:

```sh
mc admin replicate info minio1
```

** Note **
Previously, site replication required the root credentials of peer sites to be identical. This is no longer necessary because STS tokens are now signed with the site replicator service account credentials, thus allowing flexibility in the independent management of root accounts across sites and the ability to disable root accounts eventually.

However, this means that STS tokens signed previously by root credentials will no longer be valid upon upgrading to the latest version with this change. Please re-generate them as you usually do. Additionally, if site replication is ever removed - the STS tokens will become invalid, regenerate them as you usually do.