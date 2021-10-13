# Site Replication Guide #

This feature allows multiple independent MinIO sites (or clusters) that are using the same external IDentity Provider (IDP) to be configured as replicas. In this situation the set of replica sites are referred to as peer sites or just sites. When site-replication is enabled on a set of sites, the following changes are replicated to all other sites:

- creation and deletion of buckets and objects
- creation and deletion of all IAM policies
- creation of STS credentials and creation and deletion of service accounts (for users authenticated by the external IDP)
- changes to bucket policies, bucket tags, bucket object-lock configurations (including retention and legal hold configuration) and bucket encryption configuration

The following bucket-scoped items are **not replicated**, and can differ between sites:

- bucket notification configuration
- ILM configuration

This feature is built on top of multi-site bucket replication feature. It enables bucket versioning automatically for all new and existing buckets in the replicated sites.

## Pre-requisites

1. Initially, only **one** of the sites being added for replication may have data. After site-replication is successfully configured, this data is replicated to the other (initially empty) sites. Subsequently, objects may be written to any of the sites, and they will be replicated to all other sites.
2. Only the **LDAP IDP** is currently supported.
3. At present, all sites are **required** to have the same root credentials.
4. At present it is not possible to **add a new site** to an existing set of replicated sites or to **remove a site** from a set of replicated sites.
5. If using [SSE-S3 or SSE-KMS encryption via KMS](https://docs.min.io/docs/minio-kms-quickstart-guide.html "MinIO KMS Guide"), all sites are required to have access to the same KES keys. This can be achieved via a central KES server or multiple KES servers (say one per site) connected to a central KMS server.

## Configuring Site Replication ##

To configure site replication, ensure that all MinIO sites are using the same external IDP. 

1. Configure an alias in `mc` for each of the sites. For example if you have three MinIO sites, you may run:

```shell
$ mc alias set minio1 https://minio1.example.com:9000 minio1 minio1123
$ mc alias set minio2 https://minio2.example.com:9000 minio2 minio2123
$ mc alias set minio3 https://minio3.example.com:9000 minio3 minio3123
```

2. Add site replication configuration with:

```shell
$ mc admin replicate add minio1 minio2 minio3
```

3. Once the above command returns success, you may query site replication configuration with:

```shell
$ mc admin replicate info minio1
```
