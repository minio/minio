# Site Replication Guide #

This feature allows multiple independent MinIO sites (or clusters) that are using the same external IDentity Provider (IDP) to be configured as replicas. In this situation the set of replica sites are referred to as peer sites or just peers. This means that:

- when a bucket is created/deleted at a site, it is created/deleted on the other peer sites as well
- each bucket is automatically configured with versioning enabled and to replicate its data on the corresponding bucket in each of the remaining peer sites
- bucket policies, bucket tags, bucket object-lock configuration and bucket encryption settings are also replicated to all other peers
- all IAM policies are replicated to all other peers
- all service accounts belonging to users authenticated via the external IDP are replicated to all other peers

This feature is built on top of multi-site bucket replication feature.

## Configuring Site Replication ##

To configure site replication, ensure that all MinIO sites are using the same external IDP. 

1. Configure an alias in `mc` for each of the sites. For example if you have three MinIO sites, you may run:

```shell
$ mc alias set minio1 https://minio1.example.com:9000 minio1 minio1123
$ mc alias set minio2 https://minio2.example.com:9000 minio2 minio2123
$ mc alias set minio3 https://minio3.example.com:9000 minio3 minio3123
```

NOTE: When configuring site replication, each site except the first one is required to be empty.

2. Add site replication configuration with:

```shell
$ mc admin replicate add minio1 minio2 minio3
```

3. Once the above command returns success, you may query site replication configuration with:

```shell
$ mc admin replicate info minio1
```

*NOTE*: 
Site replication enables bucket versioning automatically for each bucket: it must not be modified by the cluster operator.
