# MinIO Multi-Tenant Deployment Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

This topic provides commands to set up different configurations of hosts, nodes, and drives. The examples provided here can be used as a starting point for other configurations.

1. [Standalone Deployment](#standalone-deployment)
2. [Distributed Deployment](#distributed-deployment)
3. [Cloud Scale Deployment](#cloud-scale-deployment)

## 1. Standalone Deployment

To host multiple tenants on a single machine, run one MinIO Server per tenant with a dedicated HTTPS port, configuration, and data directory.

### 1.1 Host Multiple Tenants on a Single Drive

Use the following commands to host 3 tenants on a single drive:

```sh
minio server --address :9001 /data/tenant1
minio server --address :9002 /data/tenant2
minio server --address :9003 /data/tenant3
```

![Example-1](https://github.com/minio/minio/blob/master/docs/screenshots/Example-1.jpg?raw=true)

### 1.2 Host Multiple Tenants on Multiple Drives (Erasure Code)

Use the following commands to host 3 tenants on multiple drives:

```sh
minio server --address :9001 /disk{1...4}/data/tenant1
minio server --address :9002 /disk{1...4}/data/tenant2
minio server --address :9003 /disk{1...4}/data/tenant3
```

![Example-2](https://github.com/minio/minio/blob/master/docs/screenshots/Example-2.jpg?raw=true)

## 2. Distributed Deployment

To host multiple tenants in a distributed environment, run several distributed MinIO Server instances concurrently.

### 2.1 Host Multiple Tenants on Multiple Drives (Erasure Code)

Use the following commands to host 3 tenants on a 4-node distributed configuration:

```sh
export MINIO_ROOT_USER=<TENANT1_ACCESS_KEY>
export MINIO_ROOT_PASSWORD=<TENANT1_SECRET_KEY>
minio server --address :9001 http://192.168.10.1{1...4}/data/tenant1

export MINIO_ROOT_USER=<TENANT2_ACCESS_KEY>
export MINIO_ROOT_PASSWORD=<TENANT2_SECRET_KEY>
minio server --address :9002 http://192.168.10.1{1...4}/data/tenant2

export MINIO_ROOT_USER=<TENANT3_ACCESS_KEY>
export MINIO_ROOT_PASSWORD=<TENANT3_SECRET_KEY>
minio server --address :9003 http://192.168.10.1{1...4}/data/tenant3
```

**Note:** Execute the commands on all 4 nodes.

![Example-3](https://github.com/minio/minio/blob/master/docs/screenshots/Example-3.jpg?raw=true)

**Note**: On distributed systems, root credentials are recommend to be defined by exporting the `MINIO_ROOT_USER` and  `MINIO_ROOT_PASSWORD` environment variables. If no value is set MinIO setup will assume `minioadmin/minioadmin` as default credentials. If a domain is required, it must be specified by defining and exporting the `MINIO_DOMAIN` environment variable.

## Cloud Scale Deployment

A container orchestration platform (e.g. Kubernetes) is recommended for large-scale, multi-tenant MinIO deployments. See the [MinIO Deployment Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/kubernetes.html) to get started with MinIO on orchestration platforms.
