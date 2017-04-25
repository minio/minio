# Multi-tenant Minio Deployment Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## Standalone Deployment
To host multiple tenants on a single machine, run one Minio server per tenant with dedicated HTTPS port, config and data directory.  

#### Example 1 : Single host, single drive

This example hosts 3 tenants on a single drive.
```sh
minio --config-dir /data/conf1 server --address :9001 /data/export1
minio --config-dir /data/conf2 server --address :9002 /data/export2
minio --config-dir /data/conf3 server --address :9003 /data/export3
```
#### Example 2 : Single host, multiple drives (erasure code)

This example hosts 4 tenants on multiple drives.
```sh
minio --config-dir ~/tenant1 server --address :9001 /drive1/data/tenant1 /drive2/data/tenant1 /drive3/data/tenant1 /drive4/data/tenant1
minio --config-dir ~/tenant2 server --address :9002 /drive1/data/tenant2 /drive2/data/tenant2 /drive3/data/tenant2 /drive4/data/tenant2
minio --config-dir ~/tenant3 server --address :9003 /drive1/data/tenant3 /drive2/data/tenant3 /drive3/data/tenant3 /drive4/data/tenant3
```

## Distributed Deployment
To host multiple tenants in a distributed environment, run several standalone or distributed Minio instances.  

#### Example 1 : Multiple hosts, each with single drive

This example hosts 3 tenants on 3 hosts.
```sh
minio server /data/export1
minio --config-dir /data/conf2 server --address :9002 /data/export2
minio --config-dir /data/conf3 server --address :9003 /data/export3
```
#### Example 2 : Single host, multiple drives (erasure code)

This example hosts 4 tenants on multiple drives.
```sh
minio --config-dir ~/tenant1 server --address :9001 /drive1/data/tenant1 /drive2/data/tenant1 /drive3/data/tenant1 /drive4/data/tenant1
minio --config-dir ~/tenant2 server --address :9002 /drive1/data/tenant2 /drive2/data/tenant2 /drive3/data/tenant2 /drive4/data/tenant2
minio --config-dir ~/tenant3 server --address :9003 /drive1/data/tenant3 /drive2/data/tenant3 /drive3/data/tenant3 /drive4/data/tenant3
```

## Datacentre Wide Deployment
For massive multi-tenant Minio deployments in production environments, we recommend using one of the major container orchestration platforms, e.g. Kubernetes, DC/OS or Docker Swarm. Refer [this document](https://docs.minio.io/docs/minio-deployment-quickstart-guide) to get started with Minio on orchestration platforms.  


