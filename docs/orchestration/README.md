# Minio Orchestration Platforms Deployment Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio is a cloud-native application designed to scale in a sustainable manner in multi-tenant environments. Orchestration platforms provide perfect launchpad for Minio to scale. Below is the list of documented orchestration platforms for Minio server:

| Orchestration platforms|
|:---|
| [`Kubernetes`](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/README.md) |
| [`Docker Swarm`](https://github.com/minio/minio/blob/master/docs/orchestration/docker-swarm/README.md) |
| [`DC/OS`](https://github.com/minio/minio/blob/master/docs/orchestration/dcos/README.md) |

## Why is Minio cloud-native?
The term cloud-native revolves around the idea of applications deployed as micro services, that can be scaled well. Its not about just retrofitting monolithic applications onto modern container based compute environment. A cloud-native application is portable and resilient by design, and can scale horizontally by simply replicating. Modern orchestration platforms like DC/OS, Kubernetes and Swarm make replicating and managing containers in huge clusters easier than ever.

While containers provide isolated application execution environment, orchestration platforms allow seamless scaling by helping replicate and manage containers. Minio extends this by adding isolated storage environment for each tenant.

Minio is built ground up on the cloud-native premise. With features like erasure-coding, distributed and shared setup, it focusses only on storage and does it very well. While, it can be scaled by just replicating Minio instances per tenant via an orchestration platform.  

> In a cloud-native environment, scalability is not a function of the application but the orchestration platform.

In a typical modern infrastructure deployment, application, Database, Key-store, etc. already live in containers and are managed by orchestration platforms. Minio brings robust, scalable, AWS S3 compatible object storage to the lot.

![Cloud-native](https://raw.githubusercontent.com/NitishT/minio/master/docs/screenshots/Minio_Cloud_Native_Arch.png?raw=true)
