# MinIO Deployment Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO is a cloud-native application designed to scale in a sustainable manner in multi-tenant environments. Orchestration platforms provide perfect launchpad for MinIO to scale. Below is the list of MinIO deployment documents for various orchestration platforms:

| Orchestration platforms|
|:---|
| [`Docker Swarm`](https://docs.min.io/docs/deploy-minio-on-docker-swarm) |
| [`Docker Compose`](https://docs.min.io/docs/deploy-minio-on-docker-compose) |
| [`Kubernetes`](https://docs.min.io/docs/deploy-minio-on-kubernetes) |
| [`DC/OS`](https://docs.min.io/docs/deploy-minio-on-dc-os) |

## Why is MinIO cloud-native?
The term cloud-native revolves around the idea of applications deployed as micro services, that scale well. It is not about just retrofitting monolithic applications onto modern container based compute environment. A cloud-native application is portable and resilient by design, and can scale horizontally by simply replicating. Modern orchestration platforms like Swarm, Kubernetes and DC/OS make replicating and managing containers in huge clusters easier than ever.

While containers provide isolated application execution environment, orchestration platforms allow seamless scaling by helping replicate and manage containers. MinIO extends this by adding isolated storage environment for each tenant.

MinIO is built ground up on the cloud-native premise. With features like erasure-coding, distributed and shared setup, it focuses only on storage and does it very well. While, it can be scaled by just replicating MinIO instances per tenant via an orchestration platform.  

> In a cloud-native environment, scalability is not a function of the application but the orchestration platform.

In a typical modern infrastructure deployment, application, database, key-store, etc. already live in containers and are managed by orchestration platforms. MinIO brings robust, scalable, AWS S3 compatible object storage to the lot.

![Cloud-native](https://github.com/minio/minio/blob/master/docs/screenshots/Minio_Cloud_Native_Arch.jpg?raw=true)
