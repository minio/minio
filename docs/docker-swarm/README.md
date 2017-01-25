# Minio deployment on Docker Swarm [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine provides cluster management and orchestration features when running in Swarm mode. Minio server in distributed mode can be 
easily deployed on Swarm to create a multi-tenant, highly-available and scalable object store. 

## Why deploy Minio on Swarm?

Modern, cloud-native applications are desgined around the needs of various tenants. A Minio instance best serves the needs of a 
single tenant. As and when required, you can spin new Minio instances to handle the needs of new tenant. With recent advancements in 
DevOps and Cloud deployment strategies, it doesn't make sense for Minio to manage the infrastructure it is running on. That task can be 
safely handed over to orchestration tools like Docker Swarm. 

This is why, Minio is designed to work in conjunction with external orchestrators. Not only this enables keeping each tenant relatively 
small and thus limit the failure domain, it also makes deploying new Minio instance as easy as launching a [Swarm service](https://docs.docker.com/engine/swarm/key-concepts/#/services-and-tasks). This ensures that as you scale, the complexity doesn't scale proportionately. 

# Get started

We recommend Docker Engine v1.13.0 (Docker Compose v 3.0) as the starting point. This is because of [cross-compatibility](https://docs.docker.com/compose/compose-file/#version-3) between Compose and the Docker Engine’s swarm mode, i.e. same Compose file can be used to deploy service on Swarm or Docker Componse. 

## 1. Prerequisites

