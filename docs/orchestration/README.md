# Minio on orchestration platforms [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio is a cloud-native application desgined to scale in a sustainable manner in multi-tenant environment. Orchestration platforms provide perfect launchpad for Minio to scale.  

## What is cloud-native?
Cloud-native applications offer easy scale-out and hardware decoupling. They often have small footprints with no baggage of monolithic application era. This means, while the application itself appears to be pretty lightweight, it scales-out sustainably to achieve high availability and resilience. 

Take for example a web server. If running in a cloud environment, you wouldn't think twice before launching multiple instances of the server to handle extra load. This is because modern web servers don't have any hardware or software constraints. They scale-out to handle the extra load.

While storage has been traditionally thought of as complicated, Minio changes that with its cloud-native architecture. As a true scale-out system, Minio scales with your tenants. As and when you need more storage, you can add a new Minio instance bound to a tenant. 

## Orchestration platforms

Below is the list of documented orchestration platforms for Minio server: 

| Orchestration platforms|
|:---|
| [`Kubernetes`](./kubernetes) |
| [`Docker Swam`](./docker-swarm) |
| [`DC/OS`](./dcos) |
