# Deploy Minio on Docker Swarm [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine provides cluster management and orchestration features when running in Swarm mode. Minio server in distributed mode can be 
easily deployed on Swarm to create a multi-tenant, highly-available and scalable object store. 

We recommend Docker Engine v1.13.0 (Docker Compose v 3.0) as the starting point. This is because of [cross-compatibility](https://docs.docker.com/compose/compose-file/#version-3) between Compose and the Docker Engine’s swarm mode, i.e. same Compose file can be used to deploy service on Swarm or Docker Compose. 

## 1. Prerequisites

You are familiar with [Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/), and have a cluster of atleast 4 [networked host machines](https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines).

## 2. Create a Swarm

After you have networked host machines ready, you'll need to [create a Swarm](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/).

## 3. Deploy distributed Minio service



