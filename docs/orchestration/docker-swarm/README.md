# Deploy Minio on Docker Swarm [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine provides cluster management and orchestration features in Swarm mode. Minio server can be easily deployed in distributed mode on Swarm to create a multi-tenant, highly-available and scalable object store. As of [Docker Engine v1.13.0](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/) (Docker Compose v3.0), Docker Swarm and Compose are [cross-compatible](https://docs.docker.com/compose/compose-file/#version-3), i.e. a Compose file can be used to deploy service on Swarm or Docker Compose. We use Docker Compose (v3.0) to create distributed Minio setup.

## Prerequisites

1. Familiarity with [Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/).
2. Docker engine v1.13.0 running on a cluster of atleast 4 [networked host machines](https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines). 

## Create a Swarm

1. [Add a manager](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/) to your cluster. 
2. [Add worker nodes](https://docs.docker.com/engine/swarm/swarm-tutorial/add-nodes/) to the Swarm.

## 3. Deploy distributed Minio service

Download and save the 





