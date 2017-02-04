# Deploy Minio on Docker Swarm [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine provides cluster management and orchestration features in Swarm mode. Minio server can be easily deployed in distributed mode on Swarm to create a multi-tenant, highly-available and scalable object store. As of [Docker Engine v1.13.0](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/) (Docker Compose v3.0), Docker Swarm and Compose are [cross-compatible](https://docs.docker.com/compose/compose-file/#version-3), i.e. a Compose file can be used to deploy service on Swarm or Docker Compose. We use Docker Compose (v3.0) to create distributed Minio setup.

## 1. Prerequisites

* Familiarity with [Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/).
* Docker engine v1.13.0 running on a cluster of [networked host machines](https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines). 

## 2. Create a Swarm

SSH into the machine where you want to run your manager node. If the machine is named `manager`, command to SSH is

```shell
docker-machine ssh manager
```
After logging in to the designated manager node, create the Swarm by 

```shell
docker swarm init --advertise-addr <MANAGER-IP>
```

Find detailed steps to create the Swarm on [Docker documentation site](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/). After the manager is up, [add worker nodes](https://docs.docker.com/engine/swarm/swarm-tutorial/add-nodes/) to the Swarm.

## 3. Deploy distributed Minio services

Download the [Docker compose file](./docker-compose.yaml) on your Swarm master. Then execute the command

```shell
docker stack deploy --compose-file=docker-compose.yaml minio_stack
```
This deploys services described in the Compose file as Docker stack. Look up the `docker stack` [command reference](https://docs.docker.com/engine/reference/commandline/stack/) for more info. 

## 4. Remove distributed Minio services

Remove the distributed Minio servies and related network by

```shell
docker stack rm minio_stack
```

### Notes




