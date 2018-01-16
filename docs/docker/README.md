# Minio Docker Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## Prerequisites
Docker installed on your machine. Download the relevant installer from [here](https://www.docker.com/community-edition#/download).

## Run Standalone Minio on Docker.
Minio needs a persistent volume to store configuration and application data. However, for testing purposes, you can launch Minio by simply passing a directory (`/data` in the example below). This directory gets created in the container filesystem at the time of container start. But all the data is lost after container exits.

```sh
docker run -p 9000:9000 minio/minio server /data
```

To create a Minio container with persistent storage, you need to map local persistent directories from the host OS to virtual config `~/.minio` and export `/data` directories. To do this, run the below commands

#### GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### Windows
```sh
docker run -p 9000:9000 --name minio1 \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

## Run Distributed Minio on Docker
Distributed Minio can be deployed via [Docker Compose](https://docs.minio.io/docs/deploy-minio-on-docker-compose) or [Swarm mode](https://docs.minio.io/docs/deploy-minio-on-docker-swarm). The major difference between these two being, Docker Compose creates a single host, multi-container deployment, while Swarm mode creates a multi-host, multi-container deployment.

This means Docker Compose lets you quickly get started with Distributed Minio on your computer - ideal for development, testing, staging environments. While deploying Distributed Minio on Swarm offers a more robust, production level deployment.

## Minio Docker Tips

### Minio Custom Access and Secret Keys
To override Minio's auto-generated keys, you may pass secret and access keys explicitly as environment variables. Minio server also allows regular strings as access and secret keys.

#### GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### Windows
```powershell
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

### Minio Custom Access and Secret Keys using Docker secrets
To override Minio's auto-generated keys, you may pass secret and access keys explicitly by creating access and secret keys as [Docker secrets](https://docs.docker.com/engine/swarm/secrets/). Minio server also allows regular strings as access and secret keys.

```
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

Create a Minio service using `docker service` to read from Docker secrets.
```
docker service create --name="minio-service" --secret="access_key" --secret="secret_key" minio/minio server /data
```

Read more about `docker service` [here](https://docs.docker.com/engine/swarm/how-swarm-mode-works/services/)

#### Minio Custom Access and Secret Key files
To use other secret names follow the instuctions above and replace `access_key` and `secret_key` with your custom names (e.g. `my_secret_key`,`my_custom_key`). Run your service with
```
docker service create --name="minio-service" \
  --secret="my_access_key" \
  --secret="my_secret_key" \
  --env="MINIO_ACCESS_KEY_FILE=my_access_key" \
  --env="MINIO_SECRET_KEY_FILE=my_secret_key" \
  minio/minio server /data
```

### Retrieving Container ID
To use Docker commands on a specific container, you need to know the `Container ID` for that container. To get the `Container ID`, run

```sh
docker ps -a
```

`-a` flag makes sure you get all the containers (Created, Running, Exited). Then identify the `Container ID` from the output.

### Starting and Stopping Containers
To start a stopped container, you can use the [`docker start`](https://docs.docker.com/engine/reference/commandline/start/) command.

```sh
docker start <container_id>
```

To stop a running container, you can use the [`docker stop`](https://docs.docker.com/engine/reference/commandline/stop/) command.
```sh
docker stop <container_id>
```

### Minio container logs
To access Minio logs, you can use the [`docker logs`](https://docs.docker.com/engine/reference/commandline/logs/) command.

```sh
docker logs <container_id>
```

### Monitor Minio Docker Container
To monitor the resources used by Minio container, you can use the [`docker stats`](https://docs.docker.com/engine/reference/commandline/stats/) command.

```sh
docker stats <container_id>
```

## Explore Further

* [Deploy Minio on Docker Compose](https://docs.minio.io/docs/deploy-minio-on-docker-compose)
* [Deploy Minio on Docker Swarm](https://docs.minio.io/docs/deploy-minio-on-docker-swarm)
* [Distributed Minio Quickstart Guide](https://docs.minio.io/docs/distributed-minio-quickstart-guide)
* [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
