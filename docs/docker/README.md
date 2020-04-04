# MinIO Docker Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Prerequisites
Docker installed on your machine. Download the relevant installer from [here](https://www.docker.com/community-edition#/download).

## Run Standalone MinIO on Docker.
MinIO needs a persistent volume to store configuration and application data. However, for testing purposes, you can launch MinIO by simply passing a directory (`/data` in the example below). This directory gets created in the container filesystem at the time of container start. But all the data is lost after container exits.

```sh
docker run -p 9000:9000 minio/minio server /data
```

To create a MinIO container with persistent storage, you need to map local persistent directories from the host OS to virtual config `~/.minio` and export `/data` directories. To do this, run the below commands

#### GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -v /mnt/data:/data \
  minio/minio server /data
```

#### Windows
```sh
docker run -p 9000:9000 --name minio1 \
  -v D:\data:/data \
  minio/minio server /data
```

## Run Distributed MinIO on Docker
Distributed MinIO can be deployed via [Docker Compose](https://docs.min.io/docs/deploy-minio-on-docker-compose) or [Swarm mode](https://docs.min.io/docs/deploy-minio-on-docker-swarm). The major difference between these two being, Docker Compose creates a single host, multi-container deployment, while Swarm mode creates a multi-host, multi-container deployment.

This means Docker Compose lets you quickly get started with Distributed MinIO on your computer - ideal for development, testing, staging environments. While deploying Distributed MinIO on Swarm offers a more robust, production level deployment.

## MinIO Docker Tips

### MinIO Custom Access and Secret Keys
To override MinIO's auto-generated keys, you may pass secret and access keys explicitly as environment variables. MinIO server also allows regular strings as access and secret keys.

#### GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/data:/data \
  minio/minio server /data
```

#### Windows
```powershell
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  minio/minio server /data
```

### Run MinIO Docker as a regular user
Docker provides standardized mechanisms to run docker containers as non-root users.

#### GNU/Linux and macOS
On Linux and macOS you can use `--user` to run the container as regular user.

> NOTE: make sure --user has write permission to *${HOME}/data* prior to using `--user`.
```sh
mkdir -p ${HOME}/data
docker run -p 9000:9000 \
  --user $(id -u):$(id -g) \
  --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY" \
  -v ${HOME}/data:/data \
  minio/minio server /data
```

#### Windows
On windows you would need to use [Docker integrated windows authentication](https://success.docker.com/article/modernizing-traditional-dot-net-applications#integratedwindowsauthentication) and [Create a container with Active Directory Support](https://blogs.msdn.microsoft.com/containerstuff/2017/01/30/create-a-container-with-active-directory-support/)

> NOTE: make sure your AD/Windows user has write permissions to *D:\data* prior to using `credentialspec=`.

```powershell
docker run -p 9000:9000 \
  --name minio1 \
  --security-opt "credentialspec=file://myuser.json"
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  minio/minio server /data
```

### MinIO Custom Access and Secret Keys using Docker secrets
To override MinIO's auto-generated keys, you may pass secret and access keys explicitly by creating access and secret keys as [Docker secrets](https://docs.docker.com/engine/swarm/secrets/). MinIO server also allows regular strings as access and secret keys.

```
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

Create a MinIO service using `docker service` to read from Docker secrets.
```
docker service create --name="minio-service" --secret="access_key" --secret="secret_key" minio/minio server /data
```

Read more about `docker service` [here](https://docs.docker.com/engine/swarm/how-swarm-mode-works/services/)

#### MinIO Custom Access and Secret Key files
To use other secret names follow the instructions above and replace `access_key` and `secret_key` with your custom names (e.g. `my_secret_key`,`my_custom_key`). Run your service with
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

### MinIO container logs
To access MinIO logs, you can use the [`docker logs`](https://docs.docker.com/engine/reference/commandline/logs/) command.

```sh
docker logs <container_id>
```

### Monitor MinIO Docker Container
To monitor the resources used by MinIO container, you can use the [`docker stats`](https://docs.docker.com/engine/reference/commandline/stats/) command.

```sh
docker stats <container_id>
```

## Explore Further

* [Deploy MinIO on Docker Compose](https://docs.min.io/docs/deploy-minio-on-docker-compose)
* [Deploy MinIO on Docker Swarm](https://docs.min.io/docs/deploy-minio-on-docker-swarm)
* [Distributed MinIO Quickstart Guide](https://docs.min.io/docs/distributed-minio-quickstart-guide)
* [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
