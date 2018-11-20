# Minio Docker Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

This quickstart guide describes how to quickly install and run a Minio Server Docker container.

1. [Install Docker](#installdocker) 
2. [Run Standalone Minio on Docker](#runstandalong) 
3. [Run Distributed Minio on Docker](#rundistributed) 

## <a name="installdocker"></a>1. Install Docker
Install Docker using these instructions: <https://www.docker.com/community-edition#/download>.

## <a name="runstandalong"></a>2. Run Standalone Minio on Docker

Minio can be run with either temporary or persistent storage, as described below.

### 2.1 Run Minio with Temporary Storage
Minio requires a persistent volume to store configuration and application data. However, a temporary directory can be specified for testing purposes. This temporary directory is created in the container's file system when the container is started, but all of the data is lost after the container exits.

Run the container with a temporary storage directory in `/data`:

```sh
docker run -p 9000:9000 minio/minio server /data
```

**Note:** A `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` must be specified to run containers. For more information see: [Specify a Custom Access Key and Secret Key](#specifycustomkeys).

### 2.2 Run Minio with Persistent Storage
To run a Minio container with persistent storage, local persistent directories must be mapped from the host OS to the virtual `config` and `export` directories. 

Use one of the methods below to run the container with  `~/.minio` as the virtual `config` directory and `/data` as the `export` directory:
* [GNU/Linux and macOS](#persistent_linuxmac)
* [Windows](#persistent_windows)

**Note:** In the following examples, `~/.minio` and `/data` are local directories being mapped inside the container.

#### <a name="persistent_linuxmac"></a>GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### <a name="persistent_windows"></a>Windows
```sh
docker run -p 9000:9000 --name minio1 \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

## <a name="rundistributed"></a>3. Run Distributed Minio on Docker
Distributed Minio can be deployed using one of these two methods:
* [Docker Compose](https://docs.minio.io/docs/deploy-minio-on-docker-compose): Creates a single host, multi-container deployment. This is useful for developing, testing, and staging environments with Distributed Minio.
* [Swarm mode](https://docs.minio.io/docs/deploy-minio-on-docker-swarm): Creates a multi-host, multi-container deployment. This offers a more robust, production-level deployment than Docker Compose.

## <a name="runcommands"></a>Examples of Typical Docker Commands

### <a name="specifycustomkeys"></a>Specify a Custom Access Key and Secret Key
To override Minio's auto-generated keys, pass the secret key and access key explicitly as environment variables using one of these methods:
* [GNU/Linux and macOS](#linuxmac_secret)
* [Windows](#windows_secret)

**Note:** Minio Server allows the use of regular strings for the access key and secret key.

#### <a name="linuxmac_secret"></a>GNU/Linux and macOS
```sh
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/data:/data \
  -v /mnt/config:/root/.minio \
  minio/minio server /data
```

#### <a name="windows_secret"></a>Windows
```powershell
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v D:\data:/data \
  -v D:\minio\config:/root/.minio \
  minio/minio server /data
```

### Specify a Custom Access Key and Secret Key Using Docker Secrets
When using Docker in Swarm mode, Minio's auto-generated keys can be overridden by passing the access key and secret key explicitly as [Docker secrets](https://docs.docker.com/engine/swarm/secrets/):

```
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

**Note:** Minio Server allows the use of regular strings for the access key and secret key.

Create a service for Minio using `docker service` to read from the Docker secrets:

```
docker service create --name="minio-service" --secret="access_key" --secret="secret_key" minio/minio server /data
```

See [How services work](https://docs.docker.com/engine/swarm/how-swarm-mode-works/services/) more information about `docker service`.

#### Specify a Custom Access Key and Secret Key Using Files
Run the service using the command below to specify other secret names. Replace `access_key` and `secret_key` with those from a development server (e.g. `my_secret_key`,`my_custom_key`):

```
docker service create --name="minio-service" \
  --secret="my_access_key" \
  --secret="my_secret_key" \
  --env="MINIO_ACCESS_KEY_FILE=my_access_key" \
  --env="MINIO_SECRET_KEY_FILE=my_secret_key" \
  minio/minio server /data
```

### Identify a Container's ID
The ID of a container must be specified when running Docker commands on it. 

Use the following command to get the IDs of all containers:

```sh
docker ps -a
```

**Note:** Specifying the `-a`  option returns all containers that have been created, are running, and have exited.

A response similar to this one should be displayed:

```sh
CONTAINER ID        IMAGE                     COMMAND                  CREATED             STATUS                     PORTS               NAMES
fabcc0ae9833        minio/minio               "/usr/bin/docker-e..."   4 minutes ago       Exited (0) 4 minutes ago                       minio2
1c51076ce4dc        minio/minio               "/usr/bin/docker-e..."   7 minutes ago       Created                                        minio1
```

Identify the ID of the container from the `Container ID` column displayed in the output.

### Starting and Stopping Containers
To start a stopped container, use the [`docker start`](https://docs.docker.com/engine/reference/commandline/start/) command:

```sh
docker start <container_id>
```

To stop a running container, use the [`docker stop`](https://docs.docker.com/engine/reference/commandline/stop/) command:
```sh
docker stop <container_id>
```

### Minio Container Logs
To access Minio logs, use the [`docker logs`](https://docs.docker.com/engine/reference/commandline/logs/) command:

```sh
docker logs <container_id>
```

### Monitor a Minio Docker Container
To monitor the resources used by a Minio container, use the [`docker stats`](https://docs.docker.com/engine/reference/commandline/stats/) command:

```sh
docker stats <container_id>
```

## Explore Further

* [Deploy Minio on Docker Compose](https://docs.minio.io/docs/deploy-minio-on-docker-compose)
* [Deploy Minio on Docker Swarm](https://docs.minio.io/docs/deploy-minio-on-docker-swarm)
* [Distributed Minio Quickstart Guide](https://docs.minio.io/docs/distributed-minio-quickstart-guide)
* [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
