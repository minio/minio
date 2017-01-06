# Distributed Minio Docker Guide for Windows [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## 1. Prerequisites

Install Docker Toolbox for Windows from [here](http://www.docker.com/products/docker-toolbox)

## 2. Installation 

Open Docker quickstart terminal and execute these commands.

### Pull Minio Docker image from Docker Hub.

```
docker pull minio/minio
```

### Copy Docker Compose configuration file.

```
curl https://raw.githubusercontent.com/minio/minio/master/docs/docker/docker-compose.yml > docker-compose.yml
```
### Start Minio distributed cluster.

```
docker-compose.exe up
```
Minio browser will be simply accessible at one of the ports 9001-9004.

## 3. Explore Further

* [Minio Docker Quickstart Guide ](https://docs.minio.io/docs/minio-docker-quickstart-guide)
* [Docker Compose](https://docs.docker.com/compose/)
* [Distributed Minio Quickstart Guide ](https://docs.minio.io/docs/distributed-minio-quickstart-guide)
