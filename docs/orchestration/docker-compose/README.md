# Deploy MinIO on Docker Compose [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Docker Compose allows defining and running single host, multi-container Docker applications.

With Compose, you use a Compose file to configure MinIO services. Then, using a single command, you can create and launch all the Distributed MinIO instances from your configuration. Distributed MinIO instances will be deployed in multiple containers on the same host. This is a great way to set up development, testing, and staging environments, based on Distributed MinIO.

## 1. Prerequisites

* Familiarity with [Docker Compose](https://docs.docker.com/compose/overview/).
* Docker installed on your machine. Download the relevant installer from [here](https://www.docker.com/community-edition#/download).

## 2. Run Distributed MinIO on Docker Compose

To deploy Distributed MinIO on Docker Compose, please download [docker-compose.yaml](https://github.com/minio/minio/blob/master/docs/orchestration/docker-compose/docker-compose.yaml?raw=true) and [nginx.conf](https://github.com/minio/minio/blob/master/docs/orchestration/docker-compose/nginx.conf?raw=true) to your current working directory. Note that Docker Compose pulls the MinIO Docker image, so there is no need to build MinIO from source when using Docker. For non-Docker deployments, MinIO community edition is now source-only and can be installed via `go install github.com/minio/minio@latest`. Then run one of the below commands

### GNU/Linux and macOS

```sh
docker-compose pull
docker-compose up
```

or

```sh
docker stack deploy --compose-file docker-compose.yaml minio
```

### Windows

```sh
docker-compose.exe pull
docker-compose.exe up
```

or

```sh
docker stack deploy --compose-file docker-compose.yaml minio
```

Distributed instances are now accessible on the host using the Minio CLI on port 9000 and the Minio Web Console on port 9001. Proceed to access the Web browser at <http://127.0.0.1:9001/>. Here 4 MinIO server instances are reverse proxied through Nginx load balancing.

### Notes

* By default the Docker Compose file uses the Docker image for latest MinIO server release. You can change the image tag to pull a specific [MinIO Docker image](https://hub.docker.com/r/minio/minio/).

* There are 4 minio distributed instances created by default. You can add more MinIO services (up to total 16) to your MinIO Compose deployment. To add a service
  * Replicate a service definition and change the name of the new service appropriately.
  * Update the command section in each service.
  * Add a new MinIO server instance to the upstream directive in the Nginx configuration file.

  Read more about distributed MinIO [here](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-as-a-container.html).

### Explore Further

* [Overview of Docker Compose](https://docs.docker.com/compose/overview/)
* [MinIO Docker Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-as-a-container.html)
* [MinIO Erasure Code QuickStart Guide](https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html)
