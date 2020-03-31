# Deploy MinIO on Docker Swarm [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Docker Engine provides cluster management and orchestration features in Swarm mode. MinIO server can be easily deployed in distributed mode on Swarm to create a multi-tenant, highly-available and scalable object store.

As of [Docker Engine v1.13.0](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/) (Docker Compose v3.0), Docker Swarm and Compose are [cross-compatible](https://docs.docker.com/compose/compose-file/#version-3). This allows a Compose file to be used as a template to deploy services on Swarm. We have used a Docker Compose file to create distributed MinIO setup.

## 1. Prerequisites

* Familiarity with [Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/).
* Docker engine v1.13.0 running on a cluster of [networked host machines](https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines).

## 2. Create a Swarm
Create a swarm on the manager node by running

```shell
docker swarm init --advertise-addr <MANAGER-IP>
```
Once the swarm is initialized, you'll see the below response. 

```shell
docker swarm join \
  --token  SWMTKN-1-49nj1cmql0jkz5s954yi3oex3nedyz0fb0xx14ie39trti4wxv-8vxv8rssmk743ojnwacrr2e7c \
  192.168.99.100:2377
```

You can now [add worker nodes](https://docs.docker.com/engine/swarm/swarm-tutorial/add-nodes/) to the swarm by running the above command. Find detailed steps to create the swarm on [Docker documentation site](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/).

## 3. Create Docker secrets for MinIO

```shell
echo "AKIAIOSFODNN7EXAMPLE" | docker secret create access_key -
echo "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" | docker secret create secret_key -
```

## 4. Deploy distributed MinIO services

The example MinIO stack uses 4 Docker volumes, which are created automatically by deploying the stack. We have to make sure that the services in the stack are always (re)started on the same node, where the service is deployed the first time. 
Otherwise Docker will create a new volume upon restart of the service on another Docker node, which is not in sync with the other volumes and the stack will fail to start healthy. 
Before deploying the stack, add labels to the Docker nodes where you want the minio services to run:

```
docker node update --label-add minio1=true <DOCKER-NODE1>
docker node update --label-add minio2=true <DOCKER-NODE2>
docker node update --label-add minio3=true <DOCKER-NODE3>
docker node update --label-add minio4=true <DOCKER-NODE4>
```

It is possible to run more than one minio service on one Docker Node. Set the labels accordingly.

Download the [Docker Compose file](https://github.com/minio/minio/blob/master/docs/orchestration/docker-swarm/docker-compose-secrets.yaml?raw=true) on your Swarm master. Then execute the command

```shell
docker stack deploy --compose-file=docker-compose-secrets.yaml minio_stack
```

This deploys services described in the Compose file as Docker stack `minio_stack`. Look up the `docker stack` [command reference](https://docs.docker.com/engine/reference/commandline/stack/) for more info.

After the stack is successfully deployed, you should be able to access MinIO server via [MinIO Client](https://docs.min.io/docs/minio-client-complete-guide) `mc` or your browser at http://[Node_Public_IP_Address]:[Expose_Port_on_Host]

## 4. Remove distributed MinIO services

Remove the distributed MinIO services and related network by

```shell
docker stack rm minio_stack
```
Swarm doesn't automatically remove host volumes created for services. This may lead to corruption when a new MinIO service is created in the swarm. So, we recommend removing all the volumes used by MinIO, manually. To do this, logon to each node in the swarm and run

```shell
docker volume prune
```
This will remove all the volumes not associated with any container.

## 5. Accessing MinIO services

The services are exposed, by default, on the internal overlay network by their services names (minio1, minio2, ...).
The docker-compose.yml file also exposes the MinIO services behind a single alias on the minio_distributed network.

Services in the Swarm which are attached to that network can interact with the host "minio-cluster" instead of individual services' hostnames.  This provides a simple way to loosely load balance across all the MinIO services in the Swarm as well as simplifies configuration and management.

### Notes

* By default the Docker Compose file uses the Docker image for latest MinIO server release. You can change the image tag to pull a specific [MinIO Docker image](https://hub.docker.com/r/minio/minio/).

* There are 4 minio distributed instances created by default. You can add more MinIO services (up to total 16) to your MinIO Swarm deployment. To add a service
  * Replicate a service definition and change the name of the new service appropriately.
  * Add a volume in volumes section, and update volume section in the service accordingly.
  * Update the command section in each service. Specifically, add the drive location to be used as storage on the new service.
  * Update the port number to exposed for the new service.

  Read more about distributed MinIO [here](https://docs.min.io/docs/distributed-minio-quickstart-guide).

* By default the services use `local` volume driver. Refer to [Docker documentation](https://docs.docker.com/compose/compose-file/#/volume-configuration-reference) to explore further options.

* MinIO services in the Docker compose file expose ports 9001 to 9004. This allows multiple services to run on a host. Explore other configuration options in [Docker documentation](https://docs.docker.com/compose/compose-file/#/ports).

* Docker Swarm uses ingress load balancing by default. You can configure [external load balancer based](https://docs.docker.com/engine/swarm/ingress/#/configure-an-external-load-balancer) on requirements.

### Explore Further
- [Overview of Docker Swarm mode](https://docs.docker.com/engine/swarm/)
- [MinIO Docker Quickstart Guide](https://docs.min.io/docs/minio-docker-quickstart-guide)
- [Deploy MinIO on Docker Compose](https://docs.min.io/docs/deploy-minio-on-docker-compose)
- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
