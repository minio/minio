# Deploy Minio on Docker Swarm [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Docker Engine provides cluster management and orchestration features in Swarm mode. Minio server can be easily deployed in distributed mode on Swarm to create a multi-tenant, highly-available and scalable object store. 

As of [Docker Engine v1.13.0](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/) (Docker Compose v3.0), Docker Swarm and Compose are [cross-compatible](https://docs.docker.com/compose/compose-file/#version-3). This allows a Compose file to be used as a template to deploy services on Swarm. We have used a Docker Compose file to create distributed Minio setup.

## 1. Prerequisites

* Familiarity with [Swarm mode key concepts](https://docs.docker.com/engine/swarm/key-concepts/).
* Docker engine v1.13.0 running on a cluster of [networked host machines](https://docs.docker.com/engine/swarm/swarm-tutorial/#/three-networked-host-machines). 

## 2. Create a Swarm

SSH into the machine supposed to serve as Swarm manager. If the machine is named `manager`, you can SSH by

```shell
docker-machine ssh manager
```
After logging in to the designated manager node, create the Swarm by 

```shell
docker swarm init --advertise-addr <MANAGER-IP>
```

After the manager is up, [add worker nodes](https://docs.docker.com/engine/swarm/swarm-tutorial/add-nodes/) to the Swarm. Find detailed steps to create the Swarm on [Docker documentation site](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/). 

## 3. Deploy distributed Minio services

Download the [Docker Compose file](https://github.com/minio/minio/blob/master/docs/orchestration/docker-swarm/docker-compose.yaml) on your Swarm master. Then execute the command

```shell
docker stack deploy --compose-file=docker-compose.yaml minio_stack
```
This deploys services described in the Compose file as Docker stack `minio_stack`. Look up the `docker stack` [command reference](https://docs.docker.com/engine/reference/commandline/stack/) for more info. 

After the stack is successfully deployed, you should be able to access Minio server via [Minio Client](https://docs.minio.io/docs/minio-client-complete-guide) `mc` or your browser at http://[Node_Public_IP_Address]:[Expose_Port_on_Host]

## 4. Remove distributed Minio services

Remove the distributed Minio services and related network by

```shell
docker stack rm minio_stack
```

### Notes

* By default the Docker Compose file uses the Docker image for latest Minio server release. You can change the image tag to pull a specific [Minio Docker image](https://hub.docker.com/r/minio/minio/). 

* There are 4 minio distributed instances created by default. You can add more Minio services (upto total 16) to your Minio Swarm deployment. To add a deployment
  * Replicate a service definition and change the name of the new service appropriately.
  * Add a volume in volumes section, and update volume section in the service accordingly.
  * Update the command section in each service. Specifically, add the drive location to be used as storage on the new service. 
  * Update the port number to exposed for the new service. 

  Read more about distributed Minio [here](https://docs.minio.io/docs/distributed-minio-quickstart-guide).

* By default the services use `local` volume driver. Refer to [Docker documentation](https://docs.docker.com/compose/compose-file/#/volume-configuration-reference) to explore further options. 

* Minio services in the Docker compose file expose ports 9001 to 9004. This allows multiple services to run on a host. Explore other configuration options in [Docker documentation](https://docs.docker.com/compose/compose-file/#/ports).

* Docker Swarm uses ingress load balancing by default. You can configure [external load balancer based](https://docs.docker.com/engine/swarm/ingress/#/configure-an-external-load-balancer) on requirements. 

### Explore Further
- [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
