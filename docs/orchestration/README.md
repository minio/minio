# Minio on orchestration platforms [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Modern, cloud-native applications are designed around the needs of individual tenants. A Minio instance best serves the needs of a 
single tenant. As and when required, you can spin new Minio instances to handle the needs of new tenant. With recent advancements in 
DevOps and Cloud deployment strategies, it doesn't make sense for Minio to manage the infrastructure it is running on. That task can be 
safely handed over to orchestration tools. 

This is why, Minio is designed to work in conjunction with external orchestrators. Not only this enables keeping each tenant relatively 
small and thus limit the failure domain, it also makes deploying new Minio instance. This ensures that as the application scales, the complexity doesn't scale proportionately. 

Minio server supports Amazon S3 compatible bucket event notification for the following targets

| Notification Targets|
|:---|
| [`Kubernetes`](./kubernetes) |
| [`DC/OS`](./dcos) |
| [`Docker Swam`](./docker-swarm) |
