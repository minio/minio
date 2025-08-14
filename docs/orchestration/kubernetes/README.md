# Deploy MinIO on Kubernetes [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO is a high performance distributed object storage server, designed for large-scale private cloud infrastructure. MinIO is designed in a cloud-native manner to scale sustainably in multi-tenant environments. Orchestration platforms like Kubernetes provide perfect cloud-native environment to deploy and scale MinIO.

## MinIO Deployment on Kubernetes

There are multiple options to deploy MinIO on Kubernetes:

- MinIO-Operator: Operator offers seamless way to create and update highly available distributed MinIO clusters. Refer [MinIO Operator documentation](https://github.com/minio/minio-operator/blob/master/README.md) for more details.

- Helm Chart: MinIO Helm Chart offers customizable and easy MinIO deployment with a single command. Refer [MinIO Helm Chart documentation](https://github.com/minio/minio/tree/master/helm/minio) for more details.

## Monitoring MinIO in Kubernetes

MinIO server exposes un-authenticated liveness endpoints so Kubernetes can natively identify unhealthy MinIO containers. MinIO also exposes Prometheus compatible data on a different endpoint to enable Prometheus users to natively monitor their MinIO deployments.

## Explore Further

- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Helm package manager for kubernetes](https://helm.sh/)
