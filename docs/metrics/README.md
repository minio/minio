# MinIO Monitoring Guide

MinIO server exposes monitoring data over endpoints. Monitoring tools can pick the data from these endpoints. This document lists the monitoring endpoints and relevant documentation.

## Healthcheck Probe

MinIO server has two healthcheck related un-authenticated endpoints, a liveness probe to indicate if server is responding, cluster probe to check if server can be taken down for maintenance.

- Liveness probe available at `/minio/health/live`
- Cluster probe available at `/minio/health/cluster`

Read more on how to use these endpoints in [MinIO healthcheck guide](https://github.com/minio/minio/blob/master/docs/metrics/healthcheck/README.md).

## Prometheus Probe

MinIO allows reading metrics for the entire cluster from any single node. This allows for metrics collection for a MinIO instance across all servers. Thus, metrics collection for instances behind a load balancer can be done without any knowledge of the individual node addresses. The cluster wide metrics can be read at
`<Address for MinIO Service>/minio/v2/metrics/cluster`.

The additional node specific metrics which include additional go metrics or process metrics are exposed at
`<Address for MinIO Node>/minio/v2/metrics/node`.

The additional bucket specific metrics which include additional go metrics or process metrics are exposed at
`<Address for MinIO Node>/minio/v2/metrics/bucket`.

The additional resource specific metrics which include additional go metrics or process metrics are exposed at
`<Address for MinIO Node>/minio/v2/metrics/resource`.

To use this endpoint, setup Prometheus to scrape data from this endpoint. Read more on how to configure and use Prometheus to monitor MinIO server in [How to monitor MinIO server with Prometheus](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/README.md).

### **Deprecated metrics monitoring**

- Prometheus' data available at `/minio/prometheus/metrics` is deprecated
