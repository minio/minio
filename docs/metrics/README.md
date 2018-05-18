## Minio Monitoring Guide

Minio server exposes monitoring data over un-authenticated endpoints so monitoring tools can pick the data without you having to share Minio server credentials. This document lists the monitoring endpoints and relevant documentation.

### Healthcheck Probe

Minio server has two healthcheck related endpoints, a liveness probe to indicate if server is working fine and a readiness probe to indicate if server is not accepting connections due to heavy load.

- Liveness probe available at `/minio/health/live`
- Readiness probe available at `/minio/health/ready`

Read more on how to use these endpoints in [Minio healthcheck guide](https://github.com/minio/minio/blob/master/docs/metrics/healthcheck/README.md).

### Prometheus Probe

Minio server exposes Prometheus compatible data on a single endpoint.

- Prometheus data available at `/minio/prometheus/metrics`

To use this endpoint, setup Prometheus to scrape data from this endpoint. Read more on how to use Prometheues to monitor Minio server in [How to monitor Minio server with Prometheus](https://github.com/minio/cookbook/blob/master/docs/how-to-monitor-minio-with-prometheus.md).
