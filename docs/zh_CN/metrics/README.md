## Minio监控指南 

Minio服务器通过未经身份验证的端点公开监视数据，因此监视工具可以在无需共享MinIO服务器凭据的情况下获取数据。本文档列出了监控端点和相关文档。

### 状况检查探针
MinIO服务器有两个与healthcheck相关的端点：活动探针，用于标示服务器是否正常工作，以及准备探针，以标示服务器是否由于负载过重而不接受连接。

- 活动探针可在 `/minio/health/live` 获得
- 准备探针可在 `/minio/health/ready` 获得

在[MinIO状况检查指南](https://github.com/minio/minio/blob/master/docs/metrics/healthcheck/README.md)中阅读更多如何使用这些端点的信息。

### Prometheus探针
MinIO服务器在单个端点上公开Prometheus兼容数据。

- Prometheus数据可在 `/minio/prometheus/metrics`

要使用此端点，请设置Prometheus以从此端点获取数据。了解更多关于如何使用Prometheues监测MinIO服务器[如何用Prometheus监控MinIO服务器](https://github.com/minio/cookbook/blob/master/docs/how-to-monitor-minio-with-prometheus.md)。







