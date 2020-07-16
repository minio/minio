# How to monitor MinIO server with Grafana [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

[Grafana](https://grafana.com/) allows you to query, visualize, alert on and understand your metrics no matter where they are stored. Create, explore, and share dashboards with your team and foster a data driven culture.

This document explains how to setup Grafana, to visualize MinIO metrics.

## Prerequisites

- Prometheus and MinIO configured as explained in [document here](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/README.md).
- Grafana installed as explained [here](https://grafana.com/grafana/download).

## MinIO Grafana Dashboard

To start visualizing MinIO metrics exposed by MinIO, you can use our Grafana dashboard. Import to dashboard the contents from the [file here](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/grafana/Minio-Overview-1594305200170.json).

![Grafana](https://raw.githubusercontent.com/minio/minio/master/docs/metrics/prometheus/grafana/grafana-minio.png)
