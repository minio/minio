# How to monitor MinIO server with Prometheus [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

[Prometheus](https://prometheus.io) is a cloud-native monitoring platform.

Prometheus offers a multi-dimensional data model with time series data identified by metric name and key/value pairs. The data collection happens via a pull model over HTTP/HTTPS.

MinIO exports Prometheus compatible data by default as an authorized endpoint at `/minio/v2/metrics/cluster`. Users looking to monitor their MinIO instances can point Prometheus configuration to scrape data from this endpoint. This document explains how to setup Prometheus and configure it to scrape data from MinIO servers.

**Table of Contents**

- [Prerequisites](#prerequisites)
    - [1. Download Prometheus](#1-download-prometheus)
    - [2. Configure authentication type for Prometheus metrics](#2-configure-authentication-type-for-prometheus-metrics)
    - [3. Configuring Prometheus](#3-configuring-prometheus)
        - [3.1 Authenticated Prometheus config](#31-authenticated-prometheus-config)
        - [3.2 Public Prometheus config](#32-public-prometheus-config)
    - [4. Update `scrape_configs` section in prometheus.yml](#4-update-scrapeconfigs-section-in-prometheusyml)
    - [5. Start Prometheus](#5-start-prometheus)
    - [6. Configure Grafana](#6-configure-grafana)
- [List of metrics exposed by MinIO](#list-of-metrics-exposed-by-minio)

## Prerequisites
To get started with MinIO, refer [MinIO QuickStart Document](https://docs.min.io/docs/minio-quickstart-guide).
Follow below steps to get started with MinIO monitoring using Prometheus.

### 1. Download Prometheus

[Download the latest release](https://prometheus.io/download) of Prometheus for your platform, then extract it

```sh
tar xvfz prometheus-*.tar.gz
cd prometheus-*
```

Prometheus server is a single binary called `prometheus` (or `prometheus.exe` on Microsoft Windows). Run the binary and pass `--help` flag to see available options

```sh
./prometheus --help
usage: prometheus [<flags>]

The Prometheus monitoring server

. . .
```

Refer [Prometheus documentation](https://prometheus.io/docs/introduction/first_steps/) for more details.

### 2. Configure authentication type for Prometheus metrics

MinIO supports two authentication modes for Prometheus either `jwt` or `public`, by default MinIO runs in `jwt` mode. To allow public access without authentication for prometheus metrics set environment as follows.

```
export MINIO_PROMETHEUS_AUTH_TYPE="public"
minio server ~/test
```

### 3. Configuring Prometheus

#### 3.1 Authenticated Prometheus config

> If MinIO is configured to expose metrics without authentication, you don't need to use `mc` to generate prometheus config. You can skip reading further and move to 3.2 section.

The Prometheus endpoint in MinIO requires authentication by default. Prometheus supports a bearer token approach to authenticate prometheus scrape requests, override the default Prometheus config with the one generated using mc. To generate a Prometheus config for an alias, use [mc](https://docs.min.io/docs/minio-client-quickstart-guide) as follows `mc admin prometheus generate <alias>`.

The command will generate the `scrape_configs` section of the prometheus.yml as follows:

```yaml
scrape_configs:
- job_name: minio-job
  bearer_token: <secret>
  metrics_path: /minio/v2/metrics/cluster
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

#### 3.2 Public Prometheus config

If Prometheus endpoint authentication type is set to `public`. Following prometheus config is sufficient to start scraping metrics data from MinIO.
This can be collected from any server once per collection.

##### Cluster
```yaml
scrape_configs:
- job_name: minio-job
  metrics_path: /minio/v2/metrics/cluster
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

##### Node (optional)
Optionally you can also collect per node metrics. This needs to be done on a per server instance.
```yaml
scrape_configs:
- job_name: minio-job
  metrics_path: /minio/v2/metrics/node
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

### 4. Update `scrape_configs` section in prometheus.yml

To authorize every scrape request, copy and paste the generated `scrape_configs` section in the prometheus.yml and restart the Prometheus service.

### 5. Start Prometheus

Start (or) Restart Prometheus service by running

```sh
./prometheus --config.file=prometheus.yml
```

Here `prometheus.yml` is the name of configuration file. You can now see MinIO metrics in Prometheus dashboard. By default Prometheus dashboard is accessible at `http://localhost:9090`.

Prometheus sets the `Host` header to `domain:port` as part of HTTP operations against the MinIO metrics endpoint. For MinIO deployments behind a load balancer, reverse proxy, or other control plane (HAProxy, nginx, pfsense, opnsense, etc.), ensure the network service supports routing these requests to the deployment.

### 6. Configure Grafana

After Prometheus is configured, you can use Grafana to visualize MinIO metrics.
Refer the [document here to setup Grafana with MinIO prometheus metrics](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/grafana/README.md).

## List of metrics exposed by MinIO

MinIO server exposes the following metrics on `/minio/v2/metrics/cluster` endpoint. All of these can be accessed via Prometheus dashboard. A sample list of exposed metrics along with their definition is available in the demo server at

```sh
curl https://play.min.io/minio/v2/metrics/cluster
```

### List of metrics reported

[The list of metrics reported can be here](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/list.md)
