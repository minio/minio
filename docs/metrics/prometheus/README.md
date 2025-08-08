# How to monitor MinIO server with Prometheus? [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

[Prometheus](https://prometheus.io) is a cloud-native monitoring platform. Prometheus offers a multi-dimensional data model with time series data identified by metric name and key/value pairs. The data collection happens via a pull model over HTTP/HTTPS. Users looking to monitor their MinIO instances can point Prometheus configuration to scrape data from following endpoints. 

- MinIO exports Prometheus compatible data by default as an authorized endpoint at `/minio/v2/metrics/cluster`. 
- MinIO exports Prometheus compatible data by default which is bucket centric as an authorized endpoint at `/minio/v2/metrics/bucket`.

This document explains how to setup Prometheus and configure it to scrape data from MinIO servers.

## Prerequisites

To get started with MinIO, refer [MinIO QuickStart Document](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html).
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

The Prometheus endpoint in MinIO requires authentication by default. Prometheus supports a bearer token approach to authenticate prometheus scrape requests, override the default Prometheus config with the one generated using mc. To generate a Prometheus config for an alias, use [mc](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart) as follows `mc admin prometheus generate <alias> [METRIC-TYPE]`. The valid values for METRIC-TYPE are `cluster`, `node`, `bucket` and `resource` and if not mentioned, it defaults to `cluster`.

The command will generate the `scrape_configs` section of the prometheus.yml as follows:

##### Cluster

```yaml
scrape_configs:
- job_name: minio-job
  bearer_token: <secret>
  metrics_path: /minio/v2/metrics/cluster
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

##### Bucket centric

```yaml
- job_name: minio-job-bucket
  bearer_token: <secret>
  metrics_path: /minio/v2/metrics/bucket
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

##### Node centric (optional)

```yaml
- job_name: minio-job-node
  bearer_token: <secret>
  metrics_path: /minio/v2/metrics/node
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

##### Resource centric (optional)

```yaml
- job_name: minio-job-resource
  bearer_token: <secret>
  metrics_path: /minio/v2/metrics/resource
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

#### Bucket centric

```yaml
scrape_configs:
- job_name: minio-job-bucket
  metrics_path: /minio/v2/metrics/bucket
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

##### Node (optional)

Optionally you can also collect per node metrics. This needs to be done on a per server instance.
The scrape configurations should use all the servers under `targets` so that graphing systems like
grafana can visualize them for all the nodes

```yaml
scrape_configs:
- job_name: minio-job
  metrics_path: /minio/v2/metrics/node
  scheme: http
  static_configs:
  - targets: ['server1:9000','server2:9000','server3:9000','server4:9000']
```

##### Resource (optional)

Optionally you can also collect resource metrics.

```yaml
scrape_configs:
- job_name: minio-job
  metrics_path: /minio/v2/metrics/resource
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

After Prometheus is configured, you can use Grafana to visualize MinIO metrics. Refer the [document here to setup Grafana with MinIO prometheus metrics](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/grafana/README.md).

## List of metrics exposed by MinIO

- MinIO exports Prometheus compatible data by default as an authorized endpoint at `/minio/v2/metrics/cluster`. 
- MinIO exports Prometheus compatible data by default which is bucket centric as an authorized endpoint at `/minio/v2/metrics/bucket`.
- MinIO exports Prometheus compatible data by default which is node centric as an authorized endpoint at `/minio/v2/metrics/node`.
- MinIO exports Prometheus compatible data by default which is resource centric as an authorized endpoint at `/minio/v2/metrics/resource`.

All of these can be accessed via Prometheus dashboard. A sample list of exposed metrics along with their definition is available on our public demo server at

```sh
curl https://play.min.io/minio/v2/metrics/cluster
```

### List of metrics reported Cluster and Bucket level

[The list of metrics reported can be here](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/list.md)

### Configure Alerts for Prometheus

[The Prometheus AlertManager and alerts can be configured following this](https://github.com/minio/minio/blob/master/docs/metrics/prometheus/alerts.md)
