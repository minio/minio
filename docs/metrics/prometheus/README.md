# How to monitor MinIO server with Prometheus [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

[Prometheus](https://prometheus.io) is a cloud-native monitoring platform, built originally at SoundCloud. Prometheus offers a multi-dimensional data model with time series data identified by metric name and key/value pairs. The data collection happens via a pull model over HTTP/HTTPS. Targets to pull data from are discovered via service discovery or static configuration.

MinIO exports Prometheus compatible data by default as an authorized endpoint at `/minio/prometheus/metrics`. Users looking to monitor their MinIO instances can point Prometheus configuration to scrape data from this endpoint.

This document explains how to setup Prometheus and configure it to scrape data from MinIO servers.

**Table of Contents**

- [Prerequisites](#prerequisites)
    - [1. Download Prometheus](#1-download-prometheus)
    - [2. Configure authentication type for Prometheus metrics](#2-configure-authentication-type-for-prometheus-metrics)
    - [3. Configuring Prometheus](#3-configuring-prometheus)
        - [3.1 Authenticated Prometheus config](#31-authenticated-prometheus-config)
        - [3.2 Public Prometheus config](#32-public-prometheus-config)
    - [4. Update `scrape_configs` section in prometheus.yml](#4-update-scrapeconfigs-section-in-prometheusyml)
    - [5. Start Prometheus](#5-start-prometheus)
- [List of metrics exposed by MinIO](#list-of-metrics-exposed-by-minio)

## Prerequisites
To get started with MinIO, refer [MinIO QuickStart Document](https://docs.min.io/docs/minio-quickstart-guide). Follow below steps to get started with MinIO monitoring using Prometheus.

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
  metrics_path: /minio/prometheus/metrics
  scheme: http
  static_configs:
  - targets: ['localhost:9000']
```

#### 3.2 Public Prometheus config

If Prometheus endpoint authentication type is set to `public`. Following prometheus config is sufficient to start scraping metrics data from MinIO.

```yaml
scrape_configs:
- job_name: minio-job
  metrics_path: /minio/prometheus/metrics
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

## List of metrics exposed by MinIO

MinIO server exposes the following metrics on `/minio/prometheus/metrics` endpoint. All of these can be accessed via Prometheus dashboard. The full list of exposed metrics along with their definition is available in the demo server at https://play.min.io:9000/minio/prometheus/metrics

These are the new set of metrics which will be in effect after `RELEASE.2019-10-16*`. Some of the key changes in this update are listed below.
    - Metrics are bound the respective nodes and is not cluster-wide. Each and every node in a cluster will expose its own metrics.
    - Additional metrics to cover the s3 and internode traffic statistics were added.
    - Metrics that records the http statistics and latencies are labeled to their respective APIs (putobject,getobject etc).
    - Disk usage metrics are distributed and labeled to the respective disk paths.

For more details, please check the `Migration guide for the new set of metrics`.

The list of metrics and its definition are as follows. (NOTE: instance here is one MinIO node)

> NOTES:
    > 1. Instance here is one MinIO node.
    > 2. `s3 requests` exclude internode requests.

### Default set of information
| name        | description                     |
|:------------|:--------------------------------|
| `go_`       | all standard go runtime metrics |
| `process_`  | all process level metrics       |
| `promhttp_` | all prometheus scrape metrics   |

### MinIO node specific information
| name                       | description                                                                    |
|:---------------------------|:-------------------------------------------------------------------------------|
| `minio_version_info`       | Current MinIO version with its commit-id                                       |
| `minio_disks_offline`      | Total number of offline disks on current MinIO instance                        |
| `minio_disks_total`        | Total number of disks on current MinIO instance                                |

### Disk metrics are labeled by 'disk' which indentifies each disk
| name                       | description                                                                    |
|:---------------------------|:-------------------------------------------------------------------------------|
| `disk_storage_total`       | Total size of the disk                                                         |
| `disk_storage_used`        | Total disk space used per disk                                                 |
| `disk_storage_available`   | Total available disk space per disk                                            |

### S3 API metrics are labeled by 'api' which identifies different S3 API requests
| name                       | description                                                                    |
|:---------------------------|:-------------------------------------------------------------------------------|
| `s3_requests_total`        | Total number of s3 requests in current MinIO instance                          |
| `s3_errors_total`          | Total number of errors in s3 requests in current MinIO instance                |
| `s3_requests_current`      | Total number of active s3 requests in current MinIO instance                   |
| `s3_rx_bytes_total`        | Total number of s3 bytes received by current MinIO server instance             |
| `s3_tx_bytes_total`        | Total number of s3 bytes sent by current MinIO server instance                 |
| `s3_ttfb_seconds`          | Histogram that holds the latency information of the requests                   |

#### Internode metrics only available in a distributed setup
| name                       | description                                                                    |
|:---------------------------|:-------------------------------------------------------------------------------|
| `internode_rx_bytes_total` | Total number of internode bytes received by current MinIO server instance      |
| `internode_tx_bytes_total` | Total number of bytes sent to the other nodes by current MinIO server instance |

Apart from above metrics, MinIO also exposes below mode specific metrics

### Bucket usage specific metrics
All metrics are labeled by `bucket`, each metric is displayed per bucket. `buckets_objects_histogram` is additionally labeled by `object_size` string which is represented by any of the following values

- *LESS_THAN_1024_B*
- *BETWEEN_1024_B_AND_1_MB*
- *BETWEEN_1_MB_AND_10_MB*
- *BETWEEN_10_MB_AND_64_MB*
- *BETWEEN_64_MB_AND_128_MB*
- *BETWEEN_128_MB_AND_512_MB*
- *GREATER_THAN_512_MB*

| name                       | description                                         |
|:---------------------------|:----------------------------------------------------|
| `bucket_usage_size`        | Total size of the bucket                            |
| `bucket_objects_count`     | Total number of objects in a bucket                 |
| `bucket_objects_histogram` | Total number of objects filtered by different sizes |

### Cache specific metrics

MinIO Gateway instances enabled with Disk-Caching expose caching related metrics.

#### Global cache metrics
| name                 | description                                       |
|:---------------------|:--------------------------------------------------|
| `cache_hits_total`   | Total number of cache hits                        |
| `cache_misses_total` | Total number of cache misses                      |
| `cache_data_served`  | Total number of bytes served from cache           |

#### Per disk cache metrics
| `cache_usage_percent` | Total percentage cache usage                                                     |
| `cache_usage_state`   | Indicates cache usage is high or low, relative to current cache 'quota' settings |

`cache_usage_state` holds only two states

- '1' indicates high disk usage
- '0' indicates low disk usage

### Gateway specific metrics
MinIO Gateway instance exposes metrics related to Gateway communication with the cloud backend (S3, Azure & GCS Gateway).

`<gateway_type>` changes based on the gateway in use can be 's3', 'gcs' or 'azure'. Other metrics are labeled with `method` that identifies HTTP GET, HEAD, PUT and POST requests to the backend.

| name                                    | description                                                                |
|:----------------------------------------|:---------------------------------------------------------------------------|
| `gateway_<gateway_type>_requests`       | Total number of requests made to the gateway backend                       |
| `gateway_<gateway_type>_bytes_sent`     | Total number of bytes sent to cloud backend (in PUT & POST Requests)       |
| `gateway_<gateway_type>_bytes_received` | Total number of bytes received from cloud backend (in GET & HEAD Requests) |

Note that this is currently only support for Azure, S3 and GCS Gateway.

### MinIO self-healing metrics - `self_heal_*`

MinIO exposes self-healing related metrics for erasure-code deployments _only_. These metrics are _not_ available on Gateway or Single Node, Single Drive deployments. Note that these metrics will be exposed _only_ when there is a relevant event happening on MinIO server.

| name                                 | description                                                                                                                                                                 |
|:-------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `self_heal_time_since_last_activity` | Time elapsed since last self-healing related activity                                                                                                                       |
| `self_heal_objects_scanned`          | Number of objects scanned by self-healing thread in its current run. This will reset when a fresh self-healing run starts. This is labeled with the object type scanned     |
| `self_heal_objects_healed`           | Number of objects healing by self-healing thread in its current run. This will reset when a fresh self-healing run starts. This is labeled with the object type scanned     |
| `self_heal_objects_heal_failed`      | Number of objects for which self-healing failed in its current run. This will reset when a fresh self-healing run starts. This is labeled with disk status and its endpoint |

## Migration guide for the new set of metrics

This migration guide applies for older releases or any releases before `RELEASE.2019-10-23*`

### MinIO disk level metrics - `disk_*`

The migrations include

- `minio_total_disks` to `minio_disks_total`
- `minio_offline_disks` to `minio_disks_offline`

### MinIO disk level metrics - `disk_storage_*`

These metrics have one label.

- `disk`: Holds the disk path

The migrations include

- `minio_disk_storage_used_bytes` to `disk_storage_used`
- `minio_disk_storage_available_bytes` to `disk_storage_available`
- `minio_disk_storage_total_bytes` to `disk_storage_total`

### MinIO network level metrics

These metrics are detailed to cover the s3 and internode network statistics.

The migrations include

- `minio_network_sent_bytes_total` to `s3_tx_bytes_total` and `internode_tx_bytes_total`
- `minio_network_received_bytes_total` to `s3_rx_bytes_total` and `internode_rx_bytes_total`

Some of the additional metrics added were

- `s3_requests_total`
- `s3_errors_total`
- `s3_ttfb_seconds`
