## Minio Prometheus Metric

Minio server exposes an endpoint for Promethueus to scrape server data at `/minio/metric`.

### Prometheus probe
Prometheus is used to monitor Minio server information like http request, disk storage, network stats etc.. It uses a config file named `prometheus.yaml` to scrape data from server. The value for `metrics_path` and `targets` need to be configured in the config yaml to specify the endpoint and url as shown:
```
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: minio
    metrics_path: /minio/prometheus/metrics

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
      - targets: ['localhost:9000']
```
 Prometheus can be run by executing :
```
./prometheus --config.file=prometheus.yml
```

### List of Minio metric exposed
Minio exposes the following list of metric to Prometheus
```
minio_disk_storage_bytes
minio_disk_storage_free_bytes
minio_http_requests_duration_seconds_bucket
minio_http_requests_duration_seconds_count
minio_http_requests_duration_seconds_sum
minio_http_requests_total
minio_network_received_bytes
minio_network_sent_bytes
minio_offline_disks_total
minio_online_disks_total
minio_server_uptime_seconds
```