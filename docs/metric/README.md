## Minio Prometheus Metric

Minio server exposes an endpoint for Promethueus to scrape server data at `/minio/prometheus/metric`.

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
- `minio_disk_storage_bytes` : Total byte count of disk storage available to current Minio server instance
- `minio_disk_storage_free_bytes` : Total byte count of free disk storage available to current Minio server instance
- `minio_http_requests_duration_seconds_bucket` : The bucket into which observations are counted for creating Histogram
- `minio_http_requests_duration_seconds_count` : The count of current number of observations i.e. total HTTP requests (HEAD/GET/PUT/POST/DELETE).
- `minio_http_requests_duration_seconds_sum` : The current aggregate time spent servicing all HTTP requests (HEAD/GET/PUT/POST/DELETE) in seconds
- `minio_http_requests_total` : Total number of requests served by current Minio server instance
- `minio_network_received_bytes_total` : Total number of bytes received by current Minio server instance
- `minio_network_sent_bytes_total` : Total number of bytes sent by current Minio server instance
- `minio_offline_disks` : Total number of offline disks for current Minio server instance
- `minio_total_disks` : Total number of disks for current Minio server instance
- `minio_server_start_time_seconds` : Time Unix time in seconds when current Minio server instance started