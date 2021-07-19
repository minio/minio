# List of metrics reported cluster wide

Each metric includes a label for the server that calculated the metric.
Each metric has a label for the server that generated the metric.

These metrics can be from any MinIO server once per collection.

| Name                                         | Description                                                                                                         |
|:---------------------------------------------|:--------------------------------------------------------------------------------------------------------------------|
| `minio_bucket_objects_size_distribution`     | Distribution of object sizes in the bucket, includes label for the bucket name.                                     |
| `minio_bucket_replication_failed_bytes`      | Total number of bytes failed at least once to replicate.                                                            |
| `minio_bucket_replication_received_bytes`    | Total number of bytes replicated to this bucket from another source bucket.                                         |
| `minio_bucket_replication_sent_bytes`        | Total number of bytes replicated to the target bucket.                                                              |
| `minio_bucket_replication_failed_count`      | Total number of replication foperations failed for this bucket.                                                     |
| `minio_bucket_usage_object_total`            | Total number of objects                                                                                             |
| `minio_bucket_usage_total_bytes`             | Total bucket size in bytes                                                                                          |
| `minio_cache_hits_total`                     | Total number of disk cache hits                                                                                     |
| `minio_cache_missed_total`                   | Total number of disk cache misses                                                                                   |
| `minio_cache_sent_bytes`                     | Total number of bytes served from cache                                                                             |
| `minio_cache_total_bytes`                    | Total size of cache disk in bytes                                                                                   |
| `minio_cache_usage_info`                     | Total percentage cache usage, value of 1 indicates high and 0 low, label level is set as well                       |
| `minio_cache_used_bytes`                     | Current cache usage in bytes                                                                                        |
| `minio_cluster_capacity_raw_free_bytes`      | Total free capacity online in the cluster.                                                                          |
| `minio_cluster_capacity_raw_total_bytes`     | Total capacity online in the cluster.                                                                               |
| `minio_cluster_capacity_usable_free_bytes`   | Total free usable capacity online in the cluster.                                                                   |
| `minio_cluster_capacity_usable_total_bytes`  | Total usable capacity online in the cluster.                                                                        |
| `minio_cluster_nodes_offline_total`          | Total number of MinIO nodes offline.                                                                                |
| `minio_cluster_nodes_online_total`           | Total number of MinIO nodes online.                                                                                 |
| `minio_heal_objects_error_total`             | Objects for which healing failed in current self healing run                                                        |
| `minio_heal_objects_heal_total`              | Objects healed in current self healing run                                                                          |
| `minio_heal_objects_total`                   | Objects scanned in current self healing run                                                                         |
| `minio_heal_time_last_activity_nano_seconds` | Time elapsed (in nano seconds) since last self healing activity. This is set to -1 until initial self heal activity |
| `minio_inter_node_traffic_received_bytes`    | Total number of bytes received from other peer nodes.                                                               |
| `minio_inter_node_traffic_sent_bytes`        | Total number of bytes sent to the other peer nodes.                                                                 |
| `minio_node_disk_free_bytes`                 | Total storage available on a disk.                                                                                  |
| `minio_node_disk_total_bytes`                | Total storage on a disk.                                                                                            |
| `minio_node_disk_used_bytes`                 | Total storage used on a disk.                                                                                       |
| `minio_node_file_descriptor_limit_total`     | Limit on total number of open file descriptors for the MinIO Server process.                                        |
| `minio_node_file_descriptor_open_total`      | Total number of open file descriptors by the MinIO Server process.                                                  |
| `minio_node_io_rchar_bytes`                  | Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar            |
| `minio_node_io_read_bytes`                   | Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes                       |
| `minio_node_io_wchar_bytes`                  | Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar      |
| `minio_node_io_write_bytes`                  | Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes                     |
| `minio_node_process_starttime_seconds`       | Start time for MinIO process per node, time in seconds since Unix epoc.                                             |
| `minio_node_process_uptime_seconds`          | Uptime for MinIO process per node in seconds.                                                                       |
| `minio_node_syscall_read_total`              | Total read SysCalls to the kernel. /proc/[pid]/io syscr                                                             |
| `minio_node_syscall_write_total`             | Total write SysCalls to the kernel. /proc/[pid]/io syscw                                                            |
| `minio_s3_requests_error_total`              | Total number S3 requests with errors                                                                                |
| `minio_s3_requests_inflight_total`           | Total number of S3 requests currently in flight                                                                     |
| `minio_s3_requests_total`                    | Total number S3 requests                                                                                            |
| `minio_s3_time_ttbf_seconds_distribution`    | Distribution of the time to first byte across API calls.                                                            |
| `minio_s3_traffic_received_bytes`            | Total number of s3 bytes received.                                                                                  |
| `minio_s3_traffic_sent_bytes`                | Total number of s3 bytes sent                                                                                       |
| `minio_software_commit_info`                 | Git commit hash for the MinIO release.                                                                              |
| `minio_software_version_info`                | MinIO Release tag for the server                                                                                    |
