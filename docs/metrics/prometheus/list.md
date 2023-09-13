# List of metrics reported cluster wide

Each metric includes a label for the server that calculated the metric. Each metric has a label for the server that generated the metric.

These metrics can be obtained from any MinIO server once per collection.

| Name                                          | Description                                                                                                     |
|:----------------------------------------------|:----------------------------------------------------------------------------------------------------------------|
| `minio_audit_failed_messages`                 | Total number of messages that failed to send since start.                                                       |
| `minio_audit_target_queue_length`             | Number of unsent messages in queue for target.                                                                  |
| `minio_audit_total_messages`                  | Total number of messages sent since start.                                                                      |
| `minio_cache_hits_total`                      | Total number of drive cache hits.                                                                               |
| `minio_cache_missed_total`                    | Total number of drive cache misses.                                                                             |
| `minio_cache_sent_bytes`                      | Total number of bytes served from cache.                                                                        |
| `minio_cache_total_bytes`                     | Total size of cache drive in bytes.                                                                             |
| `minio_cache_usage_info`                      | Total percentage cache usage, value of 1 indicates high and 0 low, label level is set as well.                  |
| `minio_cache_used_bytes`                      | Current cache usage in bytes.                                                                                   |
| `minio_cluster_capacity_raw_free_bytes`       | Total free capacity online in the cluster.                                                                      |
| `minio_cluster_capacity_raw_total_bytes`      | Total capacity online in the cluster.                                                                           |
| `minio_cluster_capacity_usable_free_bytes`    | Total free usable capacity online in the cluster.                                                               |
| `minio_cluster_capacity_usable_total_bytes`   | Total usable capacity online in the cluster.                                                                    |
| `minio_cluster_objects_size_distribution`     | Distribution of object sizes across a cluster                                                                   |
| `minio_cluster_objects_version_distribution`  | Distribution of object sizes across a cluster                                                                   |
| `minio_cluster_usage_object_total`            | Total number of objects in a cluster                                                                            |
| `minio_cluster_usage_total_bytes`             | Total cluster usage in bytes                                                                                    |
| `minio_cluster_usage_version_total`           | Total number of versions (includes delete marker) in a cluster                                                  |
| `minio_cluster_usage_deletemarker_total`      | Total number of delete markers in a cluster                                                                     |
| `minio_cluster_usage_total_bytes`             | Total cluster usage in bytes                                                                                    |
| `minio_cluster_buckets_total`                 | Total number of buckets in the cluster                                                                          |
| `minio_cluster_drive_offline_total`           | Total drives offline in this cluster.                                                                           |
| `minio_cluster_drive_online_total`            | Total drives online in this cluster.                                                                            |
| `minio_cluster_drive_total`                   | Total drives in this cluster.                                                                                   |
| `minio_cluster_ilm_transitioned_bytes`        | Total bytes transitioned to a tier.                                                                             |
| `minio_cluster_ilm_transitioned_objects`      | Total number of objects transitioned to a tier.                                                                 |
| `minio_cluster_ilm_transitioned_versions`     | Total number of versions transitioned to a tier.                                                                |
| `minio_cluster_kms_online`                    | Reports whether the KMS is online (1) or offline (0).                                                           |
| `minio_cluster_kms_request_error`             | Number of KMS requests that failed due to some error. (HTTP 4xx status code).                                   |
| `minio_cluster_kms_request_failure`           | Number of KMS requests that failed due to some internal failure. (HTTP 5xx status code).                        |
| `minio_cluster_kms_request_success`           | Number of KMS requests that succeeded.                                                                          |
| `minio_cluster_kms_uptime`                    | The time the KMS has been up and running in seconds.                                                            |
| `minio_cluster_nodes_offline_total`           | Total number of MinIO nodes offline.                                                                            |
| `minio_cluster_nodes_online_total`            | Total number of MinIO nodes online.                                                                             |
| `minio_cluster_write_quorum`                  | Maximum write quorum across all pools and sets                                                                  |
| `minio_cluster_health_status`                 | Get current cluster health status                                                                               |
| `minio_heal_objects_errors_total`             | Objects for which healing failed in current self healing run.                                                   |
| `minio_heal_objects_heal_total`               | Objects healed in current self healing run.                                                                     |
| `minio_heal_objects_total`                    | Objects scanned in current self healing run.                                                                    |
| `minio_heal_time_last_activity_nano_seconds`  | Time elapsed (in nano seconds) since last self healing activity.                                                |
| `minio_inter_node_traffic_dial_avg_time`      | Average time of internodes TCP dial calls.                                                                      |
| `minio_inter_node_traffic_dial_errors`        | Total number of internode TCP dial timeouts and errors.                                                         |
| `minio_inter_node_traffic_errors_total`       | Total number of failed internode calls.                                                                         |
| `minio_inter_node_traffic_received_bytes`     | Total number of bytes received from other peer nodes.                                                           |
| `minio_inter_node_traffic_sent_bytes`         | Total number of bytes sent to the other peer nodes.                                                             |
| `minio_notify_current_send_in_progress`       | Number of concurrent async Send calls active to all targets.                                                    |
| `minio_notify_target_queue_length`            | Number of unsent notifications in queue for target.                                                             |
| `minio_s3_requests_4xx_errors_total`          | Total number S3 requests with (4xx) errors.                                                                     |
| `minio_s3_requests_5xx_errors_total`          | Total number S3 requests with (5xx) errors.                                                                     |
| `minio_s3_requests_canceled_total`            | Total number S3 requests canceled by the client.                                                                |
| `minio_s3_requests_errors_total`              | Total number S3 requests with (4xx and 5xx) errors.                                                             |
| `minio_s3_requests_incoming_total`            | Volatile number of total incoming S3 requests.                                                                  |
| `minio_s3_requests_inflight_total`            | Total number of S3 requests currently in flight.                                                                |
| `minio_s3_requests_rejected_auth_total`       | Total number S3 requests rejected for auth failure.                                                             |
| `minio_s3_requests_rejected_header_total`     | Total number S3 requests rejected for invalid header.                                                           |
| `minio_s3_requests_rejected_invalid_total`    | Total number S3 invalid requests.                                                                               |
| `minio_s3_requests_rejected_timestamp_total`  | Total number S3 requests rejected for invalid timestamp.                                                        |
| `minio_s3_requests_total`                     | Total number S3 requests.                                                                                       |
| `minio_s3_requests_waiting_total`             | Number of S3 requests in the waiting queue.                                                                     |
| `minio_s3_requests_ttfb_seconds_distribution` | Distribution of the time to first byte across API calls.                                                        |
| `minio_s3_traffic_received_bytes`             | Total number of s3 bytes received.                                                                              |
| `minio_s3_traffic_sent_bytes`                 | Total number of s3 bytes sent.                                                                                  |
| `minio_software_commit_info`                  | Git commit hash for the MinIO release.                                                                          |
| `minio_software_version_info`                 | MinIO Release tag for the server.                                                                               |
| `minio_usage_last_activity_nano_seconds`      | Time elapsed (in nano seconds) since last scan activity.                                                        |
| `minio_node_drive_free_bytes`                 | Total storage available on a drive.                                                                             |
| `minio_node_drive_free_inodes`                | Total free inodes.                                                                                              |
| `minio_node_drive_latency_us`                 | Average last minute latency in µs for drive API storage operations.                                             |
| `minio_node_drive_offline_total`              | Total drives offline in this node.                                                                              |
| `minio_node_drive_online_total`               | Total drives online in this node.                                                                               |
| `minio_node_drive_total`                      | Total drives in this node.                                                                                      |
| `minio_node_drive_total_bytes`                | Total storage on a drive.                                                                                       |
| `minio_node_drive_used_bytes`                 | Total storage used on a drive.                                                                                  |
| `minio_node_file_descriptor_limit_total`      | Limit on total number of open file descriptors for the MinIO Server process.                                    |
| `minio_node_file_descriptor_open_total`       | Total number of open file descriptors by the MinIO Server process.                                              |
| `minio_node_go_routine_total`                 | Total number of go routines running.                                                                            |
| `minio_node_iam_last_sync_duration_millis`    | Last successful IAM data sync duration in milliseconds.                                                         |
| `minio_node_iam_since_last_sync_millis`       | Time (in milliseconds) since last successful IAM data sync.                                                     |
| `minio_node_iam_sync_failures`                | Number of failed IAM data syncs since server start.                                                             |
| `minio_node_iam_sync_successes`               | Number of successful IAM data syncs since server start.                                                         |
| `minio_node_ilm_expiry_pending_tasks`         | Number of pending ILM expiry tasks in the queue.                                                                |
| `minio_node_ilm_transition_active_tasks`      | Number of active ILM transition tasks.                                                                          |
| `minio_node_ilm_transition_pending_tasks`     | Number of pending ILM transition tasks in the queue.                                                            |
| `minio_node_ilm_versions_scanned`             | Total number of object versions checked for ilm actions since server start.                                     |
| `minio_node_io_rchar_bytes`                   | Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar.       |
| `minio_node_io_read_bytes`                    | Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes.                  |
| `minio_node_io_wchar_bytes`                   | Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar. |
| `minio_node_io_write_bytes`                   | Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes.                |
| `minio_node_process_cpu_total_seconds`        | Total user and system CPU time spent in seconds.                                                                |
| `minio_node_process_resident_memory_bytes`    | Resident memory size in bytes.                                                                                  |
| `minio_node_process_starttime_seconds`        | Start time for MinIO process per node, time in seconds since Unix epoc.                                         |
| `minio_node_process_uptime_seconds`           | Uptime for MinIO process per node in seconds.                                                                   |
| `minio_node_scanner_bucket_scans_finished`    | Total number of bucket scans finished since server start.                                                       |
| `minio_node_scanner_bucket_scans_started`     | Total number of bucket scans started since server start.                                                        |
| `minio_node_scanner_directories_scanned`      | Total number of directories scanned since server start.                                                         |
| `minio_node_scanner_objects_scanned`          | Total number of unique objects scanned since server start.                                                      |
| `minio_node_scanner_versions_scanned`         | Total number of object versions scanned since server start.                                                     |
| `minio_node_syscall_read_total`               | Total read SysCalls to the kernel. /proc/[pid]/io syscr.                                                        |
| `minio_node_syscall_write_total`              | Total write SysCalls to the kernel. /proc/[pid]/io syscw.                                                       |

### List of replication metrics reported cluster wide
The replication metrics marked with * are only relevant for site replication, where metrics are published at the cluster level and not at bucket level. If bucket 
replication is in use, these metrics are exported at the bucket level.
The remaining unstarred metrics are node level metrics and are exported at the cluster level with a variable label for node.

| Name                                          | Description                                                                                                     |
|:----------------------------------------------|:----------------------------------------------------------------------------------------------------------------|
| `minio_cluster_replication_current_active_workers`                 | Total number of active replication workers                                                 |
| `minio_cluster_replication_average_active_workers`                 | Average number of active replication workers                                               |
| `minio_cluster_replication_max_active_workers`                 | Maximum number of active replication workers seen since server start                           |
| `minio_cluster_replication_link_online`       | Reports whether the replication link is online (1) or offline (0).                                              |
| `minio_cluster_replication_link_offline_duration_seconds` | Total duration of replication link being offline in seconds since last offline event|
| `minio_cluster_replication_link_downtime_duration_seconds` | Total downtime of replication link in seconds since server start|
| `minio_cluster_replication_average_link_latency_ms` | Average replication link latency in milliseconds                                                          |
| `minio_cluster_replication_max_link_latency_ms`     | Maximum replication link latency in milliseconds seen since server start                                  |
| `minio_cluster_replication_current_link_latency_ms` | Current replication link latency in milliseconds                                                          |
| `minio_cluster_replication_current_transfer_rate` | Current replication transfer rate in bytes/sec                                                              |
| `minio_cluster_replication_average_transfer_rate` | Average replication transfer rate in bytes/sec                                                              |
| `minio_cluster_replication_max_transfer_rate`     | Maximum replication transfer rate in bytes/sec seen since server start                                      |
| `minio_cluster_replication_last_minute_queued_count` | Total number of objects queued for replication in the last full minute                                   |
| `minio_cluster_replication_last_minute_queued_bytes` | Total number of bytes queued for replication in the last full minute                                     |
| `minio_cluster_replication_average_queued_count` | Average number of objects queued for replication since server start                                          |
| `minio_cluster_replication_average_queued_bytes` | Average number of bytes queued for replication since server start                                            |
| `minio_cluster_replication_max_queued_bytes`  | Maximum number of bytes queued for replication seen since server start                                          |
| `minio_cluster_replication_max_queued_count`  | Maximum number of objects queued for replication seen since server start                                        |
| `minio_cluster_replication_recent_backlog_count` | Total number of objects seen in replication backlog in the last 5 minutes                                    |
| `minio_cluster_replication_last_minute_failed_bytes`           | Total number of bytes failed at least once to replicate in the last full minute.                        |
| `minio_cluster_replication_last_minute_failed_count`           | Total number of objects which failed replication in the last full minute.                               |
| `minio_cluster_replication_last_hour_failed_bytes` | * Total number of bytes failed at least once to replicate in the last full hour.                          |
| `minio_cluster_replication_last_hour_failed_count` | * Total number of objects which failed replication in the last full hour.                                 |
| `minio_cluster_replication_total_failed_bytes`     | * Total number of bytes failed at least once to replicate since server start.                            |
| `minio_cluster_replication_total_failed_count`     | * Total number of objects which failed replication since server start.                                   |
| `minio_cluster_replication_received_bytes`         | * Total number of bytes replicated to this cluster from another source cluster.     |
| `minio_cluster_replication_received_count`         | * Total number of objects received by this cluster from another source cluster.     |
| `minio_cluster_replication_sent_bytes`             | * Total number of bytes replicated to the target cluster. |                         |
| `minio_cluster_replication_sent_count`             | * Total number of objects replicated to the target cluster.  |                        |
| `minio_cluster_replication_credential_errors`      | * Total number of replication credential errors since server start                |

# List of metrics exported per bucket level

Each metric includes a label for the server that calculated the metric. Each metric has a label for the server that generated the metric. Each
metric has a label that distinguishes the bucket.

These metrics can be obtained from any MinIO server once per collection.

| Name                                              | Description                                                                     |
|:--------------------------------------------------|:--------------------------------------------------------------------------------|
| `minio_bucket_objects_size_distribution`          | Distribution of object sizes in the bucket, includes label for the bucket name. |
| `minio_bucket_objects_version_distribution`       | Distribution of object sizes in a bucket, by number of versions                 |
| `minio_bucket_quota_total_bytes`                  | Total bucket quota size in bytes.                                               |
| `minio_bucket_replication_last_minute_failed_bytes`           | Total number of bytes failed at least once to replicate in the last full minute.                        |
| `minio_bucket_replication_last_minute_failed_count`           | Total number of objects which failed replication in the last full minute.                               |
| `minio_bucket_replication_last_hour_failed_bytes` | Total number of bytes failed at least once to replicate in the last full hour.                          |
| `minio_bucket_replication_last_hour_failed_count` | Total number of objects which failed replication in the last full hour.                                 |
| `minio_bucket_replication_total_failed_bytes`     | Total number of bytes failed at least once to replicate since server start.                            |
| `minio_bucket_replication_total_failed_count`     | Total number of objects which failed replication since server start.                                   |
| `minio_bucket_replication_latency_ms`             | Replication latency in milliseconds.                                            |
| `minio_bucket_replication_received_bytes`         | Total number of bytes replicated to this bucket from another source bucket.     |
| `minio_bucket_replication_received_count`         | Total number of objects received by this bucket from another source bucket.     |
| `minio_bucket_replication_sent_bytes`             | Total number of bytes replicated to the target bucket. |                         |
| `minio_bucket_replication_sent_count`             | Total number of objects replicated to the target bucket.  |                        |
| `minio_bucket_traffic_received_bytes`             | Total number of S3 bytes received for this bucket.                              |
| `minio_bucket_traffic_sent_bytes`                 | Total number of S3 bytes sent for this bucket.                                  |
| `minio_bucket_usage_object_total`                 | Total number of objects.                                                        |
| `minio_bucket_usage_version_total`                | Total number of versions (includes delete marker)                               |
| `minio_bucket_usage_deletemarker_total`           | Total number of delete markers.                                                 |
| `minio_bucket_usage_total_bytes`                  | Total bucket size in bytes.                                                     |
| `minio_bucket_requests_4xx_errors_total`          | Total number of S3 requests with (4xx) errors on a bucket.                      |
| `minio_bucket_requests_5xx_errors_total`          | Total number of S3 requests with (5xx) errors on a bucket.                      |
| `minio_bucket_requests_inflight_total`            | Total number of S3 requests currently in flight on a bucket.                    |
| `minio_bucket_requests_total`                     | Total number of S3 requests on a bucket.                                        |
| `minio_bucket_requests_canceled_total`            | Total number S3 requests canceled by the client.                                |
| `minio_bucket_requests_ttfb_seconds_distribution` | Distribution of time to first byte across API calls per bucket.                 |
| `minio_bucket_replication_credential_errors`      | Total number of replication credential errors since server start                |
