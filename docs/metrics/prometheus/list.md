# Cluster Metrics

MinIO collects the following metrics at the cluster level.
Metrics may include one or more labels, such as the server that calculated that metric.

These metrics can be obtained from any MinIO server once per collection by using the following URL:

```shell
https://HOSTNAME:PORT/minio/v2/metrics/cluster
```

Replace ``HOSTNAME:PORT`` with the hostname of your MinIO deployment.
For deployments behind a load balancer, use the load balancer hostname instead of a single node hostname.

## Audit Metrics

| Name                              | Description                                               |
|:----------------------------------|:----------------------------------------------------------|
| `minio_audit_failed_messages`     | Total number of messages that failed to send since start. |
| `minio_audit_target_queue_length` | Number of unsent messages in queue for target.            |
| `minio_audit_total_messages`      | Total number of messages sent since start.                |

## Cluster Capacity Metrics

| Name                                         | Description                                                    |
|:---------------------------------------------|:---------------------------------------------------------------|
| `minio_cluster_capacity_raw_free_bytes`      | Total free capacity online in the cluster.                     |
| `minio_cluster_capacity_raw_total_bytes`     | Total capacity online in the cluster.                          |
| `minio_cluster_capacity_usable_free_bytes`   | Total free usable capacity online in the cluster.              |
| `minio_cluster_capacity_usable_total_bytes`  | Total usable capacity online in the cluster.                   |
| `minio_cluster_objects_size_distribution`    | Distribution of object sizes across a cluster                  |
| `minio_cluster_objects_version_distribution` | Distribution of object versions across a cluster               |
| `minio_cluster_usage_object_total`           | Total number of objects in a cluster                           |
| `minio_cluster_usage_total_bytes`            | Total cluster usage in bytes                                   |
| `minio_cluster_usage_version_total`          | Total number of versions (includes delete marker) in a cluster |
| `minio_cluster_usage_deletemarker_total`     | Total number of delete markers in a cluster                    |
| `minio_cluster_bucket_total`                 | Total number of buckets in the cluster                         |

## Cluster Drive Metrics

| Name                                | Description                           |
|:------------------------------------|:--------------------------------------|
| `minio_cluster_drive_offline_total` | Total drives offline in this cluster. |
| `minio_cluster_drive_online_total`  | Total drives online in this cluster.  |
| `minio_cluster_drive_total`         | Total drives in this cluster.         |

## Cluster ILM Metrics

| Name                                      | Description                                      |
|:------------------------------------------|:-------------------------------------------------|
| `minio_cluster_ilm_transitioned_bytes`    | Total bytes transitioned to a tier.              |
| `minio_cluster_ilm_transitioned_objects`  | Total number of objects transitioned to a tier.  |
| `minio_cluster_ilm_transitioned_versions` | Total number of versions transitioned to a tier. |

## Cluster KMS Metrics

| Name                                | Description                                                                              |
|:------------------------------------|:-----------------------------------------------------------------------------------------|
| `minio_cluster_kms_online`          | Reports whether the KMS is online (1) or offline (0).                                    |
| `minio_cluster_kms_request_error`   | Number of KMS requests that failed due to some error. (HTTP 4xx status code).            |
| `minio_cluster_kms_request_failure` | Number of KMS requests that failed due to some internal failure. (HTTP 5xx status code). |
| `minio_cluster_kms_request_success` | Number of KMS requests that succeeded.                                                   |
| `minio_cluster_kms_uptime`          | The time the KMS has been up and running in seconds.                                     |

## Cluster Health Metrics

| Name                                              | Description                                    |
|:--------------------------------------------------|:-----------------------------------------------|
| `minio_cluster_nodes_offline_total`               | Total number of MinIO nodes offline.           |
| `minio_cluster_nodes_online_total`                | Total number of MinIO nodes online.            |
| `minio_cluster_write_quorum`                      | Maximum write quorum across all pools and sets |
| `minio_cluster_health_status`                     | Get current cluster health status              |
| `minio_cluster_health_erasure_set_healing_drives` | Count of healing drives in the erasure set     |
| `minio_cluster_health_erasure_set_online_drives`  | Count of online drives in the erasure set      |
| `minio_cluster_health_erasure_set_read_quorum`    | Get read quorum of the erasure set             |
| `minio_cluster_health_erasure_set_write_quorum`   | Get write quorum of the erasure set            |
| `minio_cluster_health_erasure_set_status`         | Get current health status of the erasure set   |

## Cluster Replication Metrics

Metrics marked as ``Site Replication Only`` only populate on deployments with [Site Replication](https://docs.min.io/community/minio-object-store/operations/install-deploy-manage/multi-site-replication.html) configurations.
For deployments with [bucket](https://docs.min.io/community/minio-object-store/administration/bucket-replication.html) or [batch](https://docs.min.io/community/minio-object-store/administration/batch-framework.html#replicate) configurations, these metrics populate instead under the [Bucket Metrics](#bucket-metrics) endpoint.

| Name                                                       | Description                                                                                             
|:-----------------------------------------------------------|:---------------------------------------------------------------------------------------------------------|
| `minio_cluster_replication_last_hour_failed_bytes`         | (_Site Replication Only_) Total number of bytes failed at least once to replicate in the last full hour. |
| `minio_cluster_replication_last_hour_failed_count`         | (_Site Replication Only_) Total number of objects which failed replication in the last full hour.        |
| `minio_cluster_replication_last_minute_failed_bytes`       | Total number of bytes failed at least once to replicate in the last full minute.                         |
| `minio_cluster_replication_last_minute_failed_count`       | Total number of objects which failed replication in the last full minute.                                |
| `minio_cluster_replication_total_failed_bytes`             | (_Site Replication Only_) Total number of bytes failed at least once to replicate since server start.    |
| `minio_cluster_replication_total_failed_count`             | (_Site Replication Only_) Total number of objects which failed replication since server start.           |
| `minio_cluster_replication_received_bytes`                 | (_Site Replication Only_) Total number of bytes replicated to this cluster from another source cluster.  |
| `minio_cluster_replication_received_count`                 | (_Site Replication Only_) Total number of objects received by this cluster from another source cluster.  |
| `minio_cluster_replication_sent_bytes`                     | (_Site Replication Only_) Total number of bytes replicated to the target cluster.                        |
| `minio_cluster_replication_sent_count`                     | (_Site Replication Only_) Total number of objects replicated to the target cluster.                      |
| `minio_cluster_replication_credential_errors`              | (_Site Replication Only_) Total number of replication credential errors since server start               |
| `minio_cluster_replication_proxied_get_requests_total` | (_Site Replication Only_)Number of GET requests proxied to replication target                          |
| `minio_cluster_replication_proxied_head_requests_total` | (_Site Replication Only_)Number of HEAD requests proxied to replication target                          |
| `minio_cluster_replication_proxied_delete_tagging_requests_total` | (_Site Replication Only_)Number of DELETE tagging requests proxied to replication target                          |
| `minio_cluster_replication_proxied_get_tagging_requests_total` | (_Site Replication Only_)Number of GET tagging requests proxied to replication target                          |
| `minio_cluster_replication_proxied_put_tagging_requests_total` | (_Site Replication Only_)Number of PUT tagging requests proxied to replication target                          |
| `minio_cluster_replication_proxied_get_requests_failures` | (_Site Replication Only_)Number of failures in GET requests proxied to replication target                          |
| `minio_cluster_replication_proxied_head_requests_failures` | (_Site Replication Only_)Number of failures in HEAD requests proxied to replication target                          |
| `minio_cluster_replication_proxied_delete_tagging_requests_failures` | (_Site Replication Only_)Number of failures proxying DELETE tagging requests to replication target                          |
| `minio_cluster_replication_proxied_get_tagging_requests_failures` | (_Site Replication Only_)Number of failures proxying GET tagging requests to replication target                          |
| `minio_cluster_replication_proxied_put_tagging_requests_failures` | (_Site Replication Only_)Number of failures proxying PUT tagging requests to replication target                          |


## Node Replication Metrics

Metrics marked as ``Site Replication Only`` only populate on deployments with [Site Replication](https://docs.min.io/community/minio-object-store/operations/install-deploy-manage/multi-site-replication.html) configurations.
For deployments with [bucket](https://docs.min.io/community/minio-object-store/administration/bucket-replication.html) or [batch](https://docs.min.io/community/minio-object-store/administration/batch-framework.html#replicate) configurations, these metrics populate instead under the [Bucket Metrics](#bucket-metrics) endpoint.

| Name                                                       | Description
|:-----------------------------------------------------------|:---------------------------------------------------------------------------------------------------------|
| `minio_node_replication_current_active_workers`         | Total number of active replication workers                                                               |
| `minio_node_replication_average_active_workers`         | Average number of active replication workers                                                             |
| `minio_node_replication_max_active_workers`             | Maximum number of active replication workers seen since server start                                     |
| `minio_node_replication_link_online`                    | Reports whether the replication link is online (1) or offline (0).                                       |
| `minio_node_replication_link_offline_duration_seconds`  | Total duration of replication link being offline in seconds since last offline event                     |
| `minio_node_replication_link_downtime_duration_seconds` | Total downtime of replication link in seconds since server start                                         |
| `minio_node_replication_average_link_latency_ms`        | Average replication link latency in milliseconds                                                         |
| `minio_node_replication_max_link_latency_ms`            | Maximum replication link latency in milliseconds seen since server start                                 |
| `minio_node_replication_current_link_latency_ms`        | Current replication link latency in milliseconds                                                         |
| `minio_node_replication_current_transfer_rate`          | Current replication transfer rate in bytes/sec                                                           |
| `minio_node_replication_average_transfer_rate`          | Average replication transfer rate in bytes/sec                                                           |
| `minio_node_replication_max_transfer_rate`              | Maximum replication transfer rate in bytes/sec seen since server start                                   |
| `minio_node_replication_last_minute_queued_count`       | Total number of objects queued for replication in the last full minute                                   |
| `minio_node_replication_last_minute_queued_bytes`       | Total number of bytes queued for replication in the last full minute                                     |
| `minio_node_replication_average_queued_count`           | Average number of objects queued for replication since server start                                      |
| `minio_node_replication_average_queued_bytes`           | Average number of bytes queued for replication since server start                                        |
| `minio_node_replication_max_queued_bytes`               | Maximum number of bytes queued for replication seen since server start                                   |
| `minio_node_replication_max_queued_count`               | Maximum number of objects queued for replication seen since server start                                 |
| `minio_node_replication_recent_backlog_count`           | Total number of objects seen in replication backlog in the last 5 minutes                                |

## Healing Metrics

| Name                                         | Description                                                      |
|:---------------------------------------------|:-----------------------------------------------------------------|
| `minio_heal_objects_errors_total`            | Objects for which healing failed in current self healing run.    |
| `minio_heal_objects_heal_total`              | Objects healed in current self healing run.                      |
| `minio_heal_objects_total`                   | Objects scanned in current self healing run.                     |
| `minio_heal_time_last_activity_nano_seconds` | Time elapsed (in nano seconds) since last self healing activity. |

## Inter Node Metrics

| Name                                      | Description                                             |
|:------------------------------------------|:--------------------------------------------------------|
| `minio_inter_node_traffic_dial_avg_time`  | Average time of internodes TCP dial calls.              |
| `minio_inter_node_traffic_dial_errors`    | Total number of internode TCP dial timeouts and errors. |
| `minio_inter_node_traffic_errors_total`   | Total number of failed internode calls.                 |
| `minio_inter_node_traffic_received_bytes` | Total number of bytes received from other peer nodes.   |
| `minio_inter_node_traffic_sent_bytes`     | Total number of bytes sent to the other peer nodes.     |

## Bucket Notification Metrics

| Name                                           | Description                                                                                                                                 |
|:-----------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------|
| `minio_notify_current_send_in_progress`        | Number of concurrent async Send calls active to all targets (deprecated, please use `minio_notify_target_current_send_in_progress` instead) |
| `minio_notify_events_errors_total`             | Events that were failed to be sent to the targets (deprecated, please use `minio_notify_target_failed_events` instead)                      |
| `minio_notify_events_sent_total`               | Total number of events sent to the targets (deprecated, please use `minio_notify_target_total_events` instead)                              |
| `minio_notify_events_skipped_total`            | Events that were skipped to be sent to the targets due to the in-memory queue being full                                                    |
| `minio_notify_target_current_send_in_progress` | Number of concurrent async Send calls active to the target                                                                                  |
| `minio_notify_target_queue_length`             | Number of events currently staged in the queue_dir configured for the target.                                                               |
| `minio_notify_target_total_events`             | Total number of events sent (or) queued to the target                                                                                       |

## S3 API Request Metrics

| Name                                          | Description                                              |
|:----------------------------------------------|:---------------------------------------------------------|
| `minio_s3_requests_4xx_errors_total`          | Total number S3 requests with (4xx) errors.              |
| `minio_s3_requests_5xx_errors_total`          | Total number S3 requests with (5xx) errors.              |
| `minio_s3_requests_canceled_total`            | Total number S3 requests canceled by the client.         |
| `minio_s3_requests_errors_total`              | Total number S3 requests with (4xx and 5xx) errors.      |
| `minio_s3_requests_incoming_total`            | Volatile number of total incoming S3 requests.           |
| `minio_s3_requests_inflight_total`            | Total number of S3 requests currently in flight.         |
| `minio_s3_requests_rejected_auth_total`       | Total number S3 requests rejected for auth failure.      |
| `minio_s3_requests_rejected_header_total`     | Total number S3 requests rejected for invalid header.    |
| `minio_s3_requests_rejected_invalid_total`    | Total number S3 invalid requests.                        |
| `minio_s3_requests_rejected_timestamp_total`  | Total number S3 requests rejected for invalid timestamp. |
| `minio_s3_requests_total`                     | Total number S3 requests.                                |
| `minio_s3_requests_waiting_total`             | Number of S3 requests in the waiting queue.              |
| `minio_s3_requests_ttfb_seconds_distribution` | Distribution of the time to first byte across API calls. |
| `minio_s3_traffic_received_bytes`             | Total number of s3 bytes received.                       |
| `minio_s3_traffic_sent_bytes`                 | Total number of s3 bytes sent.                           |

## Software Metrics

| Name                          | Description                            |
|:------------------------------|:---------------------------------------|
| `minio_software_commit_info`  | Git commit hash for the MinIO release. |
| `minio_software_version_info` | MinIO Release tag for the server.      |

## Drive Metrics

| Name                                   | Description                                                         |
|:---------------------------------------|:--------------------------------------------------------------------|
| `minio_node_drive_free_bytes`          | Total storage available on a drive.                                 |
| `minio_node_drive_free_inodes`         | Total free inodes.                                                  |
| `minio_node_drive_latency_us`          | Average last minute latency in µs for drive API storage operations. |
| `minio_node_drive_offline_total`       | Total drives offline in this node.                                  |
| `minio_node_drive_online_total`        | Total drives online in this node.                                   |
| `minio_node_drive_total`               | Total drives in this node.                                          |
| `minio_node_drive_total_bytes`         | Total storage on a drive.                                           |
| `minio_node_drive_used_bytes`          | Total storage used on a drive.                                      |
| `minio_node_drive_errors_timeout`      | Total number of drive timeout errors since server start             |
| `minio_node_drive_errors_ioerror`      | Total number of drive I/O errors since server start                 |
| `minio_node_drive_errors_availability` | Total number of drive I/O errors, timeouts since server start       |
| `minio_node_drive_io_waiting`          | Total number I/O operations waiting on drive                        |

## Identity and Access Management (IAM) Metrics

| Name                                       | Description                                                 |
|:-------------------------------------------|:------------------------------------------------------------|
| `minio_node_iam_last_sync_duration_millis` | Last successful IAM data sync duration in milliseconds.     |
| `minio_node_iam_since_last_sync_millis`    | Time (in milliseconds) since last successful IAM data sync. |
| `minio_node_iam_sync_failures`             | Number of failed IAM data syncs since server start.         |
| `minio_node_iam_sync_successes`            | Number of successful IAM data syncs since server start.     |

## Information Lifecycle Management (ILM) Metrics

| Name                                                         | Description                                                                                                |
|:-------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| `minio_node_ilm_expiry_pending_tasks`                        | Number of pending ILM expiry tasks in the queue.                                                           |
| `minio_node_ilm_transition_active_tasks`                     | Number of active ILM transition tasks.                                                                     |
| `minio_node_ilm_transition_pending_tasks`                    | Number of pending ILM transition tasks in the queue.                                                       |
| `minio_node_ilm_transition_missed_immediate_tasks`           | Number of missed immediate ILM transition tasks.                                                           |
| `minio_node_ilm_versions_scanned`                            | Total number of object versions checked for ilm actions since server start.                                |
| `minio_node_ilm_action_count_delete_action`                  | Total action outcome of lifecycle checks since server start for deleting object                            |
| `minio_node_ilm_action_count_delete_version_action`          | Total action outcome of lifecycle checks since server start for deleting a version                         |
| `minio_node_ilm_action_count_transition_action`              | Total action outcome of lifecycle checks since server start for transition of an object                    |
| `minio_node_ilm_action_count_transition_version_action`      | Total action outcome of lifecycle checks since server start for transition of a particular object version  |
| `minio_node_ilm_action_count_delete_restored_action`         | Total action outcome of lifecycle checks since server start for deletion of temporarily restored object    |
| `minio_node_ilm_action_count_delete_restored_version_action` | Total action outcome of lifecycle checks since server start for deletion of a temporarily restored version |
| `minio_node_ilm_action_count_delete_all_versions_action`     | Total action outcome of lifecycle checks since server start for deletion of all versions                   |

## Tier Metrics

| Name                                               | Description                                                                 |
|:---------------------------------------------------|:----------------------------------------------------------------------------|
| `minio_node_tier_tier_ttlb_seconds_distribution`   | Distribution of time to last byte for objects downloaded from warm tier     |
| `minio_node_tier_requests_success`                 | Number of requests to download object from warm tier that were successful   | 
| `minio_node_tier_requests_failure`                 | Number of requests to download object from warm tier that were failure      | 

## System Metrics

| Name                                       | Description                                                                                                     |
|:-------------------------------------------|:----------------------------------------------------------------------------------------------------------------|
| `minio_node_file_descriptor_limit_total`   | Limit on total number of open file descriptors for the MinIO Server process.                                    |
| `minio_node_file_descriptor_open_total`    | Total number of open file descriptors by the MinIO Server process.                                              |
| `minio_node_go_routine_total`              | Total number of go routines running.                                                                            |
| `minio_node_io_rchar_bytes`                | Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar.       |
| `minio_node_io_read_bytes`                 | Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes.                  |
| `minio_node_io_wchar_bytes`                | Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar. |
| `minio_node_io_write_bytes`                | Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes.                |
| `minio_node_process_cpu_total_seconds`     | Total user and system CPU time spent in seconds by the process.                                                |
| `minio_node_process_resident_memory_bytes` | Resident memory size in bytes.                                                                                  |
| `minio_node_process_virtual_memory_bytes`  | Virtual memory size in bytes.                                                                                   |
| `minio_node_process_starttime_seconds`     | Start time for MinIO process per node, time in seconds since Unix epoc.                                         |
| `minio_node_process_uptime_seconds`        | Uptime for MinIO process per node in seconds.                                                                   |

## Scanner Metrics

| Name                                       | Description                                                 |
|:-------------------------------------------|:------------------------------------------------------------|
| `minio_node_scanner_bucket_scans_finished` | Total number of bucket scans finished since server start.   |
| `minio_node_scanner_bucket_scans_started`  | Total number of bucket scans started since server start.    |
| `minio_node_scanner_directories_scanned`   | Total number of directories scanned since server start.     |
| `minio_node_scanner_objects_scanned`       | Total number of unique objects scanned since server start.  |
| `minio_node_scanner_versions_scanned`      | Total number of object versions scanned since server start. |
| `minio_node_syscall_read_total`            | Total read SysCalls to the kernel. /proc/[pid]/io syscr.    |
| `minio_node_syscall_write_total`           | Total write SysCalls to the kernel. /proc/[pid]/io syscw.   |
| `minio_usage_last_activity_nano_seconds`   | Time elapsed (in nano seconds) since last scan activity.    |

# Bucket Metrics

MinIO collects the following metrics at the bucket level.
Each metric includes the ``bucket`` label to identify the corresponding bucket.
Metrics may include one or more additional labels, such as the server that calculated that metric.

These metrics can be obtained from any MinIO server once per collection by using the following URL:

```shell
https://HOSTNAME:PORT/minio/v2/metrics/bucket
```

Replace ``HOSTNAME:PORT`` with the hostname of your MinIO deployment.
For deployments behind a load balancer, use the load balancer hostname instead of a single node hostname.

## Distribution Metrics

| Name                                        | Description                                                                     |
|:--------------------------------------------|:--------------------------------------------------------------------------------|
| `minio_bucket_objects_size_distribution`    | Distribution of object sizes in the bucket, includes label for the bucket name. |
| `minio_bucket_objects_version_distribution` | Distribution of object sizes in a bucket, by number of versions                 |

## Replication Metrics

These metrics only populate on deployments with [Bucket Replication](https://docs.min.io/community/minio-object-store/administration/bucket-replication.html) or [Batch Replication](https://docs.min.io/community/minio-object-store/administration/batch-framework.html) configurations.
For deployments with [Site Replication](https://docs.min.io/community/minio-object-store/operations/install-deploy-manage/multi-site-replication.html) configured, select metrics populate under the [Cluster Metrics](#cluster-metrics) endpoint.

| Name                                                | Description                                                                      |
|:----------------------------------------------------|:---------------------------------------------------------------------------------|
| `minio_bucket_replication_last_minute_failed_bytes` | Total number of bytes failed at least once to replicate in the last full minute. |
| `minio_bucket_replication_last_minute_failed_count` | Total number of objects which failed replication in the last full minute.        |
| `minio_bucket_replication_last_hour_failed_bytes`   | Total number of bytes failed at least once to replicate in the last full hour.   |
| `minio_bucket_replication_last_hour_failed_count`   | Total number of objects which failed replication in the last full hour.          |
| `minio_bucket_replication_total_failed_bytes`       | Total number of bytes failed at least once to replicate since server start.      |
| `minio_bucket_replication_total_failed_count`       | Total number of objects which failed replication since server start.             |
| `minio_bucket_replication_latency_ms`               | Replication latency in milliseconds.                                             |
| `minio_bucket_replication_received_bytes`           | Total number of bytes replicated to this bucket from another source bucket.      |
| `minio_bucket_replication_received_count`           | Total number of objects received by this bucket from another source bucket.      |
| `minio_bucket_replication_sent_bytes`               | Total number of bytes replicated to the target bucket.                           |
| `minio_bucket_replication_sent_count`               | Total number of objects replicated to the target bucket.                         |
| `minio_bucket_replication_credential_errors`        | Total number of replication credential errors since server start                 |
| `minio_bucket_replication_proxied_get_requests_total` | Number of GET requests proxied to replication target                          |
| `minio_bucket_replication_proxied_head_requests_total` | Number of HEAD requests proxied to replication target                          |
| `minio_bucket_replication_proxied_delete_tagging_requests_total` | Number of DELETE tagging requests proxied to replication target                          |
| `minio_bucket_replication_proxied_get_tagging_requests_total` | Number of GET tagging requests proxied to replication target                          |
| `minio_bucket_replication_proxied_put_tagging_requests_total` | Number of PUT tagging requests proxied to replication target                          |
| `minio_bucket_replication_proxied_get_requests_failures` | Number of failures in GET requests proxied to replication target                          |
| `minio_bucket_replication_proxied_head_requests_failures` | Number of failures in HEAD requests proxied to replication target                          |
| `minio_bucket_replication_proxied_delete_tagging_requests_failures` | Number of failures in DELETE tagging proxy requests to replication target                          |
| `minio_bucket_replication_proxied_get_tagging_requests_failures` |Number of failures in GET tagging proxy requests to replication target                          |
| `minio_bucket_replication_proxied_put_tagging_requests_failures` | Number of failures in PUT tagging proxy requests to replication target                          |

## Traffic Metrics

| Name                                  | Description                                        |
|:--------------------------------------|:---------------------------------------------------|
| `minio_bucket_traffic_received_bytes` | Total number of S3 bytes received for this bucket. |
| `minio_bucket_traffic_sent_bytes`     | Total number of S3 bytes sent for this bucket.     |
	
## Usage Metrics

| Name                                    | Description                                       |
|:----------------------------------------|:--------------------------------------------------|
| `minio_bucket_usage_object_total`       | Total number of objects.                          |
| `minio_bucket_usage_version_total`      | Total number of versions (includes delete marker) |
| `minio_bucket_usage_deletemarker_total` | Total number of delete markers.                   |
| `minio_bucket_usage_total_bytes`        | Total bucket size in bytes.                       |
| `minio_bucket_quota_total_bytes`        | Total bucket quota size in bytes.                 |

## Requests Metrics

| Name                                              | Description                                                     |
|:--------------------------------------------------|:----------------------------------------------------------------|
| `minio_bucket_requests_4xx_errors_total`          | Total number of S3 requests with (4xx) errors on a bucket.      |
| `minio_bucket_requests_5xx_errors_total`          | Total number of S3 requests with (5xx) errors on a bucket.      |
| `minio_bucket_requests_inflight_total`            | Total number of S3 requests currently in flight on a bucket.    |
| `minio_bucket_requests_total`                     | Total number of S3 requests on a bucket.                        |
| `minio_bucket_requests_canceled_total`            | Total number S3 requests canceled by the client.                |
| `minio_bucket_requests_ttfb_seconds_distribution` | Distribution of time to first byte across API calls per bucket. |

# Resource Metrics

MinIO collects the following resource metrics at the node level.
Each metric includes the `server` label to identify the corresponding node.
Metrics may include one or more additional labels, such as the drive path, interface name, etc.

These metrics can be obtained from any MinIO server once per collection by using the following URL:

```shell
https://HOSTNAME:PORT/minio/v2/metrics/resource
```

Replace `HOSTNAME:PORT` with the hostname of your MinIO deployment.
For deployments behind a load balancer, use the load balancer hostname instead of a single node hostname.

## Drive Resource Metrics

| Name                                 | Description                                              |
|:-------------------------------------|:---------------------------------------------------------|
| `minio_node_drive_total_bytes`       | Total bytes on a drive.                                  |
| `minio_node_drive_used_bytes`        | Used bytes on a drive.                                   |
| `minio_node_drive_total_inodes`      | Total inodes on a drive.                                 |
| `minio_node_drive_used_inodes`       | Total inodes used on a drive.                            |
| `minio_node_drive_reads_per_sec`     | Reads per second on a drive.                             |
| `minio_node_drive_reads_kb_per_sec`  | Kilobytes read per second on a drive.                    |
| `minio_node_drive_reads_await`       | Average time for read requests to be served on a drive.  |
| `minio_node_drive_writes_per_sec`    | Writes per second on a drive.                            |
| `minio_node_drive_writes_kb_per_sec` | Kilobytes written per second on a drive.                 |
| `minio_node_drive_writes_await`      | Average time for write requests to be served on a drive. |
| `minio_node_drive_perc_util`         | Percentage of time the disk was busy since uptime.       |

## Network Interface Metrics

| Name                          | Description                                                |
|:------------------------------|:-----------------------------------------------------------|
| `minio_node_if_rx_bytes`      | Bytes received on the interface in 60s.                    |
| `minio_node_if_rx_bytes_avg`  | Bytes received on the interface in 60s (avg) since uptime. |
| `minio_node_if_rx_bytes_max`  | Bytes received on the interface in 60s (max) since uptime. |
| `minio_node_if_rx_errors`     | Receive errors in 60s.                                     |
| `minio_node_if_rx_errors_avg` | Receive errors in 60s (avg).                               |
| `minio_node_if_rx_errors_max` | Receive errors in 60s (max).                               |
| `minio_node_if_tx_bytes`      | Bytes transmitted in 60s.                                  |
| `minio_node_if_tx_bytes_avg`  | Bytes transmitted in 60s (avg).                            |
| `minio_node_if_tx_bytes_max`  | Bytes transmitted in 60s (max).                            |
| `minio_node_if_tx_errors`     | Transmit errors in 60s.                                    |
| `minio_node_if_tx_errors_avg` | Transmit errors in 60s (avg).                              |
| `minio_node_if_tx_errors_max` | Transmit errors in 60s (max).                              |

## CPU Metrics

| Name                                 | Description                                |
|:-------------------------------------|:-------------------------------------------|
| `minio_node_cpu_avg_user`            | CPU user time.                             |
| `minio_node_cpu_avg_user_avg`        | CPU user time (avg).                       |
| `minio_node_cpu_avg_user_max`        | CPU user time (max).                       |
| `minio_node_cpu_avg_system`          | CPU system time.                           |
| `minio_node_cpu_avg_system_avg`      | CPU system time (avg).                     |
| `minio_node_cpu_avg_system_max`      | CPU system time (max).                     |
| `minio_node_cpu_avg_idle`            | CPU idle time.                             |
| `minio_node_cpu_avg_idle_avg`        | CPU idle time (avg).                       |
| `minio_node_cpu_avg_idle_max`        | CPU idle time (max).                       |
| `minio_node_cpu_avg_iowait`          | CPU ioWait time.                           |
| `minio_node_cpu_avg_iowait_avg`      | CPU ioWait time (avg).                     |
| `minio_node_cpu_avg_iowait_max`      | CPU ioWait time (max).                     |
| `minio_node_cpu_avg_nice`            | CPU nice time.                             |
| `minio_node_cpu_avg_nice_avg`        | CPU nice time (avg).                       |
| `minio_node_cpu_avg_nice_max`        | CPU nice time (max).                       |
| `minio_node_cpu_avg_steal`           | CPU steam time.                            |
| `minio_node_cpu_avg_steal_avg`       | CPU steam time (avg).                      |
| `minio_node_cpu_avg_steal_max`       | CPU steam time (max).                      |
| `minio_node_cpu_avg_load1`           | CPU load average 1min.                     |
| `minio_node_cpu_avg_load1_avg`       | CPU load average 1min (avg).               |
| `minio_node_cpu_avg_load1_max`       | CPU load average 1min (max).               |
| `minio_node_cpu_avg_load1_perc`      | CPU load average 1min (percentage).        |
| `minio_node_cpu_avg_load1_perc_avg`  | CPU load average 1min (percentage) (avg).  |
| `minio_node_cpu_avg_load1_perc_max`  | CPU load average 1min (percentage) (max).  |
| `minio_node_cpu_avg_load5`           | CPU load average 5min.                     |
| `minio_node_cpu_avg_load5_avg`       | CPU load average 5min (avg).               |
| `minio_node_cpu_avg_load5_max`       | CPU load average 5min (max).               |
| `minio_node_cpu_avg_load5_perc`      | CPU load average 5min (percentage).        |
| `minio_node_cpu_avg_load5_perc_avg`  | CPU load average 5min (percentage) (avg).  |
| `minio_node_cpu_avg_load5_perc_max`  | CPU load average 5min (percentage) (max).  |
| `minio_node_cpu_avg_load15`          | CPU load average 15min.                    |
| `minio_node_cpu_avg_load15_avg`      | CPU load average 15min (avg).              |
| `minio_node_cpu_avg_load15_max`      | CPU load average 15min (max).              |
| `minio_node_cpu_avg_load15_perc`     | CPU load average 15min (percentage).       |
| `minio_node_cpu_avg_load15_perc_avg` | CPU load average 15min (percentage) (avg). |
| `minio_node_cpu_avg_load15_perc_max` | CPU load average 15min (percentage) (max). |

## Memory Metrics

| Name                           | Description                               |
|:-------------------------------|:------------------------------------------|
| `minio_node_mem_available`     | Available memory on the node.             |
| `minio_node_mem_available_avg` | Available memory on the node (avg).       |
| `minio_node_mem_available_max` | Available memory on the node (max).       |
| `minio_node_mem_buffers`       | Buffers memory on the node.               |
| `minio_node_mem_buffers_avg`   | Buffers memory on the node (avg).         |
| `minio_node_mem_buffers_max`   | Buffers memory on the node (max).         |
| `minio_node_mem_cache`         | Cache memory on the node.                 |
| `minio_node_mem_cache_avg`     | Cache memory on the node (avg).           |
| `minio_node_mem_cache_max`     | Cache memory on the node (max).           |
| `minio_node_mem_free`          | Free memory on the node.                  |
| `minio_node_mem_free_avg`      | Free memory on the node (avg).            |
| `minio_node_mem_free_max`      | Free memory on the node (max).            |
| `minio_node_mem_shared`        | Shared memory on the node.                |
| `minio_node_mem_shared_avg`    | Shared memory on the node (avg).          |
| `minio_node_mem_shared_max`    | Shared memory on the node (max).          |
| `minio_node_mem_total`         | Total memory on the node.                 |
| `minio_node_mem_total_avg`     | Total memory on the node (avg).           |
| `minio_node_mem_total_max`     | Total memory on the node (max).           |
| `minio_node_mem_used`          | Used memory on the node.                  |
| `minio_node_mem_used_avg`      | Used memory on the node (avg).            |
| `minio_node_mem_used_max`      | Used memory on the node (max).            |
| `minio_node_mem_used_perc`     | Used memory percentage on the node.       |
| `minio_node_mem_used_perc_avg` | Used memory percentage on the node (avg). |
| `minio_node_mem_used_perc_max` | Used memory percentage on the node (max). |
