// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"maps"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/cachevalue"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
	"github.com/minio/minio/internal/rest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/procfs"
)

//go:generate msgp -file=$GOFILE -unexported -io=false

var (
	nodeCollector           *minioNodeCollector
	clusterCollector        *minioClusterCollector
	bucketCollector         *minioBucketCollector
	peerMetricsGroups       []*MetricsGroupV2
	bucketPeerMetricsGroups []*MetricsGroupV2
)

// v2MetricsMaxBuckets enforces a bucket count limit on metrics for v2 calls.
// If people hit this limit, they should move to v3, as certain calls explode with high bucket count.
const v2MetricsMaxBuckets = 100

func init() {
	clusterMetricsGroups := []*MetricsGroupV2{
		getNodeHealthMetrics(MetricsGroupOpts{dependGlobalNotificationSys: true}),
		getClusterStorageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getClusterTierMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getClusterUsageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getKMSMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, dependGlobalKMS: true}),
		getClusterHealthMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getIAMNodeMetrics(MetricsGroupOpts{dependGlobalAuthNPlugin: true, dependGlobalIAMSys: true}),
		getReplicationSiteMetrics(MetricsGroupOpts{dependGlobalSiteReplicationSys: true}),
		getBatchJobsMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
	}

	peerMetricsGroups = []*MetricsGroupV2{
		getGoMetrics(),
		getHTTPMetrics(MetricsGroupOpts{}),
		getNotificationMetrics(MetricsGroupOpts{dependGlobalLambdaTargetList: true}),
		getMinioProcMetrics(),
		getMinioVersionMetrics(),
		getNetworkMetrics(),
		getS3TTFBMetric(),
		getILMNodeMetrics(),
		getScannerNodeMetrics(),
		getIAMNodeMetrics(MetricsGroupOpts{dependGlobalAuthNPlugin: true, dependGlobalIAMSys: true}),
		getKMSNodeMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, dependGlobalKMS: true}),
		getMinioHealingMetrics(MetricsGroupOpts{dependGlobalBackgroundHealState: true}),
		getWebhookMetrics(),
		getTierMetrics(),
	}

	allMetricsGroups := func() (allMetrics []*MetricsGroupV2) {
		allMetrics = append(allMetrics, clusterMetricsGroups...)
		allMetrics = append(allMetrics, peerMetricsGroups...)
		return allMetrics
	}()

	nodeGroups := []*MetricsGroupV2{
		getNodeHealthMetrics(MetricsGroupOpts{dependGlobalNotificationSys: true}),
		getHTTPMetrics(MetricsGroupOpts{}),
		getNetworkMetrics(),
		getMinioVersionMetrics(),
		getS3TTFBMetric(),
		getTierMetrics(),
		getNotificationMetrics(MetricsGroupOpts{dependGlobalLambdaTargetList: true}),
		getDistLockMetrics(MetricsGroupOpts{dependGlobalIsDistErasure: true, dependGlobalLockServer: true}),
		getIAMNodeMetrics(MetricsGroupOpts{dependGlobalAuthNPlugin: true, dependGlobalIAMSys: true}),
		getLocalStorageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getReplicationNodeMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, dependBucketTargetSys: true}),
	}

	bucketMetricsGroups := []*MetricsGroupV2{
		getBucketUsageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getHTTPMetrics(MetricsGroupOpts{bucketOnly: true}),
		getBucketTTFBMetric(),
	}

	bucketPeerMetricsGroups = []*MetricsGroupV2{
		getHTTPMetrics(MetricsGroupOpts{bucketOnly: true}),
		getBucketTTFBMetric(),
	}

	nodeCollector = newMinioCollectorNode(nodeGroups)
	clusterCollector = newMinioClusterCollector(allMetricsGroups)
	bucketCollector = newMinioBucketCollector(bucketMetricsGroups)
}

// MetricNamespace is top level grouping of metrics to create the metric name.
type MetricNamespace string

// MetricSubsystem is the sub grouping for metrics within a namespace.
type MetricSubsystem string

const (
	bucketMetricNamespace    MetricNamespace = "minio_bucket"
	clusterMetricNamespace   MetricNamespace = "minio_cluster"
	healMetricNamespace      MetricNamespace = "minio_heal"
	interNodeMetricNamespace MetricNamespace = "minio_inter_node"
	nodeMetricNamespace      MetricNamespace = "minio_node"
	minioMetricNamespace     MetricNamespace = "minio"
	s3MetricNamespace        MetricNamespace = "minio_s3"
)

const (
	cacheSubsystem            MetricSubsystem = "cache"
	capacityRawSubsystem      MetricSubsystem = "capacity_raw"
	capacityUsableSubsystem   MetricSubsystem = "capacity_usable"
	driveSubsystem            MetricSubsystem = "drive"
	interfaceSubsystem        MetricSubsystem = "if"
	memSubsystem              MetricSubsystem = "mem"
	cpuSubsystem              MetricSubsystem = "cpu_avg"
	storageClassSubsystem     MetricSubsystem = "storage_class"
	fileDescriptorSubsystem   MetricSubsystem = "file_descriptor"
	goRoutines                MetricSubsystem = "go_routine"
	ioSubsystem               MetricSubsystem = "io"
	nodesSubsystem            MetricSubsystem = "nodes"
	objectsSubsystem          MetricSubsystem = "objects"
	bucketsSubsystem          MetricSubsystem = "bucket"
	processSubsystem          MetricSubsystem = "process"
	replicationSubsystem      MetricSubsystem = "replication"
	requestsSubsystem         MetricSubsystem = "requests"
	requestsRejectedSubsystem MetricSubsystem = "requests_rejected"
	timeSubsystem             MetricSubsystem = "time"
	ttfbSubsystem             MetricSubsystem = "requests_ttfb"
	trafficSubsystem          MetricSubsystem = "traffic"
	softwareSubsystem         MetricSubsystem = "software"
	sysCallSubsystem          MetricSubsystem = "syscall"
	usageSubsystem            MetricSubsystem = "usage"
	quotaSubsystem            MetricSubsystem = "quota"
	ilmSubsystem              MetricSubsystem = "ilm"
	tierSubsystem             MetricSubsystem = "tier"
	scannerSubsystem          MetricSubsystem = "scanner"
	iamSubsystem              MetricSubsystem = "iam"
	kmsSubsystem              MetricSubsystem = "kms"
	notifySubsystem           MetricSubsystem = "notify"
	lambdaSubsystem           MetricSubsystem = "lambda"
	auditSubsystem            MetricSubsystem = "audit"
	webhookSubsystem          MetricSubsystem = "webhook"
)

// MetricName are the individual names for the metric.
type MetricName string

const (
	authTotal         MetricName = "auth_total"
	canceledTotal     MetricName = "canceled_total"
	errorsTotal       MetricName = "errors_total"
	headerTotal       MetricName = "header_total"
	healTotal         MetricName = "heal_total"
	hitsTotal         MetricName = "hits_total"
	inflightTotal     MetricName = "inflight_total"
	invalidTotal      MetricName = "invalid_total"
	limitTotal        MetricName = "limit_total"
	missedTotal       MetricName = "missed_total"
	waitingTotal      MetricName = "waiting_total"
	incomingTotal     MetricName = "incoming_total"
	objectTotal       MetricName = "object_total"
	versionTotal      MetricName = "version_total"
	deleteMarkerTotal MetricName = "deletemarker_total"
	offlineTotal      MetricName = "offline_total"
	onlineTotal       MetricName = "online_total"
	openTotal         MetricName = "open_total"
	readTotal         MetricName = "read_total"
	timestampTotal    MetricName = "timestamp_total"
	writeTotal        MetricName = "write_total"
	total             MetricName = "total"
	freeInodes        MetricName = "free_inodes"

	lastMinFailedCount  MetricName = "last_minute_failed_count"
	lastMinFailedBytes  MetricName = "last_minute_failed_bytes"
	lastHourFailedCount MetricName = "last_hour_failed_count"
	lastHourFailedBytes MetricName = "last_hour_failed_bytes"
	totalFailedCount    MetricName = "total_failed_count"
	totalFailedBytes    MetricName = "total_failed_bytes"

	currActiveWorkers  MetricName = "current_active_workers"
	avgActiveWorkers   MetricName = "average_active_workers"
	maxActiveWorkers   MetricName = "max_active_workers"
	recentBacklogCount MetricName = "recent_backlog_count"
	currInQueueCount   MetricName = "last_minute_queued_count"
	currInQueueBytes   MetricName = "last_minute_queued_bytes"
	receivedCount      MetricName = "received_count"
	sentCount          MetricName = "sent_count"
	currTransferRate   MetricName = "current_transfer_rate"
	avgTransferRate    MetricName = "average_transfer_rate"
	maxTransferRate    MetricName = "max_transfer_rate"
	credentialErrors   MetricName = "credential_errors"

	currLinkLatency MetricName = "current_link_latency_ms"
	avgLinkLatency  MetricName = "average_link_latency_ms"
	maxLinkLatency  MetricName = "max_link_latency_ms"

	linkOnline                MetricName = "link_online"
	linkOfflineDuration       MetricName = "link_offline_duration_seconds"
	linkDowntimeTotalDuration MetricName = "link_downtime_duration_seconds"

	avgInQueueCount                     MetricName = "average_queued_count"
	avgInQueueBytes                     MetricName = "average_queued_bytes"
	maxInQueueCount                     MetricName = "max_queued_count"
	maxInQueueBytes                     MetricName = "max_queued_bytes"
	proxiedGetRequestsTotal             MetricName = "proxied_get_requests_total"
	proxiedHeadRequestsTotal            MetricName = "proxied_head_requests_total"
	proxiedPutTaggingRequestsTotal      MetricName = "proxied_put_tagging_requests_total"
	proxiedGetTaggingRequestsTotal      MetricName = "proxied_get_tagging_requests_total"
	proxiedDeleteTaggingRequestsTotal   MetricName = "proxied_delete_tagging_requests_total"
	proxiedGetRequestsFailures          MetricName = "proxied_get_requests_failures"
	proxiedHeadRequestsFailures         MetricName = "proxied_head_requests_failures"
	proxiedPutTaggingRequestFailures    MetricName = "proxied_put_tagging_requests_failures"
	proxiedGetTaggingRequestFailures    MetricName = "proxied_get_tagging_requests_failures"
	proxiedDeleteTaggingRequestFailures MetricName = "proxied_delete_tagging_requests_failures"

	freeBytes       MetricName = "free_bytes"
	readBytes       MetricName = "read_bytes"
	rcharBytes      MetricName = "rchar_bytes"
	receivedBytes   MetricName = "received_bytes"
	latencyMilliSec MetricName = "latency_ms"
	sentBytes       MetricName = "sent_bytes"
	totalBytes      MetricName = "total_bytes"
	usedBytes       MetricName = "used_bytes"
	writeBytes      MetricName = "write_bytes"
	wcharBytes      MetricName = "wchar_bytes"

	latencyMicroSec MetricName = "latency_us"
	latencyNanoSec  MetricName = "latency_ns"

	commitInfo  MetricName = "commit_info"
	usageInfo   MetricName = "usage_info"
	versionInfo MetricName = "version_info"

	sizeDistribution    = "size_distribution"
	versionDistribution = "version_distribution"
	ttfbDistribution    = "seconds_distribution"
	ttlbDistribution    = "ttlb_seconds_distribution"

	lastActivityTime = "last_activity_nano_seconds"
	startTime        = "starttime_seconds"
	upTime           = "uptime_seconds"
	memory           = "resident_memory_bytes"
	vmemory          = "virtual_memory_bytes"
	cpu              = "cpu_total_seconds"

	expiryMissedTasks            MetricName = "expiry_missed_tasks"
	expiryMissedFreeVersions     MetricName = "expiry_missed_freeversions"
	expiryMissedTierJournalTasks MetricName = "expiry_missed_tierjournal_tasks"
	expiryNumWorkers             MetricName = "expiry_num_workers"
	transitionMissedTasks        MetricName = "transition_missed_immediate_tasks"

	transitionedBytes    MetricName = "transitioned_bytes"
	transitionedObjects  MetricName = "transitioned_objects"
	transitionedVersions MetricName = "transitioned_versions"

	tierRequestsSuccess MetricName = "requests_success"
	tierRequestsFailure MetricName = "requests_failure"

	kmsOnline          = "online"
	kmsRequestsSuccess = "request_success"
	kmsRequestsError   = "request_error"
	kmsRequestsFail    = "request_failure"
	kmsUptime          = "uptime"

	webhookOnline = "online"
)

const (
	serverName = "server"
)

// MetricTypeV2 for the types of metrics supported
type MetricTypeV2 string

const (
	gaugeMetric     = "gaugeMetric"
	counterMetric   = "counterMetric"
	histogramMetric = "histogramMetric"
)

// MetricDescription describes the metric
type MetricDescription struct {
	Namespace MetricNamespace `json:"MetricNamespace"`
	Subsystem MetricSubsystem `json:"Subsystem"`
	Name      MetricName      `json:"MetricName"`
	Help      string          `json:"Help"`
	Type      MetricTypeV2    `json:"Type"`
}

// MetricV2 captures the details for a metric
type MetricV2 struct {
	Description          MetricDescription `json:"Description"`
	StaticLabels         map[string]string `json:"StaticLabels"`
	Value                float64           `json:"Value"`
	VariableLabels       map[string]string `json:"VariableLabels"`
	HistogramBucketLabel string            `json:"HistogramBucketLabel"`
	Histogram            map[string]uint64 `json:"Histogram"`
}

// MetricsGroupV2 are a group of metrics that are initialized together.
type MetricsGroupV2 struct {
	metricsCache     *cachevalue.Cache[[]MetricV2] `msg:"-"`
	cacheInterval    time.Duration
	metricsGroupOpts MetricsGroupOpts
}

// MetricsGroupOpts are a group of metrics opts to be used to initialize the metrics group.
type MetricsGroupOpts struct {
	dependGlobalObjectAPI           bool
	dependGlobalAuthNPlugin         bool
	dependGlobalSiteReplicationSys  bool
	dependGlobalNotificationSys     bool
	dependGlobalKMS                 bool
	bucketOnly                      bool
	dependGlobalLambdaTargetList    bool
	dependGlobalIAMSys              bool
	dependGlobalLockServer          bool
	dependGlobalIsDistErasure       bool
	dependGlobalBackgroundHealState bool
	dependBucketTargetSys           bool
}

// RegisterRead register the metrics populator function to be used
// to populate new values upon cache invalidation.
func (g *MetricsGroupV2) RegisterRead(read func(context.Context) []MetricV2) {
	g.metricsCache = cachevalue.NewFromFunc(g.cacheInterval,
		cachevalue.Opts{ReturnLastGood: true},
		func(ctx context.Context) ([]MetricV2, error) {
			if g.metricsGroupOpts.dependGlobalObjectAPI {
				objLayer := newObjectLayerFn()
				// Service not initialized yet
				if objLayer == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalAuthNPlugin {
				if globalAuthNPlugin == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalSiteReplicationSys {
				if !globalSiteReplicationSys.isEnabled() {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalNotificationSys {
				if globalNotificationSys == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalKMS {
				if GlobalKMS == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalLambdaTargetList {
				if globalLambdaTargetList == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalIAMSys {
				if globalIAMSys == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalLockServer {
				if globalLockServer == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalIsDistErasure {
				if !globalIsDistErasure {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependGlobalBackgroundHealState {
				if globalBackgroundHealState == nil {
					return []MetricV2{}, nil
				}
			}
			if g.metricsGroupOpts.dependBucketTargetSys {
				if globalBucketTargetSys == nil {
					return []MetricV2{}, nil
				}
			}
			return read(GlobalContext), nil
		},
	)
}

func (m *MetricV2) clone() MetricV2 {
	metric := MetricV2{
		Description:          m.Description,
		Value:                m.Value,
		HistogramBucketLabel: m.HistogramBucketLabel,
		StaticLabels:         make(map[string]string, len(m.StaticLabels)),
		VariableLabels:       make(map[string]string, len(m.VariableLabels)),
		Histogram:            make(map[string]uint64, len(m.Histogram)),
	}
	maps.Copy(metric.StaticLabels, m.StaticLabels)
	maps.Copy(metric.VariableLabels, m.VariableLabels)
	maps.Copy(metric.Histogram, m.Histogram)
	return metric
}

// Get - returns cached value always upton the configured TTL,
// once the TTL expires "read()" registered function is called
// to return the new values and updated.
func (g *MetricsGroupV2) Get() (metrics []MetricV2) {
	m, _ := g.metricsCache.Get()
	if len(m) == 0 {
		return []MetricV2{}
	}

	metrics = make([]MetricV2, 0, len(m))
	for i := range m {
		metrics = append(metrics, m[i].clone())
	}
	return metrics
}

func getClusterBucketsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: bucketsSubsystem,
		Name:      total,
		Help:      "Total number of buckets in the cluster",
		Type:      gaugeMetric,
	}
}

func getClusterCapacityTotalBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: capacityRawSubsystem,
		Name:      totalBytes,
		Help:      "Total capacity online in the cluster",
		Type:      gaugeMetric,
	}
}

func getClusterCapacityFreeBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: capacityRawSubsystem,
		Name:      freeBytes,
		Help:      "Total free capacity online in the cluster",
		Type:      gaugeMetric,
	}
}

func getClusterCapacityUsageBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: capacityUsableSubsystem,
		Name:      totalBytes,
		Help:      "Total usable capacity online in the cluster",
		Type:      gaugeMetric,
	}
}

func getClusterCapacityUsageFreeBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: capacityUsableSubsystem,
		Name:      freeBytes,
		Help:      "Total free usable capacity online in the cluster",
		Type:      gaugeMetric,
	}
}

func getNodeDriveAPILatencyMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      latencyMicroSec,
		Help:      "Average last minute latency in µs for drive API storage operations",
		Type:      gaugeMetric,
	}
}

func getNodeDriveUsedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      usedBytes,
		Help:      "Total storage used on a drive",
		Type:      gaugeMetric,
	}
}

func getNodeDriveTimeoutErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      "errors_timeout",
		Help:      "Total number of drive timeout errors since server uptime",
		Type:      counterMetric,
	}
}

func getNodeDriveIOErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      "errors_ioerror",
		Help:      "Total number of drive I/O errors since server uptime",
		Type:      counterMetric,
	}
}

func getNodeDriveAvailabilityErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      "errors_availability",
		Help:      "Total number of drive I/O errors, timeouts since server uptime",
		Type:      counterMetric,
	}
}

func getNodeDriveWaitingIOMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      "io_waiting",
		Help:      "Total number I/O operations waiting on drive",
		Type:      counterMetric,
	}
}

func getNodeDriveFreeBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      freeBytes,
		Help:      "Total storage available on a drive",
		Type:      gaugeMetric,
	}
}

func getClusterDrivesOfflineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      offlineTotal,
		Help:      "Total drives offline in this cluster",
		Type:      gaugeMetric,
	}
}

func getClusterDrivesOnlineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      onlineTotal,
		Help:      "Total drives online in this cluster",
		Type:      gaugeMetric,
	}
}

func getClusterDrivesTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      total,
		Help:      "Total drives in this cluster",
		Type:      gaugeMetric,
	}
}

func getNodeDrivesOfflineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      offlineTotal,
		Help:      "Total drives offline in this node",
		Type:      gaugeMetric,
	}
}

func getNodeDrivesOnlineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      onlineTotal,
		Help:      "Total drives online in this node",
		Type:      gaugeMetric,
	}
}

func getNodeDrivesTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      total,
		Help:      "Total drives in this node",
		Type:      gaugeMetric,
	}
}

func getNodeStandardParityMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: storageClassSubsystem,
		Name:      "standard_parity",
		Help:      "standard storage class parity",
		Type:      gaugeMetric,
	}
}

func getNodeRRSParityMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: storageClassSubsystem,
		Name:      "rrs_parity",
		Help:      "reduced redundancy storage class parity",
		Type:      gaugeMetric,
	}
}

func getNodeDrivesFreeInodesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      freeInodes,
		Help:      "Free inodes on a drive",
		Type:      gaugeMetric,
	}
}

func getNodeDriveTotalBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: driveSubsystem,
		Name:      totalBytes,
		Help:      "Total storage on a drive",
		Type:      gaugeMetric,
	}
}

func getUsageLastScanActivityMD() MetricDescription {
	return MetricDescription{
		Namespace: minioMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      lastActivityTime,
		Help:      "Time elapsed (in nano seconds) since last scan activity",
		Type:      gaugeMetric,
	}
}

func getBucketUsageLastScanActivityMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      lastActivityTime,
		Help:      "Time elapsed (in nano seconds) since last scan activity",
		Type:      gaugeMetric,
	}
}

func getBucketUsageQuotaTotalBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: quotaSubsystem,
		Name:      totalBytes,
		Help:      "Total bucket quota size in bytes",
		Type:      gaugeMetric,
	}
}

func getBucketTrafficReceivedBytes() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      receivedBytes,
		Help:      "Total number of S3 bytes received for this bucket",
		Type:      gaugeMetric,
	}
}

func getBucketTrafficSentBytes() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      sentBytes,
		Help:      "Total number of S3 bytes sent for this bucket",
		Type:      gaugeMetric,
	}
}

func getBucketUsageTotalBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      totalBytes,
		Help:      "Total bucket size in bytes",
		Type:      gaugeMetric,
	}
}

func getClusterUsageTotalBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      totalBytes,
		Help:      "Total cluster usage in bytes",
		Type:      gaugeMetric,
	}
}

func getClusterUsageObjectsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      objectTotal,
		Help:      "Total number of objects in a cluster",
		Type:      gaugeMetric,
	}
}

func getClusterUsageVersionsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      versionTotal,
		Help:      "Total number of versions (includes delete marker) in a cluster",
		Type:      gaugeMetric,
	}
}

func getClusterUsageDeleteMarkersTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      deleteMarkerTotal,
		Help:      "Total number of delete markers in a cluster",
		Type:      gaugeMetric,
	}
}

func getBucketUsageObjectsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      objectTotal,
		Help:      "Total number of objects",
		Type:      gaugeMetric,
	}
}

func getBucketUsageVersionsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      versionTotal,
		Help:      "Total number of versions (includes delete marker)",
		Type:      gaugeMetric,
	}
}

func getBucketUsageDeleteMarkersTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: usageSubsystem,
		Name:      deleteMarkerTotal,
		Help:      "Total number of delete markers",
		Type:      gaugeMetric,
	}
}

func getClusterObjectDistributionMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      sizeDistribution,
		Help:      "Distribution of object sizes across a cluster",
		Type:      histogramMetric,
	}
}

func getClusterObjectVersionsMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      versionDistribution,
		Help:      "Distribution of object versions across a cluster",
		Type:      histogramMetric,
	}
}

func getClusterRepLinkLatencyCurrMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      currLinkLatency,
		Help:      "Replication current link latency in milliseconds",
		Type:      gaugeMetric,
	}
}

func getClusterRepLinkOnlineMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      linkOnline,
		Help:      "Reports whether replication link is online (1) or offline(0)",
		Type:      gaugeMetric,
	}
}

func getClusterRepLinkCurrOfflineDurationMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      linkOfflineDuration,
		Help:      "Duration of replication link being offline in seconds since last offline event",
		Type:      gaugeMetric,
	}
}

func getClusterRepLinkTotalOfflineDurationMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      linkDowntimeTotalDuration,
		Help:      "Total downtime of replication link in seconds since server uptime",
		Type:      gaugeMetric,
	}
}

func getBucketRepLatencyMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      latencyMilliSec,
		Help:      "Replication latency in milliseconds",
		Type:      histogramMetric,
	}
}

func getRepFailedBytesLastMinuteMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      lastMinFailedBytes,
		Help:      "Total number of bytes failed at least once to replicate in the last full minute",
		Type:      gaugeMetric,
	}
}

func getRepFailedOperationsLastMinuteMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      lastMinFailedCount,
		Help:      "Total number of objects which failed replication in the last full minute",
		Type:      gaugeMetric,
	}
}

func getRepFailedBytesLastHourMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      lastHourFailedBytes,
		Help:      "Total number of bytes failed at least once to replicate in the last hour",
		Type:      gaugeMetric,
	}
}

func getRepFailedOperationsLastHourMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      lastHourFailedCount,
		Help:      "Total number of objects which failed replication in the last hour",
		Type:      gaugeMetric,
	}
}

func getRepFailedBytesTotalMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      totalFailedBytes,
		Help:      "Total number of bytes failed at least once to replicate since server uptime",
		Type:      counterMetric,
	}
}

func getRepFailedOperationsTotalMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      totalFailedCount,
		Help:      "Total number of objects which failed replication since server uptime",
		Type:      counterMetric,
	}
}

func getRepSentBytesMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      sentBytes,
		Help:      "Total number of bytes replicated to the target",
		Type:      counterMetric,
	}
}

func getRepSentOperationsMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      sentCount,
		Help:      "Total number of objects replicated to the target",
		Type:      gaugeMetric,
	}
}

func getRepReceivedBytesMD(namespace MetricNamespace) MetricDescription {
	helpText := "Total number of bytes replicated to this bucket from another source bucket"
	if namespace == clusterMetricNamespace {
		helpText = "Total number of bytes replicated to this cluster from site replication peer"
	}
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      receivedBytes,
		Help:      helpText,
		Type:      counterMetric,
	}
}

func getRepReceivedOperationsMD(namespace MetricNamespace) MetricDescription {
	help := "Total number of objects received by this cluster"
	if namespace == bucketMetricNamespace {
		help = "Total number of objects received by this bucket from another source bucket"
	}
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      receivedCount,
		Help:      help,
		Type:      gaugeMetric,
	}
}

func getClusterReplMRFFailedOperationsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      recentBacklogCount,
		Help:      "Total number of objects seen in replication backlog in the last 5 minutes",
		Type:      gaugeMetric,
	}
}

func getClusterRepCredentialErrorsMD(namespace MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: namespace,
		Subsystem: replicationSubsystem,
		Name:      credentialErrors,
		Help:      "Total number of replication credential errors since server uptime",
		Type:      counterMetric,
	}
}

func getClusterReplCurrQueuedOperationsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      currInQueueCount,
		Help:      "Total number of objects queued for replication in the last full minute",
		Type:      gaugeMetric,
	}
}

func getClusterReplCurrQueuedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      currInQueueBytes,
		Help:      "Total number of bytes queued for replication in the last full minute",
		Type:      gaugeMetric,
	}
}

func getClusterReplActiveWorkersCountMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      currActiveWorkers,
		Help:      "Total number of active replication workers",
		Type:      gaugeMetric,
	}
}

func getClusterReplAvgActiveWorkersCountMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      avgActiveWorkers,
		Help:      "Average number of active replication workers",
		Type:      gaugeMetric,
	}
}

func getClusterReplMaxActiveWorkersCountMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      maxActiveWorkers,
		Help:      "Maximum number of active replication workers seen since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplCurrentTransferRateMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      currTransferRate,
		Help:      "Current replication transfer rate in bytes/sec",
		Type:      gaugeMetric,
	}
}

func getClusterRepLinkLatencyMaxMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      maxLinkLatency,
		Help:      "Maximum replication link latency in milliseconds seen since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterRepLinkLatencyAvgMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      avgLinkLatency,
		Help:      "Average replication link latency in milliseconds",
		Type:      gaugeMetric,
	}
}

func getClusterReplAvgQueuedOperationsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      avgInQueueCount,
		Help:      "Average number of objects queued for replication since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplAvgQueuedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      avgInQueueBytes,
		Help:      "Average number of bytes queued for replication since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplMaxQueuedOperationsMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      maxInQueueCount,
		Help:      "Maximum number of objects queued for replication since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplMaxQueuedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      maxInQueueBytes,
		Help:      "Maximum number of bytes queued for replication since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplAvgTransferRateMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      avgTransferRate,
		Help:      "Average replication transfer rate in bytes/sec",
		Type:      gaugeMetric,
	}
}

func getClusterReplMaxTransferRateMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: replicationSubsystem,
		Name:      maxTransferRate,
		Help:      "Maximum replication transfer rate in bytes/sec seen since server uptime",
		Type:      gaugeMetric,
	}
}

func getClusterReplProxiedGetOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedGetRequestsTotal,
		Help:      "Number of GET requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedHeadOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedHeadRequestsTotal,
		Help:      "Number of HEAD requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedPutTaggingOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedPutTaggingRequestsTotal,
		Help:      "Number of PUT tagging requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedGetTaggingOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedGetTaggingRequestsTotal,
		Help:      "Number of GET tagging requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedRmvTaggingOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedDeleteTaggingRequestsTotal,
		Help:      "Number of DELETE tagging requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedGetFailedOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedGetRequestsFailures,
		Help:      "Number of failures in GET requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedHeadFailedOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedHeadRequestsFailures,
		Help:      "Number of failures in HEAD requests proxied to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedPutTaggingFailedOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedPutTaggingRequestFailures,
		Help:      "Number of failures in PUT tagging proxy requests to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedGetTaggingFailedOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedGetTaggingRequestFailures,
		Help:      "Number of failures in GET tagging proxy requests to replication target",
		Type:      counterMetric,
	}
}

func getClusterReplProxiedRmvTaggingFailedOperationsMD(ns MetricNamespace) MetricDescription {
	return MetricDescription{
		Namespace: ns,
		Subsystem: replicationSubsystem,
		Name:      proxiedDeleteTaggingRequestFailures,
		Help:      "Number of  failures in DELETE tagging proxy requests to replication target",
		Type:      counterMetric,
	}
}

func getBucketObjectDistributionMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      sizeDistribution,
		Help:      "Distribution of object sizes in the bucket, includes label for the bucket name",
		Type:      histogramMetric,
	}
}

func getBucketObjectVersionsMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      versionDistribution,
		Help:      "Distribution of object sizes in the bucket, includes label for the bucket name",
		Type:      histogramMetric,
	}
}

func getInternodeFailedRequests() MetricDescription {
	return MetricDescription{
		Namespace: interNodeMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      errorsTotal,
		Help:      "Total number of failed internode calls",
		Type:      counterMetric,
	}
}

func getInternodeTCPDialTimeout() MetricDescription {
	return MetricDescription{
		Namespace: interNodeMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      "dial_errors",
		Help:      "Total number of internode TCP dial timeouts and errors",
		Type:      counterMetric,
	}
}

func getInternodeTCPAvgDuration() MetricDescription {
	return MetricDescription{
		Namespace: interNodeMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      "dial_avg_time",
		Help:      "Average time of internodes TCP dial calls",
		Type:      gaugeMetric,
	}
}

func getInterNodeSentBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: interNodeMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      sentBytes,
		Help:      "Total number of bytes sent to the other peer nodes",
		Type:      counterMetric,
	}
}

func getInterNodeReceivedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: interNodeMetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      receivedBytes,
		Help:      "Total number of bytes received from other peer nodes",
		Type:      counterMetric,
	}
}

func getS3SentBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      sentBytes,
		Help:      "Total number of s3 bytes sent",
		Type:      counterMetric,
	}
}

func getS3ReceivedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: trafficSubsystem,
		Name:      receivedBytes,
		Help:      "Total number of s3 bytes received",
		Type:      counterMetric,
	}
}

func getS3RequestsInFlightMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      inflightTotal,
		Help:      "Total number of S3 requests currently in flight",
		Type:      gaugeMetric,
	}
}

func getS3RequestsInQueueMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      waitingTotal,
		Help:      "Total number of S3 requests in the waiting queue",
		Type:      gaugeMetric,
	}
}

func getIncomingS3RequestsMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      incomingTotal,
		Help:      "Total number of incoming S3 requests",
		Type:      gaugeMetric,
	}
}

func getS3RequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      total,
		Help:      "Total number of S3 requests",
		Type:      counterMetric,
	}
}

func getS3RequestsErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      errorsTotal,
		Help:      "Total number of S3 requests with (4xx and 5xx) errors",
		Type:      counterMetric,
	}
}

func getS3Requests4xxErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      "4xx_" + errorsTotal,
		Help:      "Total number of S3 requests with (4xx) errors",
		Type:      counterMetric,
	}
}

func getS3Requests5xxErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      "5xx_" + errorsTotal,
		Help:      "Total number of S3 requests with (5xx) errors",
		Type:      counterMetric,
	}
}

func getS3RequestsCanceledMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      canceledTotal,
		Help:      "Total number of S3 requests that were canceled by the client",
		Type:      counterMetric,
	}
}

func getS3RejectedAuthRequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsRejectedSubsystem,
		Name:      authTotal,
		Help:      "Total number of S3 requests rejected for auth failure",
		Type:      counterMetric,
	}
}

func getS3RejectedHeaderRequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsRejectedSubsystem,
		Name:      headerTotal,
		Help:      "Total number of S3 requests rejected for invalid header",
		Type:      counterMetric,
	}
}

func getS3RejectedTimestampRequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsRejectedSubsystem,
		Name:      timestampTotal,
		Help:      "Total number of S3 requests rejected for invalid timestamp",
		Type:      counterMetric,
	}
}

func getS3RejectedInvalidRequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: requestsRejectedSubsystem,
		Name:      invalidTotal,
		Help:      "Total number of invalid S3 requests",
		Type:      counterMetric,
	}
}

func getHealObjectsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: healMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      total,
		Help:      "Objects scanned since server uptime",
		Type:      counterMetric,
	}
}

func getHealObjectsHealTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: healMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      healTotal,
		Help:      "Objects healed since server uptime",
		Type:      counterMetric,
	}
}

func getHealObjectsFailTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: healMetricNamespace,
		Subsystem: objectsSubsystem,
		Name:      errorsTotal,
		Help:      "Objects with healing failed since server uptime",
		Type:      counterMetric,
	}
}

func getHealLastActivityTimeMD() MetricDescription {
	return MetricDescription{
		Namespace: healMetricNamespace,
		Subsystem: timeSubsystem,
		Name:      lastActivityTime,
		Help:      "Time elapsed (in nano seconds) since last self healing activity",
		Type:      gaugeMetric,
	}
}

func getNodeOnlineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: nodesSubsystem,
		Name:      onlineTotal,
		Help:      "Total number of MinIO nodes online",
		Type:      gaugeMetric,
	}
}

func getNodeOfflineTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: nodesSubsystem,
		Name:      offlineTotal,
		Help:      "Total number of MinIO nodes offline",
		Type:      gaugeMetric,
	}
}

func getMinIOVersionMD() MetricDescription {
	return MetricDescription{
		Namespace: minioMetricNamespace,
		Subsystem: softwareSubsystem,
		Name:      versionInfo,
		Help:      "MinIO Release tag for the server",
		Type:      gaugeMetric,
	}
}

func getMinIOCommitMD() MetricDescription {
	return MetricDescription{
		Namespace: minioMetricNamespace,
		Subsystem: softwareSubsystem,
		Name:      commitInfo,
		Help:      "Git commit hash for the MinIO release",
		Type:      gaugeMetric,
	}
}

func getS3TTFBDistributionMD() MetricDescription {
	return MetricDescription{
		Namespace: s3MetricNamespace,
		Subsystem: ttfbSubsystem,
		Name:      ttfbDistribution,
		Help:      "Distribution of time to first byte across API calls",
		Type:      gaugeMetric,
	}
}

func getBucketTTFBDistributionMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: ttfbSubsystem,
		Name:      ttfbDistribution,
		Help:      "Distribution of time to first byte across API calls per bucket",
		Type:      gaugeMetric,
	}
}

func getMinioFDOpenMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: fileDescriptorSubsystem,
		Name:      openTotal,
		Help:      "Total number of open file descriptors by the MinIO Server process",
		Type:      gaugeMetric,
	}
}

func getMinioFDLimitMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: fileDescriptorSubsystem,
		Name:      limitTotal,
		Help:      "Limit on total number of open file descriptors for the MinIO Server process",
		Type:      gaugeMetric,
	}
}

func getMinioProcessIOWriteBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ioSubsystem,
		Name:      writeBytes,
		Help:      "Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes",
		Type:      counterMetric,
	}
}

func getMinioProcessIOReadBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ioSubsystem,
		Name:      readBytes,
		Help:      "Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes",
		Type:      counterMetric,
	}
}

func getMinioProcessIOWriteCachedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ioSubsystem,
		Name:      wcharBytes,
		Help:      "Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar",
		Type:      counterMetric,
	}
}

func getMinioProcessIOReadCachedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ioSubsystem,
		Name:      rcharBytes,
		Help:      "Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar",
		Type:      counterMetric,
	}
}

func getMinIOProcessSysCallRMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: sysCallSubsystem,
		Name:      readTotal,
		Help:      "Total read SysCalls to the kernel. /proc/[pid]/io syscr",
		Type:      counterMetric,
	}
}

func getMinIOProcessSysCallWMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: sysCallSubsystem,
		Name:      writeTotal,
		Help:      "Total write SysCalls to the kernel. /proc/[pid]/io syscw",
		Type:      counterMetric,
	}
}

func getMinIOGORoutineCountMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: goRoutines,
		Name:      total,
		Help:      "Total number of go routines running",
		Type:      gaugeMetric,
	}
}

func getMinIOProcessStartTimeMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: processSubsystem,
		Name:      startTime,
		Help:      "Start time for MinIO process per node, time in seconds since Unix epoc",
		Type:      gaugeMetric,
	}
}

func getMinIOProcessUptimeMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: processSubsystem,
		Name:      upTime,
		Help:      "Uptime for MinIO process per node in seconds",
		Type:      gaugeMetric,
	}
}

func getMinIOProcessResidentMemory() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: processSubsystem,
		Name:      memory,
		Help:      "Resident memory size in bytes",
		Type:      gaugeMetric,
	}
}

func getMinIOProcessVirtualMemory() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: processSubsystem,
		Name:      memory,
		Help:      "Virtual memory size in bytes",
		Type:      gaugeMetric,
	}
}

func getMinIOProcessCPUTime() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: processSubsystem,
		Name:      cpu,
		Help:      "Total user and system CPU time spent in seconds",
		Type:      counterMetric,
	}
}

func getMinioProcMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		if runtime.GOOS == globalWindowsOSName || runtime.GOOS == globalMacOSName {
			return nil
		}

		p, err := procfs.Self()
		if err != nil {
			internalLogOnceIf(ctx, err, string(nodeMetricNamespace))
			return metrics
		}

		openFDs, _ := p.FileDescriptorsLen()
		l, _ := p.Limits()
		io, _ := p.IO()
		stat, _ := p.Stat()
		startTime, _ := stat.StartTime()

		metrics = make([]MetricV2, 0, 20)

		if openFDs > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioFDOpenMD(),
					Value:       float64(openFDs),
				},
			)
		}

		if l.OpenFiles > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioFDLimitMD(),
					Value:       float64(l.OpenFiles),
				})
		}

		if io.SyscR > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessSysCallRMD(),
					Value:       float64(io.SyscR),
				})
		}

		if io.SyscW > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessSysCallWMD(),
					Value:       float64(io.SyscW),
				})
		}

		if io.ReadBytes > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioProcessIOReadBytesMD(),
					Value:       float64(io.ReadBytes),
				})
		}

		if io.WriteBytes > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioProcessIOWriteBytesMD(),
					Value:       float64(io.WriteBytes),
				})
		}

		if io.RChar > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioProcessIOReadCachedBytesMD(),
					Value:       float64(io.RChar),
				})
		}

		if io.WChar > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinioProcessIOWriteCachedBytesMD(),
					Value:       float64(io.WChar),
				})
		}

		if startTime > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessStartTimeMD(),
					Value:       startTime,
				})
		}

		if !globalBootTime.IsZero() {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessUptimeMD(),
					Value:       time.Since(globalBootTime).Seconds(),
				})
		}

		if stat.ResidentMemory() > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessResidentMemory(),
					Value:       float64(stat.ResidentMemory()),
				})
		}

		if stat.VirtualMemory() > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessVirtualMemory(),
					Value:       float64(stat.VirtualMemory()),
				})
		}

		if stat.CPUTime() > 0 {
			metrics = append(metrics,
				MetricV2{
					Description: getMinIOProcessCPUTime(),
					Value:       stat.CPUTime(),
				})
		}
		return metrics
	})
	return mg
}

func getGoMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		metrics = append(metrics, MetricV2{
			Description: getMinIOGORoutineCountMD(),
			Value:       float64(runtime.NumGoroutine()),
		})
		return metrics
	})
	return mg
}

// getHistogramMetrics fetches histogram metrics and returns it in a []Metric
// Note: Typically used in MetricGroup.RegisterRead
//
// The toLowerAPILabels parameter is added for compatibility,
// if set, it lowercases the `api` label values.
func getHistogramMetrics(hist *prometheus.HistogramVec, desc MetricDescription, toLowerAPILabels, limitBuckets bool) []MetricV2 {
	ch := make(chan prometheus.Metric)
	go func() {
		defer xioutil.SafeClose(ch)
		// Collects prometheus metrics from hist and sends it over ch
		hist.Collect(ch)
	}()

	// Converts metrics received into internal []Metric type
	var metrics []MetricV2
	buckets := make(map[string][]MetricV2, v2MetricsMaxBuckets)
	for promMetric := range ch {
		dtoMetric := &dto.Metric{}
		err := promMetric.Write(dtoMetric)
		if err != nil {
			// Log error and continue to receive other metric
			// values
			bugLogIf(GlobalContext, err)
			continue
		}

		h := dtoMetric.GetHistogram()
		for _, b := range h.Bucket {
			labels := make(map[string]string)
			for _, lp := range dtoMetric.GetLabel() {
				if *lp.Name == "api" && toLowerAPILabels {
					labels[*lp.Name] = strings.ToLower(*lp.Value)
				} else {
					labels[*lp.Name] = *lp.Value
				}
			}
			labels["le"] = fmt.Sprintf("%.3f", *b.UpperBound)
			metric := MetricV2{
				Description:    desc,
				VariableLabels: labels,
				Value:          float64(b.GetCumulativeCount()),
			}
			if limitBuckets && labels["bucket"] != "" {
				buckets[labels["bucket"]] = append(buckets[labels["bucket"]], metric)
			} else {
				metrics = append(metrics, metric)
			}
		}
		// add metrics with +Inf label
		labels1 := make(map[string]string)
		for _, lp := range dtoMetric.GetLabel() {
			if *lp.Name == "api" && toLowerAPILabels {
				labels1[*lp.Name] = strings.ToLower(*lp.Value)
			} else {
				labels1[*lp.Name] = *lp.Value
			}
		}
		labels1["le"] = fmt.Sprintf("%.3f", math.Inf(+1))

		metric := MetricV2{
			Description:    desc,
			VariableLabels: labels1,
			Value:          float64(dtoMetric.Histogram.GetSampleCount()),
		}
		if limitBuckets && labels1["bucket"] != "" {
			buckets[labels1["bucket"]] = append(buckets[labels1["bucket"]], metric)
		} else {
			metrics = append(metrics, metric)
		}
	}

	// Limit bucket metrics...
	if limitBuckets {
		bucketNames := mapKeysSorted(buckets)
		bucketNames = bucketNames[:min(len(buckets), v2MetricsMaxBuckets)]
		for _, b := range bucketNames {
			metrics = append(metrics, buckets[b]...)
		}
	}
	return metrics
}

func getBucketTTFBMetric() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		return getHistogramMetrics(bucketHTTPRequestsDuration,
			getBucketTTFBDistributionMD(), true, true)
	})
	return mg
}

func getS3TTFBMetric() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		return getHistogramMetrics(httpRequestsDuration,
			getS3TTFBDistributionMD(), true, true)
	})
	return mg
}

func getTierMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		return globalTierMetrics.Report()
	})
	return mg
}

func getTransitionPendingTasksMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionPendingTasks,
		Help:      "Number of pending ILM transition tasks in the queue",
		Type:      gaugeMetric,
	}
}

func getTransitionActiveTasksMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionActiveTasks,
		Help:      "Number of active ILM transition tasks",
		Type:      gaugeMetric,
	}
}

func getTransitionMissedTasksMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionMissedTasks,
		Help:      "Number of missed immediate ILM transition tasks",
		Type:      gaugeMetric,
	}
}

func getExpiryPendingTasksMD() MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      expiryPendingTasks,
		Help:      "Number of pending ILM expiry tasks in the queue",
		Type:      gaugeMetric,
	}
}

func getBucketS3RequestsInFlightMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      inflightTotal,
		Help:      "Total number of S3 requests currently in flight on a bucket",
		Type:      gaugeMetric,
	}
}

func getBucketS3RequestsTotalMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      total,
		Help:      "Total number of S3 requests on a bucket",
		Type:      counterMetric,
	}
}

func getBucketS3Requests4xxErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      "4xx_" + errorsTotal,
		Help:      "Total number of S3 requests with (4xx) errors on a bucket",
		Type:      counterMetric,
	}
}

func getBucketS3Requests5xxErrorsMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      "5xx_" + errorsTotal,
		Help:      "Total number of S3 requests with (5xx) errors on a bucket",
		Type:      counterMetric,
	}
}

func getBucketS3RequestsCanceledMD() MetricDescription {
	return MetricDescription{
		Namespace: bucketMetricNamespace,
		Subsystem: requestsSubsystem,
		Name:      canceledTotal,
		Help:      "Total number of S3 requests that were canceled from the client while processing on a bucket",
		Type:      counterMetric,
	}
}

func getILMNodeMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(_ context.Context) []MetricV2 {
		expPendingTasks := MetricV2{
			Description: getExpiryPendingTasksMD(),
		}
		expMissedTasks := MetricV2{
			Description: MetricDescription{
				Namespace: nodeMetricNamespace,
				Subsystem: ilmSubsystem,
				Name:      expiryMissedTasks,
				Help:      "Number of object version expiry missed due to busy system",
				Type:      counterMetric,
			},
		}
		expMissedFreeVersions := MetricV2{
			Description: MetricDescription{
				Namespace: nodeMetricNamespace,
				Subsystem: ilmSubsystem,
				Name:      expiryMissedFreeVersions,
				Help:      "Number of free versions expiry missed due to busy system",
				Type:      counterMetric,
			},
		}
		expMissedTierJournalTasks := MetricV2{
			Description: MetricDescription{
				Namespace: nodeMetricNamespace,
				Subsystem: ilmSubsystem,
				Name:      expiryMissedTierJournalTasks,
				Help:      "Number of tier journal entries cleanup missed due to busy system",
				Type:      counterMetric,
			},
		}
		expNumWorkers := MetricV2{
			Description: MetricDescription{
				Namespace: nodeMetricNamespace,
				Subsystem: ilmSubsystem,
				Name:      expiryNumWorkers,
				Help:      "Number of workers expiring object versions currently",
				Type:      gaugeMetric,
			},
		}
		trPendingTasks := MetricV2{
			Description: getTransitionPendingTasksMD(),
		}
		trActiveTasks := MetricV2{
			Description: getTransitionActiveTasksMD(),
		}
		trMissedTasks := MetricV2{
			Description: getTransitionMissedTasksMD(),
		}
		if globalExpiryState != nil {
			expPendingTasks.Value = float64(globalExpiryState.PendingTasks())
			expMissedTasks.Value = float64(globalExpiryState.stats.MissedTasks())
			expMissedFreeVersions.Value = float64(globalExpiryState.stats.MissedFreeVersTasks())
			expMissedTierJournalTasks.Value = float64(globalExpiryState.stats.MissedTierJournalTasks())
			expNumWorkers.Value = float64(globalExpiryState.stats.NumWorkers())
		}
		if globalTransitionState != nil {
			trPendingTasks.Value = float64(globalTransitionState.PendingTasks())
			trActiveTasks.Value = float64(globalTransitionState.ActiveTasks())
			trMissedTasks.Value = float64(globalTransitionState.MissedImmediateTasks())
		}
		return []MetricV2{
			expPendingTasks,
			expMissedTasks,
			expMissedFreeVersions,
			expMissedTierJournalTasks,
			expNumWorkers,
			trPendingTasks,
			trActiveTasks,
			trMissedTasks,
		}
	})
	return mg
}

func getScannerNodeMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(_ context.Context) []MetricV2 {
		metrics := []MetricV2{
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: scannerSubsystem,
					Name:      "objects_scanned",
					Help:      "Total number of unique objects scanned since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricScanObject)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: scannerSubsystem,
					Name:      "versions_scanned",
					Help:      "Total number of object versions scanned since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricApplyVersion)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: scannerSubsystem,
					Name:      "directories_scanned",
					Help:      "Total number of directories scanned since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricScanFolder)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: scannerSubsystem,
					Name:      "bucket_scans_started",
					Help:      "Total number of bucket scans started since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricScanBucketDrive) + uint64(globalScannerMetrics.activeDrives())),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: scannerSubsystem,
					Name:      "bucket_scans_finished",
					Help:      "Total number of bucket scans finished since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricScanBucketDrive)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: ilmSubsystem,
					Name:      "versions_scanned",
					Help:      "Total number of object versions checked for ilm actions since server uptime",
					Type:      counterMetric,
				},
				Value: float64(globalScannerMetrics.lifetime(scannerMetricILM)),
			},
		}
		for i := range globalScannerMetrics.actions {
			action := lifecycle.Action(i)
			v := globalScannerMetrics.lifetimeActions(action)
			if v == 0 {
				continue
			}
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: ilmSubsystem,
					Name:      MetricName("action_count_" + toSnake(action.String())),
					Help:      "Total action outcome of lifecycle checks since server uptime",
					Type:      counterMetric,
				},
				Value: float64(v),
			})
		}
		return metrics
	})
	return mg
}

func getIAMNodeMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(_ context.Context) (metrics []MetricV2) {
		lastSyncTime := atomic.LoadUint64(&globalIAMSys.LastRefreshTimeUnixNano)
		var sinceLastSyncMillis uint64
		if lastSyncTime != 0 {
			sinceLastSyncMillis = (uint64(time.Now().UnixNano()) - lastSyncTime) / uint64(time.Millisecond)
		}

		pluginAuthNMetrics := globalAuthNPlugin.Metrics()
		metrics = []MetricV2{
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "last_sync_duration_millis",
					Help:      "Last successful IAM data sync duration in milliseconds",
					Type:      gaugeMetric,
				},
				Value: float64(atomic.LoadUint64(&globalIAMSys.LastRefreshDurationMilliseconds)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "since_last_sync_millis",
					Help:      "Time (in milliseconds) since last successful IAM data sync",
					Type:      gaugeMetric,
				},
				Value: float64(sinceLastSyncMillis),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "sync_successes",
					Help:      "Number of successful IAM data syncs since server uptime",
					Type:      counterMetric,
				},
				Value: float64(atomic.LoadUint64(&globalIAMSys.TotalRefreshSuccesses)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "sync_failures",
					Help:      "Number of failed IAM data syncs since server uptime",
					Type:      counterMetric,
				},
				Value: float64(atomic.LoadUint64(&globalIAMSys.TotalRefreshFailures)),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_last_succ_seconds",
					Help:      "When plugin authentication is configured, returns time (in seconds) since the last successful request to the service",
					Type:      gaugeMetric,
				},
				Value: pluginAuthNMetrics.LastReachableSecs,
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_last_fail_seconds",
					Help:      "When plugin authentication is configured, returns time (in seconds) since the last failed request to the service",
					Type:      gaugeMetric,
				},
				Value: pluginAuthNMetrics.LastUnreachableSecs,
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_total_requests_minute",
					Help:      "When plugin authentication is configured, returns total requests count in the last full minute",
					Type:      gaugeMetric,
				},
				Value: float64(pluginAuthNMetrics.TotalRequests),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_failed_requests_minute",
					Help:      "When plugin authentication is configured, returns failed requests count in the last full minute",
					Type:      gaugeMetric,
				},
				Value: float64(pluginAuthNMetrics.FailedRequests),
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_succ_avg_rtt_ms_minute",
					Help:      "When plugin authentication is configured, returns average round-trip-time of successful requests in the last full minute",
					Type:      gaugeMetric,
				},
				Value: pluginAuthNMetrics.AvgSuccRTTMs,
			},
			{
				Description: MetricDescription{
					Namespace: nodeMetricNamespace,
					Subsystem: iamSubsystem,
					Name:      "plugin_authn_service_succ_max_rtt_ms_minute",
					Help:      "When plugin authentication is configured, returns maximum round-trip-time of successful requests in the last full minute",
					Type:      gaugeMetric,
				},
				Value: pluginAuthNMetrics.MaxSuccRTTMs,
			},
		}

		return metrics
	})
	return mg
}

// replication metrics for each node - published to the cluster endpoint with nodename as label
func getReplicationNodeMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	const (
		Online  = 1
		Offline = 0
	)

	mg.RegisterRead(func(_ context.Context) []MetricV2 {
		var ml []MetricV2
		// common operational metrics for bucket replication and site replication - published
		// at cluster level
		if rStats := globalReplicationStats.Load(); rStats != nil {
			qs := rStats.getNodeQueueStatsSummary()
			activeWorkersCount := MetricV2{
				Description: getClusterReplActiveWorkersCountMD(),
			}
			avgActiveWorkersCount := MetricV2{
				Description: getClusterReplAvgActiveWorkersCountMD(),
			}
			maxActiveWorkersCount := MetricV2{
				Description: getClusterReplMaxActiveWorkersCountMD(),
			}
			currInQueueCount := MetricV2{
				Description: getClusterReplCurrQueuedOperationsMD(),
			}
			currInQueueBytes := MetricV2{
				Description: getClusterReplCurrQueuedBytesMD(),
			}

			currTransferRate := MetricV2{
				Description: getClusterReplCurrentTransferRateMD(),
			}
			avgQueueCount := MetricV2{
				Description: getClusterReplAvgQueuedOperationsMD(),
			}
			avgQueueBytes := MetricV2{
				Description: getClusterReplAvgQueuedBytesMD(),
			}
			maxQueueCount := MetricV2{
				Description: getClusterReplMaxQueuedOperationsMD(),
			}
			maxQueueBytes := MetricV2{
				Description: getClusterReplMaxQueuedBytesMD(),
			}
			avgTransferRate := MetricV2{
				Description: getClusterReplAvgTransferRateMD(),
			}
			maxTransferRate := MetricV2{
				Description: getClusterReplMaxTransferRateMD(),
			}
			mrfCount := MetricV2{
				Description: getClusterReplMRFFailedOperationsMD(),
				Value:       float64(qs.MRFStats.LastFailedCount),
			}

			if qs.QStats.Avg.Count > 0 || qs.QStats.Curr.Count > 0 {
				qt := qs.QStats
				currInQueueBytes.Value = qt.Curr.Bytes
				currInQueueCount.Value = qt.Curr.Count
				avgQueueBytes.Value = qt.Avg.Bytes
				avgQueueCount.Value = qt.Avg.Count
				maxQueueBytes.Value = qt.Max.Bytes
				maxQueueCount.Value = qt.Max.Count
			}
			activeWorkersCount.Value = float64(qs.ActiveWorkers.Curr)
			avgActiveWorkersCount.Value = float64(qs.ActiveWorkers.Avg)
			maxActiveWorkersCount.Value = float64(qs.ActiveWorkers.Max)

			if len(qs.XferStats) > 0 {
				tots := qs.XferStats[Total]
				currTransferRate.Value = tots.Curr
				avgTransferRate.Value = tots.Avg
				maxTransferRate.Value = tots.Peak
			}
			ml = []MetricV2{
				activeWorkersCount,
				avgActiveWorkersCount,
				maxActiveWorkersCount,
				currInQueueCount,
				currInQueueBytes,
				avgQueueCount,
				avgQueueBytes,
				maxQueueCount,
				maxQueueBytes,
				currTransferRate,
				avgTransferRate,
				maxTransferRate,
				mrfCount,
			}
		}
		for ep, health := range globalBucketTargetSys.healthStats() {
			// link latency current
			m := MetricV2{
				Description: getClusterRepLinkLatencyCurrMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			m.Value = float64(health.latency.curr / time.Millisecond)
			ml = append(ml, m)

			// link latency average
			m = MetricV2{
				Description: getClusterRepLinkLatencyAvgMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			m.Value = float64(health.latency.avg / time.Millisecond)
			ml = append(ml, m)

			// link latency max
			m = MetricV2{
				Description: getClusterRepLinkLatencyMaxMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			m.Value = float64(health.latency.peak / time.Millisecond)
			ml = append(ml, m)

			linkOnline := MetricV2{
				Description: getClusterRepLinkOnlineMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			online := Offline
			if health.Online {
				online = Online
			}
			linkOnline.Value = float64(online)
			ml = append(ml, linkOnline)
			offlineDuration := MetricV2{
				Description: getClusterRepLinkCurrOfflineDurationMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			currDowntime := time.Duration(0)
			if !health.Online && !health.lastOnline.IsZero() {
				currDowntime = UTCNow().Sub(health.lastOnline)
			}
			offlineDuration.Value = float64(currDowntime / time.Second)
			ml = append(ml, offlineDuration)

			downtimeDuration := MetricV2{
				Description: getClusterRepLinkTotalOfflineDurationMD(),
				VariableLabels: map[string]string{
					"endpoint": ep,
				},
			}
			dwntime := max(health.offlineDuration, currDowntime)
			downtimeDuration.Value = float64(dwntime / time.Second)
			ml = append(ml, downtimeDuration)
		}
		return ml
	})
	return mg
}

// replication metrics for site replication
func getReplicationSiteMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(_ context.Context) []MetricV2 {
		ml := []MetricV2{}

		// metrics pertinent to site replication - overall roll up.
		if globalSiteReplicationSys.isEnabled() {
			m, err := globalSiteReplicationSys.getSiteMetrics(GlobalContext)
			if err != nil {
				metricsLogIf(GlobalContext, err)
				return ml
			}
			ml = append(ml, MetricV2{
				Description: getRepReceivedBytesMD(clusterMetricNamespace),
				Value:       float64(m.ReplicaSize),
			})
			ml = append(ml, MetricV2{
				Description: getRepReceivedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.ReplicaCount),
			})

			for _, stat := range m.Metrics {
				ml = append(ml, MetricV2{
					Description:    getRepFailedBytesLastMinuteMD(clusterMetricNamespace),
					Value:          float64(stat.Failed.LastMinute.Bytes),
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepFailedOperationsLastMinuteMD(clusterMetricNamespace),
					Value:          stat.Failed.LastMinute.Count,
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepFailedBytesLastHourMD(clusterMetricNamespace),
					Value:          float64(stat.Failed.LastHour.Bytes),
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepFailedOperationsLastHourMD(clusterMetricNamespace),
					Value:          stat.Failed.LastHour.Count,
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepFailedBytesTotalMD(clusterMetricNamespace),
					Value:          float64(stat.Failed.Totals.Bytes),
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepFailedOperationsTotalMD(clusterMetricNamespace),
					Value:          stat.Failed.Totals.Count,
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})

				ml = append(ml, MetricV2{
					Description:    getRepSentBytesMD(clusterMetricNamespace),
					Value:          float64(stat.ReplicatedSize),
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})
				ml = append(ml, MetricV2{
					Description:    getRepSentOperationsMD(clusterMetricNamespace),
					Value:          float64(stat.ReplicatedCount),
					VariableLabels: map[string]string{"endpoint": stat.Endpoint},
				})

				if c, ok := stat.Failed.ErrCounts["AccessDenied"]; ok {
					ml = append(ml, MetricV2{
						Description:    getClusterRepCredentialErrorsMD(clusterMetricNamespace),
						Value:          float64(c),
						VariableLabels: map[string]string{"endpoint": stat.Endpoint},
					})
				}
			}
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedGetOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.GetTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedHeadOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.HeadTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedPutTaggingOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.PutTagTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedGetTaggingOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.GetTagTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedRmvTaggingOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.RmvTagTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedGetFailedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.GetFailedTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedHeadFailedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.HeadFailedTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedPutTaggingFailedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.PutTagFailedTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedGetTaggingFailedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.GetTagFailedTotal),
			})
			ml = append(ml, MetricV2{
				Description: getClusterReplProxiedRmvTaggingFailedOperationsMD(clusterMetricNamespace),
				Value:       float64(m.Proxied.RmvTagFailedTotal),
			})
		}

		return ml
	})
	return mg
}

func getMinioVersionMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(_ context.Context) (metrics []MetricV2) {
		metrics = append(metrics, MetricV2{
			Description:    getMinIOCommitMD(),
			VariableLabels: map[string]string{"commit": CommitID},
		})
		metrics = append(metrics, MetricV2{
			Description:    getMinIOVersionMD(),
			VariableLabels: map[string]string{"version": Version},
		})
		return metrics
	})
	return mg
}

func getNodeHealthMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(_ context.Context) (metrics []MetricV2) {
		metrics = make([]MetricV2, 0, 16)
		nodesUp, nodesDown := globalNotificationSys.GetPeerOnlineCount()
		metrics = append(metrics, MetricV2{
			Description: getNodeOnlineTotalMD(),
			Value:       float64(nodesUp),
		})
		metrics = append(metrics, MetricV2{
			Description: getNodeOfflineTotalMD(),
			Value:       float64(nodesDown),
		})
		return metrics
	})
	return mg
}

func getMinioHealingMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(_ context.Context) (metrics []MetricV2) {
		bgSeq, exists := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if !exists {
			return metrics
		}

		if bgSeq.lastHealActivity.IsZero() {
			return metrics
		}

		metrics = make([]MetricV2, 0, 5)
		metrics = append(metrics, MetricV2{
			Description: getHealLastActivityTimeMD(),
			Value:       float64(time.Since(bgSeq.lastHealActivity)),
		})
		metrics = append(metrics, getObjectsScanned(bgSeq)...)
		metrics = append(metrics, getHealedItems(bgSeq)...)
		metrics = append(metrics, getFailedItems(bgSeq)...)
		return metrics
	})
	return mg
}

func getFailedItems(seq *healSequence) (m []MetricV2) {
	items := seq.getHealFailedItemsMap()
	m = make([]MetricV2, 0, len(items))
	for k, v := range items {
		m = append(m, MetricV2{
			Description:    getHealObjectsFailTotalMD(),
			VariableLabels: map[string]string{"type": string(k)},
			Value:          float64(v),
		})
	}
	return m
}

func getHealedItems(seq *healSequence) (m []MetricV2) {
	items := seq.getHealedItemsMap()
	m = make([]MetricV2, 0, len(items))
	for k, v := range items {
		m = append(m, MetricV2{
			Description:    getHealObjectsHealTotalMD(),
			VariableLabels: map[string]string{"type": string(k)},
			Value:          float64(v),
		})
	}
	return m
}

func getObjectsScanned(seq *healSequence) (m []MetricV2) {
	items := seq.getScannedItemsMap()
	m = make([]MetricV2, 0, len(items))
	for k, v := range items {
		m = append(m, MetricV2{
			Description:    getHealObjectsTotalMD(),
			VariableLabels: map[string]string{"type": string(k)},
			Value:          float64(v),
		})
	}
	return m
}

func getDistLockMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		if !globalIsDistErasure {
			return []MetricV2{}
		}

		st := globalLockServer.stats()

		metrics := make([]MetricV2, 0, 3)
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: minioNamespace,
				Subsystem: "locks",
				Name:      "total",
				Help:      "Number of current locks on this peer",
				Type:      gaugeMetric,
			},
			Value: float64(st.Total),
		})
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: minioNamespace,
				Subsystem: "locks",
				Name:      "write_total",
				Help:      "Number of current WRITE locks on this peer",
				Type:      gaugeMetric,
			},
			Value: float64(st.Writes),
		})
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: minioNamespace,
				Subsystem: "locks",
				Name:      "read_total",
				Help:      "Number of current READ locks on this peer",
				Type:      gaugeMetric,
			},
			Value: float64(st.Reads),
		})
		return metrics
	})
	return mg
}

func getNotificationMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		metrics := make([]MetricV2, 0, 3)

		if globalEventNotifier != nil {
			nstats := globalEventNotifier.targetList.Stats()
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: notifySubsystem,
					Name:      "current_send_in_progress",
					Help:      "Number of concurrent async Send calls active to all targets (deprecated, please use 'minio_notify_target_current_send_in_progress' instead)",
					Type:      gaugeMetric,
				},
				Value: float64(nstats.CurrentSendCalls),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: notifySubsystem,
					Name:      "events_skipped_total",
					Help:      "Events that were skipped to be sent to the targets due to the in-memory queue being full",
					Type:      counterMetric,
				},
				Value: float64(nstats.EventsSkipped),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: notifySubsystem,
					Name:      "events_errors_total",
					Help:      "Events that were failed to be sent to the targets (deprecated, please use 'minio_notify_target_failed_events' instead)",
					Type:      counterMetric,
				},
				Value: float64(nstats.EventsErrorsTotal),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: notifySubsystem,
					Name:      "events_sent_total",
					Help:      "Total number of events sent to the targets (deprecated, please use 'minio_notify_target_total_events' instead)",
					Type:      counterMetric,
				},
				Value: float64(nstats.TotalEvents),
			})
			for id, st := range nstats.TargetStats {
				metrics = append(metrics, MetricV2{
					Description: MetricDescription{
						Namespace: minioNamespace,
						Subsystem: notifySubsystem,
						Name:      "target_total_events",
						Help:      "Total number of events sent (or) queued to the target",
						Type:      counterMetric,
					},
					VariableLabels: map[string]string{"target_id": id.ID, "target_name": id.Name},
					Value:          float64(st.TotalEvents),
				})
				metrics = append(metrics, MetricV2{
					Description: MetricDescription{
						Namespace: minioNamespace,
						Subsystem: notifySubsystem,
						Name:      "target_failed_events",
						Help:      "Number of events failed to be sent (or) queued to the target",
						Type:      counterMetric,
					},
					VariableLabels: map[string]string{"target_id": id.ID, "target_name": id.Name},
					Value:          float64(st.FailedEvents),
				})
				metrics = append(metrics, MetricV2{
					Description: MetricDescription{
						Namespace: minioNamespace,
						Subsystem: notifySubsystem,
						Name:      "target_current_send_in_progress",
						Help:      "Number of concurrent async Send calls active to the target",
						Type:      gaugeMetric,
					},
					VariableLabels: map[string]string{"target_id": id.ID, "target_name": id.Name},
					Value:          float64(st.CurrentSendCalls),
				})
				metrics = append(metrics, MetricV2{
					Description: MetricDescription{
						Namespace: minioNamespace,
						Subsystem: notifySubsystem,
						Name:      "target_queue_length",
						Help:      "Number of events currently staged in the queue_dir configured for the target",
						Type:      gaugeMetric,
					},
					VariableLabels: map[string]string{"target_id": id.ID, "target_name": id.Name},
					Value:          float64(st.CurrentQueue),
				})
			}
		}

		lstats := globalLambdaTargetList.Stats()
		for _, st := range lstats.TargetStats {
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: lambdaSubsystem,
					Name:      "active_requests",
					Help:      "Number of in progress requests",
				},
				VariableLabels: map[string]string{"target_id": st.ID.ID, "target_name": st.ID.Name},
				Value:          float64(st.ActiveRequests),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: lambdaSubsystem,
					Name:      "total_requests",
					Help:      "Total number of requests sent since start",
					Type:      counterMetric,
				},
				VariableLabels: map[string]string{"target_id": st.ID.ID, "target_name": st.ID.Name},
				Value:          float64(st.TotalRequests),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: lambdaSubsystem,
					Name:      "failed_requests",
					Help:      "Total number of requests that failed to send since start",
					Type:      counterMetric,
				},
				VariableLabels: map[string]string{"target_id": st.ID.ID, "target_name": st.ID.Name},
				Value:          float64(st.FailedRequests),
			})
		}

		// Audit and system:
		audit := logger.CurrentStats()
		for id, st := range audit {
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: auditSubsystem,
					Name:      "target_queue_length",
					Help:      "Number of unsent messages in queue for target",
					Type:      gaugeMetric,
				},
				VariableLabels: map[string]string{"target_id": id},
				Value:          float64(st.QueueLength),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: auditSubsystem,
					Name:      "total_messages",
					Help:      "Total number of messages sent since start",
					Type:      counterMetric,
				},
				VariableLabels: map[string]string{"target_id": id},
				Value:          float64(st.TotalMessages),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: minioNamespace,
					Subsystem: auditSubsystem,
					Name:      "failed_messages",
					Help:      "Total number of messages that failed to send since start",
					Type:      counterMetric,
				},
				VariableLabels: map[string]string{"target_id": id},
				Value:          float64(st.FailedMessages),
			})
		}
		return metrics
	})
	return mg
}

func getHTTPMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		if !mg.metricsGroupOpts.bucketOnly {
			httpStats := globalHTTPStats.toServerHTTPStats(true)
			metrics = make([]MetricV2, 0, 3+
				len(httpStats.CurrentS3Requests.APIStats)+
				len(httpStats.TotalS3Requests.APIStats)+
				len(httpStats.TotalS3Errors.APIStats)+
				len(httpStats.TotalS35xxErrors.APIStats)+
				len(httpStats.TotalS34xxErrors.APIStats))
			metrics = append(metrics, MetricV2{
				Description: getS3RejectedAuthRequestsTotalMD(),
				Value:       float64(httpStats.TotalS3RejectedAuth),
			})
			metrics = append(metrics, MetricV2{
				Description: getS3RejectedTimestampRequestsTotalMD(),
				Value:       float64(httpStats.TotalS3RejectedTime),
			})
			metrics = append(metrics, MetricV2{
				Description: getS3RejectedHeaderRequestsTotalMD(),
				Value:       float64(httpStats.TotalS3RejectedHeader),
			})
			metrics = append(metrics, MetricV2{
				Description: getS3RejectedInvalidRequestsTotalMD(),
				Value:       float64(httpStats.TotalS3RejectedInvalid),
			})
			metrics = append(metrics, MetricV2{
				Description: getS3RequestsInQueueMD(),
				Value:       float64(httpStats.S3RequestsInQueue),
			})
			metrics = append(metrics, MetricV2{
				Description: getIncomingS3RequestsMD(),
				Value:       float64(httpStats.S3RequestsIncoming),
			})

			for api, value := range httpStats.CurrentS3Requests.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3RequestsInFlightMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			for api, value := range httpStats.TotalS3Requests.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3RequestsTotalMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			for api, value := range httpStats.TotalS3Errors.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3RequestsErrorsMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			for api, value := range httpStats.TotalS35xxErrors.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3Requests5xxErrorsMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			for api, value := range httpStats.TotalS34xxErrors.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3Requests4xxErrorsMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			for api, value := range httpStats.TotalS3Canceled.APIStats {
				metrics = append(metrics, MetricV2{
					Description:    getS3RequestsCanceledMD(),
					Value:          float64(value),
					VariableLabels: map[string]string{"api": api},
				})
			}
			return metrics
		}

		// If we have too many, limit them
		bConnStats := globalBucketConnStats.getS3InOutBytes()
		buckets := mapKeysSorted(bConnStats)
		buckets = buckets[:min(v2MetricsMaxBuckets, len(buckets))]

		for _, bucket := range buckets {
			inOut := bConnStats[bucket]
			recvBytes := inOut.In
			if recvBytes > 0 {
				metrics = append(metrics, MetricV2{
					Description:    getBucketTrafficReceivedBytes(),
					Value:          float64(recvBytes),
					VariableLabels: map[string]string{"bucket": bucket},
				})
			}
			sentBytes := inOut.Out
			if sentBytes > 0 {
				metrics = append(metrics, MetricV2{
					Description:    getBucketTrafficSentBytes(),
					Value:          float64(sentBytes),
					VariableLabels: map[string]string{"bucket": bucket},
				})
			}

			httpStats := globalBucketHTTPStats.load(bucket)
			for k, v := range httpStats.currentS3Requests.Load(true) {
				metrics = append(metrics, MetricV2{
					Description:    getBucketS3RequestsInFlightMD(),
					Value:          float64(v),
					VariableLabels: map[string]string{"bucket": bucket, "api": k},
				})
			}

			for k, v := range httpStats.totalS3Requests.Load(true) {
				metrics = append(metrics, MetricV2{
					Description:    getBucketS3RequestsTotalMD(),
					Value:          float64(v),
					VariableLabels: map[string]string{"bucket": bucket, "api": k},
				})
			}

			for k, v := range httpStats.totalS3Canceled.Load(true) {
				metrics = append(metrics, MetricV2{
					Description:    getBucketS3RequestsCanceledMD(),
					Value:          float64(v),
					VariableLabels: map[string]string{"bucket": bucket, "api": k},
				})
			}

			for k, v := range httpStats.totalS34xxErrors.Load(true) {
				metrics = append(metrics, MetricV2{
					Description:    getBucketS3Requests4xxErrorsMD(),
					Value:          float64(v),
					VariableLabels: map[string]string{"bucket": bucket, "api": k},
				})
			}

			for k, v := range httpStats.totalS35xxErrors.Load(true) {
				metrics = append(metrics, MetricV2{
					Description:    getBucketS3Requests5xxErrorsMD(),
					Value:          float64(v),
					VariableLabels: map[string]string{"bucket": bucket, "api": k},
				})
			}
		}

		return metrics
	})
	return mg
}

func getNetworkMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		metrics = make([]MetricV2, 0, 10)
		connStats := globalConnStats.toServerConnStats()
		rpcStats := rest.GetRPCStats()
		if globalIsDistErasure {
			metrics = append(metrics, MetricV2{
				Description: getInternodeFailedRequests(),
				Value:       float64(rpcStats.Errs),
			})
			metrics = append(metrics, MetricV2{
				Description: getInternodeTCPDialTimeout(),
				Value:       float64(rpcStats.DialErrs),
			})
			metrics = append(metrics, MetricV2{
				Description: getInternodeTCPAvgDuration(),
				Value:       float64(rpcStats.DialAvgDuration),
			})
			metrics = append(metrics, MetricV2{
				Description: getInterNodeSentBytesMD(),
				Value:       float64(connStats.internodeOutputBytes),
			})
			metrics = append(metrics, MetricV2{
				Description: getInterNodeReceivedBytesMD(),
				Value:       float64(connStats.internodeInputBytes),
			})
		}
		metrics = append(metrics, MetricV2{
			Description: getS3SentBytesMD(),
			Value:       float64(connStats.s3OutputBytes),
		})
		metrics = append(metrics, MetricV2{
			Description: getS3ReceivedBytesMD(),
			Value:       float64(connStats.s3InputBytes),
		})
		return metrics
	})
	return mg
}

func getClusterUsageMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return metrics
		}

		metrics = make([]MetricV2, 0, 50)
		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objLayer)
		if err != nil {
			metricsLogIf(ctx, err)
			return metrics
		}

		// data usage has not captured any data yet.
		if dataUsageInfo.LastUpdate.IsZero() {
			return metrics
		}

		metrics = append(metrics, MetricV2{
			Description: getUsageLastScanActivityMD(),
			Value:       float64(time.Since(dataUsageInfo.LastUpdate)),
		})

		var (
			clusterSize               uint64
			clusterBuckets            uint64
			clusterObjectsCount       uint64
			clusterVersionsCount      uint64
			clusterDeleteMarkersCount uint64
		)

		clusterObjectSizesHistogram := map[string]uint64{}
		clusterVersionsHistogram := map[string]uint64{}
		for _, usage := range dataUsageInfo.BucketsUsage {
			clusterBuckets++
			clusterSize += usage.Size
			clusterObjectsCount += usage.ObjectsCount
			clusterVersionsCount += usage.VersionsCount
			clusterDeleteMarkersCount += usage.DeleteMarkersCount
			for k, v := range usage.ObjectSizesHistogram {
				v1, ok := clusterObjectSizesHistogram[k]
				if !ok {
					clusterObjectSizesHistogram[k] = v
				} else {
					v1 += v
					clusterObjectSizesHistogram[k] = v1
				}
			}
			for k, v := range usage.ObjectVersionsHistogram {
				v1, ok := clusterVersionsHistogram[k]
				if !ok {
					clusterVersionsHistogram[k] = v
				} else {
					v1 += v
					clusterVersionsHistogram[k] = v1
				}
			}
		}

		metrics = append(metrics, MetricV2{
			Description: getClusterUsageTotalBytesMD(),
			Value:       float64(clusterSize),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterUsageObjectsTotalMD(),
			Value:       float64(clusterObjectsCount),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterUsageVersionsTotalMD(),
			Value:       float64(clusterVersionsCount),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterUsageDeleteMarkersTotalMD(),
			Value:       float64(clusterDeleteMarkersCount),
		})

		metrics = append(metrics, MetricV2{
			Description:          getClusterObjectDistributionMD(),
			Histogram:            clusterObjectSizesHistogram,
			HistogramBucketLabel: "range",
		})

		metrics = append(metrics, MetricV2{
			Description:          getClusterObjectVersionsMD(),
			Histogram:            clusterVersionsHistogram,
			HistogramBucketLabel: "range",
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterBucketsTotalMD(),
			Value:       float64(clusterBuckets),
		})

		return metrics
	})
	return mg
}

func getBucketUsageMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()

		metrics = make([]MetricV2, 0, 50)
		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objLayer)
		if err != nil {
			metricsLogIf(ctx, err)
			return metrics
		}

		// data usage has not captured any data yet.
		if dataUsageInfo.LastUpdate.IsZero() {
			return metrics
		}

		metrics = append(metrics, MetricV2{
			Description: getBucketUsageLastScanActivityMD(),
			Value:       float64(time.Since(dataUsageInfo.LastUpdate)),
		})

		var bucketReplStats map[string]BucketStats
		if !globalSiteReplicationSys.isEnabled() {
			bucketReplStats = globalReplicationStats.Load().getAllLatest(dataUsageInfo.BucketsUsage)
		}
		buckets := mapKeysSorted(dataUsageInfo.BucketsUsage)
		if len(buckets) > v2MetricsMaxBuckets {
			buckets = buckets[:v2MetricsMaxBuckets]
		}
		for _, bucket := range buckets {
			usage := dataUsageInfo.BucketsUsage[bucket]
			quota, _ := globalBucketQuotaSys.Get(ctx, bucket)

			metrics = append(metrics, MetricV2{
				Description:    getBucketUsageTotalBytesMD(),
				Value:          float64(usage.Size),
				VariableLabels: map[string]string{"bucket": bucket},
			})

			metrics = append(metrics, MetricV2{
				Description:    getBucketUsageObjectsTotalMD(),
				Value:          float64(usage.ObjectsCount),
				VariableLabels: map[string]string{"bucket": bucket},
			})

			metrics = append(metrics, MetricV2{
				Description:    getBucketUsageVersionsTotalMD(),
				Value:          float64(usage.VersionsCount),
				VariableLabels: map[string]string{"bucket": bucket},
			})

			metrics = append(metrics, MetricV2{
				Description:    getBucketUsageDeleteMarkersTotalMD(),
				Value:          float64(usage.DeleteMarkersCount),
				VariableLabels: map[string]string{"bucket": bucket},
			})

			if quota != nil && quota.Quota > 0 {
				metrics = append(metrics, MetricV2{
					Description:    getBucketUsageQuotaTotalBytesMD(),
					Value:          float64(quota.Quota),
					VariableLabels: map[string]string{"bucket": bucket},
				})
			}
			if !globalSiteReplicationSys.isEnabled() {
				var stats BucketReplicationStats
				s, ok := bucketReplStats[bucket]
				if ok {
					stats = s.ReplicationStats
					metrics = append(metrics, MetricV2{
						Description:    getRepReceivedBytesMD(bucketMetricNamespace),
						Value:          float64(stats.ReplicaSize),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getRepReceivedOperationsMD(bucketMetricNamespace),
						Value:          float64(stats.ReplicaCount),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getClusterReplProxiedGetOperationsMD(bucketMetricNamespace),
						Value:          float64(s.ProxyStats.GetTotal),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getClusterReplProxiedHeadOperationsMD(bucketMetricNamespace),
						Value:          float64(s.ProxyStats.HeadTotal),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getClusterReplProxiedPutTaggingOperationsMD(bucketMetricNamespace),
						Value:          float64(s.ProxyStats.PutTagTotal),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getClusterReplProxiedGetTaggingOperationsMD(bucketMetricNamespace),
						Value:          float64(s.ProxyStats.GetTagTotal),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description:    getClusterReplProxiedRmvTaggingOperationsMD(bucketMetricNamespace),
						Value:          float64(s.ProxyStats.RmvTagTotal),
						VariableLabels: map[string]string{"bucket": bucket},
					})
					metrics = append(metrics, MetricV2{
						Description: getClusterReplProxiedGetFailedOperationsMD(bucketMetricNamespace),
						Value:       float64(s.ProxyStats.GetFailedTotal),
					})
					metrics = append(metrics, MetricV2{
						Description: getClusterReplProxiedHeadFailedOperationsMD(bucketMetricNamespace),
						Value:       float64(s.ProxyStats.HeadFailedTotal),
					})
					metrics = append(metrics, MetricV2{
						Description: getClusterReplProxiedPutTaggingFailedOperationsMD(bucketMetricNamespace),
						Value:       float64(s.ProxyStats.PutTagFailedTotal),
					})
					metrics = append(metrics, MetricV2{
						Description: getClusterReplProxiedGetTaggingFailedOperationsMD(bucketMetricNamespace),
						Value:       float64(s.ProxyStats.GetTagFailedTotal),
					})
					metrics = append(metrics, MetricV2{
						Description: getClusterReplProxiedRmvTaggingFailedOperationsMD(bucketMetricNamespace),
						Value:       float64(s.ProxyStats.RmvTagFailedTotal),
					})
				}
				if stats.hasReplicationUsage() {
					for arn, stat := range stats.Stats {
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedBytesLastMinuteMD(bucketMetricNamespace),
							Value:          float64(stat.Failed.LastMinute.Bytes),
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedOperationsLastMinuteMD(bucketMetricNamespace),
							Value:          stat.Failed.LastMinute.Count,
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedBytesLastHourMD(bucketMetricNamespace),
							Value:          float64(stat.Failed.LastHour.Bytes),
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedOperationsLastHourMD(bucketMetricNamespace),
							Value:          stat.Failed.LastHour.Count,
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedBytesTotalMD(bucketMetricNamespace),
							Value:          float64(stat.Failed.Totals.Bytes),
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepFailedOperationsTotalMD(bucketMetricNamespace),
							Value:          stat.Failed.Totals.Count,
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepSentBytesMD(bucketMetricNamespace),
							Value:          float64(stat.ReplicatedSize),
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:    getRepSentOperationsMD(bucketMetricNamespace),
							Value:          float64(stat.ReplicatedCount),
							VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
						})
						metrics = append(metrics, MetricV2{
							Description:          getBucketRepLatencyMD(),
							HistogramBucketLabel: "range",
							Histogram:            stat.Latency.getUploadLatency(),
							VariableLabels:       map[string]string{"bucket": bucket, "operation": "upload", "targetArn": arn},
						})
						if c, ok := stat.Failed.ErrCounts["AccessDenied"]; ok {
							metrics = append(metrics, MetricV2{
								Description:    getClusterRepCredentialErrorsMD(bucketMetricNamespace),
								Value:          float64(c),
								VariableLabels: map[string]string{"bucket": bucket, "targetArn": arn},
							})
						}
					}
				}
			}
			metrics = append(metrics, MetricV2{
				Description:          getBucketObjectDistributionMD(),
				Histogram:            usage.ObjectSizesHistogram,
				HistogramBucketLabel: "range",
				VariableLabels:       map[string]string{"bucket": bucket},
			})

			metrics = append(metrics, MetricV2{
				Description:          getBucketObjectVersionsMD(),
				Histogram:            usage.ObjectVersionsHistogram,
				HistogramBucketLabel: "range",
				VariableLabels:       map[string]string{"bucket": bucket},
			})
		}
		return metrics
	})
	return mg
}

func getClusterTransitionedBytesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionedBytes,
		Help:      "Total bytes transitioned to a tier",
		Type:      gaugeMetric,
	}
}

func getClusterTransitionedObjectsMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionedObjects,
		Help:      "Total number of objects transitioned to a tier",
		Type:      gaugeMetric,
	}
}

func getClusterTransitionedVersionsMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: ilmSubsystem,
		Name:      transitionedVersions,
		Help:      "Total number of versions transitioned to a tier",
		Type:      gaugeMetric,
	}
}

func getClusterTierMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()

		if globalTierConfigMgr.Empty() {
			return metrics
		}

		dui, err := loadDataUsageFromBackend(ctx, objLayer)
		if err != nil {
			metricsLogIf(ctx, err)
			return metrics
		}
		// data usage has not captured any tier stats yet.
		if dui.TierStats == nil {
			return metrics
		}

		return dui.tierMetrics()
	})
	return mg
}

func getLocalStorageMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()

		metrics = make([]MetricV2, 0, 50)
		storageInfo := objLayer.LocalStorageInfo(ctx, true)
		onlineDrives, offlineDrives := getOnlineOfflineDisksStats(storageInfo.Disks)
		totalDrives := onlineDrives.Merge(offlineDrives)

		for _, disk := range storageInfo.Disks {
			metrics = append(metrics, MetricV2{
				Description:    getNodeDriveUsedBytesMD(),
				Value:          float64(disk.UsedSpace),
				VariableLabels: map[string]string{"drive": disk.DrivePath},
			})

			metrics = append(metrics, MetricV2{
				Description:    getNodeDriveFreeBytesMD(),
				Value:          float64(disk.AvailableSpace),
				VariableLabels: map[string]string{"drive": disk.DrivePath},
			})

			metrics = append(metrics, MetricV2{
				Description:    getNodeDriveTotalBytesMD(),
				Value:          float64(disk.TotalSpace),
				VariableLabels: map[string]string{"drive": disk.DrivePath},
			})

			metrics = append(metrics, MetricV2{
				Description:    getNodeDrivesFreeInodesMD(),
				Value:          float64(disk.FreeInodes),
				VariableLabels: map[string]string{"drive": disk.DrivePath},
			})

			if disk.Metrics != nil {
				metrics = append(metrics, MetricV2{
					Description:    getNodeDriveTimeoutErrorsMD(),
					Value:          float64(disk.Metrics.TotalErrorsTimeout),
					VariableLabels: map[string]string{"drive": disk.DrivePath},
				})

				metrics = append(metrics, MetricV2{
					Description:    getNodeDriveIOErrorsMD(),
					Value:          float64(disk.Metrics.TotalErrorsAvailability - disk.Metrics.TotalErrorsTimeout),
					VariableLabels: map[string]string{"drive": disk.DrivePath},
				})

				metrics = append(metrics, MetricV2{
					Description:    getNodeDriveAvailabilityErrorsMD(),
					Value:          float64(disk.Metrics.TotalErrorsAvailability),
					VariableLabels: map[string]string{"drive": disk.DrivePath},
				})

				metrics = append(metrics, MetricV2{
					Description:    getNodeDriveWaitingIOMD(),
					Value:          float64(disk.Metrics.TotalWaiting),
					VariableLabels: map[string]string{"drive": disk.DrivePath},
				})

				for apiName, latency := range disk.Metrics.LastMinute {
					metrics = append(metrics, MetricV2{
						Description:    getNodeDriveAPILatencyMD(),
						Value:          float64(latency.Avg().Microseconds()),
						VariableLabels: map[string]string{"drive": disk.DrivePath, "api": "storage." + apiName},
					})
				}
			}
		}

		metrics = append(metrics, MetricV2{
			Description: getNodeDrivesOfflineTotalMD(),
			Value:       float64(offlineDrives.Sum()),
		})

		metrics = append(metrics, MetricV2{
			Description: getNodeDrivesOnlineTotalMD(),
			Value:       float64(onlineDrives.Sum()),
		})

		metrics = append(metrics, MetricV2{
			Description: getNodeDrivesTotalMD(),
			Value:       float64(totalDrives.Sum()),
		})

		metrics = append(metrics, MetricV2{
			Description: getNodeStandardParityMD(),
			Value:       float64(storageInfo.Backend.StandardSCParity),
		})

		metrics = append(metrics, MetricV2{
			Description: getNodeRRSParityMD(),
			Value:       float64(storageInfo.Backend.RRSCParity),
		})

		return metrics
	})
	return mg
}

func getClusterWriteQuorumMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "write",
		Name:      "quorum",
		Help:      "Maximum write quorum across all pools and sets",
		Type:      gaugeMetric,
	}
}

func getClusterHealthStatusMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "status",
		Help:      "Get current cluster health status",
		Type:      gaugeMetric,
	}
}

func getClusterErasureSetHealthStatusMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "erasure_set_status",
		Help:      "Get current health status for this erasure set",
		Type:      gaugeMetric,
	}
}

func getClusterErasureSetReadQuorumMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "erasure_set_read_quorum",
		Help:      "Get the read quorum for this erasure set",
		Type:      gaugeMetric,
	}
}

func getClusterErasureSetWriteQuorumMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "erasure_set_write_quorum",
		Help:      "Get the write quorum for this erasure set",
		Type:      gaugeMetric,
	}
}

func getClusterErasureSetOnlineDrivesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "erasure_set_online_drives",
		Help:      "Get the count of the online drives in this erasure set",
		Type:      gaugeMetric,
	}
}

func getClusterErasureSetHealingDrivesMD() MetricDescription {
	return MetricDescription{
		Namespace: clusterMetricNamespace,
		Subsystem: "health",
		Name:      "erasure_set_healing_drives",
		Help:      "Get the count of healing drives of this erasure set",
		Type:      gaugeMetric,
	}
}

func getClusterHealthMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()

		opts := HealthOptions{}
		result := objLayer.Health(ctx, opts)

		metrics = make([]MetricV2, 0, 2+4*len(result.ESHealth))

		metrics = append(metrics, MetricV2{
			Description: getClusterWriteQuorumMD(),
			Value:       float64(result.WriteQuorum),
		})

		health := 1
		if !result.Healthy {
			health = 0
		}

		metrics = append(metrics, MetricV2{
			Description: getClusterHealthStatusMD(),
			Value:       float64(health),
		})

		for _, h := range result.ESHealth {
			labels := map[string]string{
				"pool": strconv.Itoa(h.PoolID),
				"set":  strconv.Itoa(h.SetID),
			}
			metrics = append(metrics, MetricV2{
				Description:    getClusterErasureSetReadQuorumMD(),
				VariableLabels: labels,
				Value:          float64(h.ReadQuorum),
			})
			metrics = append(metrics, MetricV2{
				Description:    getClusterErasureSetWriteQuorumMD(),
				VariableLabels: labels,
				Value:          float64(h.WriteQuorum),
			})
			metrics = append(metrics, MetricV2{
				Description:    getClusterErasureSetOnlineDrivesMD(),
				VariableLabels: labels,
				Value:          float64(h.HealthyDrives),
			})
			metrics = append(metrics, MetricV2{
				Description:    getClusterErasureSetHealingDrivesMD(),
				VariableLabels: labels,
				Value:          float64(h.HealingDrives),
			})

			health := 1
			if !h.Healthy {
				health = 0
			}

			metrics = append(metrics, MetricV2{
				Description:    getClusterErasureSetHealthStatusMD(),
				VariableLabels: labels,
				Value:          float64(health),
			})
		}

		return metrics
	})

	return mg
}

func getBatchJobsMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}

	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		var m madmin.RealtimeMetrics
		mLocal := collectLocalMetrics(madmin.MetricsBatchJobs, collectMetricsOpts{})
		m.Merge(&mLocal)

		mRemote := collectRemoteMetrics(ctx, madmin.MetricsBatchJobs, collectMetricsOpts{})
		m.Merge(&mRemote)

		if m.Aggregated.BatchJobs == nil {
			return metrics
		}

		for _, mj := range m.Aggregated.BatchJobs.Jobs {
			jtype := toSnake(mj.JobType)
			var objects, objectsFailed float64
			var bucket string
			switch madmin.BatchJobType(mj.JobType) {
			case madmin.BatchJobReplicate:
				objects = float64(mj.Replicate.Objects)
				objectsFailed = float64(mj.Replicate.ObjectsFailed)
				bucket = mj.Replicate.Bucket
			case madmin.BatchJobKeyRotate:
				objects = float64(mj.KeyRotate.Objects)
				objectsFailed = float64(mj.KeyRotate.ObjectsFailed)
				bucket = mj.KeyRotate.Bucket
			case madmin.BatchJobExpire:
				objects = float64(mj.Expired.Objects)
				objectsFailed = float64(mj.Expired.ObjectsFailed)
				bucket = mj.Expired.Bucket
			}
			metrics = append(metrics,
				MetricV2{
					Description: MetricDescription{
						Namespace: bucketMetricNamespace,
						Subsystem: "batch",
						Name:      MetricName(jtype + "_objects"),
						Help:      "Get successfully completed batch job " + jtype + "objects",
						Type:      counterMetric,
					},
					Value:          objects,
					VariableLabels: map[string]string{"bucket": bucket, "jobId": mj.JobID},
				},
				MetricV2{
					Description: MetricDescription{
						Namespace: bucketMetricNamespace,
						Subsystem: "batch",
						Name:      MetricName(jtype + "_objects_failed"),
						Help:      "Get failed batch job " + jtype + "objects",
						Type:      counterMetric,
					},
					Value:          objectsFailed,
					VariableLabels: map[string]string{"bucket": bucket, "jobId": mj.JobID},
				},
			)
		}
		return metrics
	})
	return mg
}

func getClusterStorageMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    1 * time.Minute,
		metricsGroupOpts: opts,
	}
	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		objLayer := newObjectLayerFn()

		// Fetch disk space info, ignore errors
		metrics = make([]MetricV2, 0, 10)
		storageInfo := objLayer.StorageInfo(ctx, true)
		onlineDrives, offlineDrives := getOnlineOfflineDisksStats(storageInfo.Disks)
		totalDrives := onlineDrives.Merge(offlineDrives)

		metrics = append(metrics, MetricV2{
			Description: getClusterCapacityTotalBytesMD(),
			Value:       float64(GetTotalCapacity(storageInfo.Disks)),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterCapacityFreeBytesMD(),
			Value:       float64(GetTotalCapacityFree(storageInfo.Disks)),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterCapacityUsageBytesMD(),
			Value:       float64(GetTotalUsableCapacity(storageInfo.Disks, storageInfo)),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterCapacityUsageFreeBytesMD(),
			Value:       float64(GetTotalUsableCapacityFree(storageInfo.Disks, storageInfo)),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterDrivesOfflineTotalMD(),
			Value:       float64(offlineDrives.Sum()),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterDrivesOnlineTotalMD(),
			Value:       float64(onlineDrives.Sum()),
		})

		metrics = append(metrics, MetricV2{
			Description: getClusterDrivesTotalMD(),
			Value:       float64(totalDrives.Sum()),
		})
		return metrics
	})
	return mg
}

func getKMSNodeMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}

	mg.RegisterRead(func(ctx context.Context) (metrics []MetricV2) {
		const (
			Online  = 1
			Offline = 0
		)
		desc := MetricDescription{
			Namespace: clusterMetricNamespace,
			Subsystem: kmsSubsystem,
			Name:      kmsOnline,
			Help:      "Reports whether the KMS is online (1) or offline (0)",
			Type:      gaugeMetric,
		}
		_, err := GlobalKMS.Metrics(ctx)
		if _, ok := kes.IsConnError(err); ok {
			return []MetricV2{{
				Description: desc,
				Value:       float64(Offline),
			}}
		}
		return []MetricV2{{
			Description: desc,
			Value:       float64(Online),
		}}
	})
	return mg
}

func getWebhookMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: 10 * time.Second,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		tgts := append(logger.SystemTargets(), logger.AuditTargets()...)
		metrics := make([]MetricV2, 0, len(tgts)*4)
		for _, t := range tgts {
			isOnline := 0
			if t.IsOnline(ctx) {
				isOnline = 1
			}
			labels := map[string]string{
				"name":     t.String(),
				"endpoint": t.Endpoint(),
			}
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: clusterMetricNamespace,
					Subsystem: webhookSubsystem,
					Name:      webhookOnline,
					Help:      "Is the webhook online?",
					Type:      gaugeMetric,
				},
				VariableLabels: labels,
				Value:          float64(isOnline),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: clusterMetricNamespace,
					Subsystem: webhookSubsystem,
					Name:      webhookQueueLength,
					Help:      "Webhook queue length",
					Type:      gaugeMetric,
				},
				VariableLabels: labels,
				Value:          float64(t.Stats().QueueLength),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: clusterMetricNamespace,
					Subsystem: webhookSubsystem,
					Name:      webhookTotalMessages,
					Help:      "Total number of messages sent to this target",
					Type:      counterMetric,
				},
				VariableLabels: labels,
				Value:          float64(t.Stats().TotalMessages),
			})
			metrics = append(metrics, MetricV2{
				Description: MetricDescription{
					Namespace: clusterMetricNamespace,
					Subsystem: webhookSubsystem,
					Name:      webhookFailedMessages,
					Help:      "Number of messages that failed to send",
					Type:      counterMetric,
				},
				VariableLabels: labels,
				Value:          float64(t.Stats().FailedMessages),
			})
		}

		return metrics
	})
	return mg
}

func getKMSMetrics(opts MetricsGroupOpts) *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval:    10 * time.Second,
		metricsGroupOpts: opts,
	}

	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		metrics := make([]MetricV2, 0, 4)
		metric, err := GlobalKMS.Metrics(ctx)
		if err != nil {
			return metrics
		}
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: clusterMetricNamespace,
				Subsystem: kmsSubsystem,
				Name:      kmsRequestsSuccess,
				Help:      "Number of KMS requests that succeeded",
				Type:      counterMetric,
			},
			Value: float64(metric.ReqOK),
		})
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: clusterMetricNamespace,
				Subsystem: kmsSubsystem,
				Name:      kmsRequestsError,
				Help:      "Number of KMS requests that failed due to some error. (HTTP 4xx status code)",
				Type:      counterMetric,
			},
			Value: float64(metric.ReqErr),
		})
		metrics = append(metrics, MetricV2{
			Description: MetricDescription{
				Namespace: clusterMetricNamespace,
				Subsystem: kmsSubsystem,
				Name:      kmsRequestsFail,
				Help:      "Number of KMS requests that failed due to some internal failure. (HTTP 5xx status code)",
				Type:      counterMetric,
			},
			Value: float64(metric.ReqFail),
		})
		return metrics
	})
	return mg
}

func collectMetric(metric MetricV2, labels []string, values []string, metricName string, out chan<- prometheus.Metric) {
	if metric.Description.Type == histogramMetric {
		if metric.Histogram == nil {
			return
		}
		for k, v := range metric.Histogram {
			pmetric, err := prometheus.NewConstMetric(
				prometheus.NewDesc(
					prometheus.BuildFQName(string(metric.Description.Namespace),
						string(metric.Description.Subsystem),
						string(metric.Description.Name)),
					metric.Description.Help,
					append(labels, metric.HistogramBucketLabel),
					metric.StaticLabels,
				),
				prometheus.GaugeValue,
				float64(v),
				append(values, k)...)
			if err != nil {
				// Enable for debugging
				if serverDebugLog {
					bugLogIf(GlobalContext, fmt.Errorf("unable to validate prometheus metric (%w) %v+%v", err, values, metric.Histogram))
				}
			} else {
				out <- pmetric
			}
		}
		return
	}
	metricType := prometheus.GaugeValue
	if metric.Description.Type == counterMetric {
		metricType = prometheus.CounterValue
	}
	pmetric, err := prometheus.NewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(string(metric.Description.Namespace),
				string(metric.Description.Subsystem),
				string(metric.Description.Name)),
			metric.Description.Help,
			labels,
			metric.StaticLabels,
		),
		metricType,
		metric.Value,
		values...)
	if err != nil {
		// Enable for debugging
		if serverDebugLog {
			bugLogIf(GlobalContext, fmt.Errorf("unable to validate prometheus metric (%w) %v", err, values))
		}
	} else {
		out <- pmetric
	}
}

//msgp:ignore minioBucketCollector
type minioBucketCollector struct {
	metricsGroups []*MetricsGroupV2
	desc          *prometheus.Desc
}

func newMinioBucketCollector(metricsGroups []*MetricsGroupV2) *minioBucketCollector {
	return &minioBucketCollector{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_bucket_stats", "Statistics exposed by MinIO server cluster wide per bucket", nil, nil),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioBucketCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioBucketCollector) Collect(out chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	publish := func(in <-chan MetricV2) {
		defer wg.Done()
		for metric := range in {
			labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
			collectMetric(metric, labels, values, "bucket", out)
		}
	}

	// Call peer api to fetch metrics
	wg.Add(2)
	go publish(ReportMetrics(GlobalContext, c.metricsGroups))
	go publish(globalNotificationSys.GetBucketMetrics(GlobalContext))
	wg.Wait()
}

//msgp:ignore minioClusterCollector
type minioClusterCollector struct {
	metricsGroups []*MetricsGroupV2
	desc          *prometheus.Desc
}

func newMinioClusterCollector(metricsGroups []*MetricsGroupV2) *minioClusterCollector {
	return &minioClusterCollector{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_stats", "Statistics exposed by MinIO server per cluster", nil, nil),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioClusterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioClusterCollector) Collect(out chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	publish := func(in <-chan MetricV2) {
		defer wg.Done()
		for metric := range in {
			labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
			collectMetric(metric, labels, values, "cluster", out)
		}
	}

	// Call peer api to fetch metrics
	wg.Add(2)
	go publish(ReportMetrics(GlobalContext, c.metricsGroups))
	go publish(globalNotificationSys.GetClusterMetrics(GlobalContext))
	wg.Wait()
}

// ReportMetrics reports serialized metrics to the channel passed for the metrics generated.
func ReportMetrics(ctx context.Context, metricsGroups []*MetricsGroupV2) <-chan MetricV2 {
	ch := make(chan MetricV2)
	go func() {
		defer xioutil.SafeClose(ch)
		populateAndPublish(metricsGroups, func(m MetricV2) bool {
			if m.VariableLabels == nil {
				m.VariableLabels = make(map[string]string)
			}
			m.VariableLabels[serverName] = globalLocalNodeName
			for {
				select {
				case ch <- m:
					return true
				case <-ctx.Done():
					return false
				}
			}
		})
	}()
	return ch
}

// minioNodeCollector is the Custom Collector
//
//msgp:ignore minioNodeCollector
type minioNodeCollector struct {
	metricsGroups []*MetricsGroupV2
	desc          *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// populateAndPublish populates and then publishes the metrics generated by the generator function.
func populateAndPublish(metricsGroups []*MetricsGroupV2, publish func(m MetricV2) bool) {
	for _, mg := range metricsGroups {
		if mg == nil {
			continue
		}
		for _, metric := range mg.Get() {
			if !publish(metric) {
				return
			}
		}
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioNodeCollector) Collect(ch chan<- prometheus.Metric) {
	// Expose MinIO's version information
	minioVersionInfo.WithLabelValues(Version, CommitID).Set(1.0)

	populateAndPublish(c.metricsGroups, func(metric MetricV2) bool {
		labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
		values = append(values, globalLocalNodeName)
		labels = append(labels, serverName)

		if metric.Description.Type == histogramMetric {
			if metric.Histogram == nil {
				return true
			}
			for k, v := range metric.Histogram {
				labels = append(labels, metric.HistogramBucketLabel)
				values = append(values, k)
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(string(metric.Description.Namespace),
							string(metric.Description.Subsystem),
							string(metric.Description.Name)),
						metric.Description.Help,
						labels,
						metric.StaticLabels,
					),
					prometheus.GaugeValue,
					float64(v),
					values...)
			}
			return true
		}

		metricType := prometheus.GaugeValue
		if metric.Description.Type == counterMetric {
			metricType = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(string(metric.Description.Namespace),
					string(metric.Description.Subsystem),
					string(metric.Description.Name)),
				metric.Description.Help,
				labels,
				metric.StaticLabels,
			),
			metricType,
			metric.Value,
			values...)
		return true
	})
}

func getOrderedLabelValueArrays(labelsWithValue map[string]string) (labels, values []string) {
	labels = make([]string, 0, len(labelsWithValue))
	values = make([]string, 0, len(labelsWithValue))
	for l, v := range labelsWithValue {
		labels = append(labels, l)
		values = append(values, v)
	}
	return labels, values
}

// newMinioCollectorNode describes the collector
// and returns reference of minioCollector for version 2
// It creates the Prometheus Description which is used
// to define Metric and  help string
func newMinioCollectorNode(metricsGroups []*MetricsGroupV2) *minioNodeCollector {
	return &minioNodeCollector{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_stats", "Statistics exposed by MinIO server per node", nil, nil),
	}
}

func metricsHTTPHandler(c prometheus.Collector, funcName string) http.Handler {
	registry := prometheus.NewRegistry()

	// Report all other metrics
	logger.CriticalIf(GlobalContext, registry.Register(c))

	// DefaultGatherers include golang metrics and process metrics.
	gatherers := prometheus.Gatherers{
		registry,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = funcName
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil && len(mfs) == 0 {
			writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
			return
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				// client may disconnect for any reasons
				// we do not have to log this.
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}

func metricsBucketHandler() http.Handler {
	return metricsHTTPHandler(bucketCollector, "handler.MetricsBucket")
}

func metricsServerHandler() http.Handler {
	registry := prometheus.NewRegistry()

	// Report all other metrics
	logger.CriticalIf(GlobalContext, registry.Register(clusterCollector))

	// DefaultGatherers include golang metrics and process metrics.
	gatherers := prometheus.Gatherers{
		registry,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsCluster"
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil && len(mfs) == 0 {
			writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
			return
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				// client may disconnect for any reasons
				// we do not have to log this.
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}

func metricsNodeHandler() http.Handler {
	registry := prometheus.NewRegistry()

	logger.CriticalIf(GlobalContext, registry.Register(nodeCollector))
	if err := registry.Register(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{
		Namespace:    minioNamespace,
		ReportErrors: true,
	})); err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	if err := registry.Register(prometheus.NewGoCollector()); err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	gatherers := prometheus.Gatherers{
		registry,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsNode"
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil {
			if len(mfs) == 0 {
				writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
				return
			}
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				metricsLogIf(r.Context(), err)
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}

func toSnake(camel string) (snake string) {
	var b strings.Builder
	l := len(camel)
	for i, v := range camel {
		// A is 65, a is 97
		if v >= 'a' {
			b.WriteRune(v)
			continue
		}
		// v is capital letter here
		// disregard first letter
		// add underscore if last letter is capital letter
		// add underscore when previous letter is lowercase
		// add underscore when next letter is lowercase
		if (i != 0 || i == l-1) && ((i > 0 && rune(camel[i-1]) >= 'a') ||
			(i < l-1 && rune(camel[i+1]) >= 'a')) {
			b.WriteRune('_')
		}
		b.WriteRune(v + 'a' - 'A')
	}
	return b.String()
}
