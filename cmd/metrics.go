// Copyright (c) 2015-2021 MinIO, Inc.
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
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3_ttfb_seconds",
			Help:    "Time taken by requests served by current MinIO server instance",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"api"},
	)
	minioVersionInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "minio",
			Name:      "version_info",
			Help:      "Version of current MinIO server instance",
		},
		[]string{
			// current version
			"version",
			// commit-id of the current version
			"commit",
		},
	)
)

const (
	healMetricsNamespace = "self_heal"
	gatewayNamespace     = "gateway"
	cacheNamespace       = "cache"
	s3Namespace          = "s3"
	bucketNamespace      = "bucket"
	minioNamespace       = "minio"
	diskNamespace        = "disk"
	interNodeNamespace   = "internode"
)

func init() {
	prometheus.MustRegister(httpRequestsDuration)
	prometheus.MustRegister(newMinioCollector())
	prometheus.MustRegister(minioVersionInfo)
}

// newMinioCollector describes the collector
// and returns reference of minioCollector
// It creates the Prometheus Description which is used
// to define metric and  help string
func newMinioCollector() *minioCollector {
	return &minioCollector{
		desc: prometheus.NewDesc("minio_stats", "Statistics exposed by MinIO server", nil, nil),
	}
}

// minioCollector is the Custom Collector
type minioCollector struct {
	desc *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioCollector) Collect(ch chan<- prometheus.Metric) {

	// Expose MinIO's version information
	minioVersionInfo.WithLabelValues(Version, CommitID).Set(1.0)

	storageMetricsPrometheus(ch)
	nodeHealthMetricsPrometheus(ch)
	bucketUsageMetricsPrometheus(ch)
	networkMetricsPrometheus(ch)
	httpMetricsPrometheus(ch)
	cacheMetricsPrometheus(ch)
	gatewayMetricsPrometheus(ch)
	healingMetricsPrometheus(ch)
}

func nodeHealthMetricsPrometheus(ch chan<- prometheus.Metric) {
	nodesUp, nodesDown := GetPeerOnlineCount()
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "nodes", "online"),
			"Total number of MinIO nodes online",
			nil, nil),
		prometheus.GaugeValue,
		float64(nodesUp),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "nodes", "offline"),
			"Total number of MinIO nodes offline",
			nil, nil),
		prometheus.GaugeValue,
		float64(nodesDown),
	)
}

// collects healing specific metrics for MinIO instance in Prometheus specific format
// and sends to given channel
func healingMetricsPrometheus(ch chan<- prometheus.Metric) {
	if !globalIsErasure {
		return
	}
	bgSeq, exists := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !exists {
		return
	}

	var dur time.Duration
	if !bgSeq.lastHealActivity.IsZero() {
		dur = time.Since(bgSeq.lastHealActivity)
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(healMetricsNamespace, "time", "since_last_activity"),
			"Time elapsed (in nano seconds) since last self healing activity. This is set to -1 until initial self heal activity",
			nil, nil),
		prometheus.GaugeValue,
		float64(dur),
	)
	for k, v := range bgSeq.getScannedItemsMap() {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(healMetricsNamespace, "objects", "scanned"),
				"Objects scanned in current self healing run",
				[]string{"type"}, nil),
			prometheus.GaugeValue,
			float64(v), string(k),
		)
	}
	for k, v := range bgSeq.getHealedItemsMap() {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(healMetricsNamespace, "objects", "healed"),
				"Objects healed in current self healing run",
				[]string{"type"}, nil),
			prometheus.GaugeValue,
			float64(v), string(k),
		)
	}
	for k, v := range bgSeq.gethealFailedItemsMap() {
		// healFailedItemsMap stores the endpoint and volume state separated by comma,
		// split the fields and pass to channel at correct index
		s := strings.Split(k, ",")
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(healMetricsNamespace, "objects", "heal_failed"),
				"Objects for which healing failed in current self healing run",
				[]string{"mount_path", "volume_status"}, nil),
			prometheus.GaugeValue,
			float64(v), s[0], s[1],
		)
	}
}

// collects gateway specific metrics for MinIO instance in Prometheus specific format
// and sends to given channel
func gatewayMetricsPrometheus(ch chan<- prometheus.Metric) {
	if !globalIsGateway || (globalGatewayName != S3BackendGateway && globalGatewayName != AzureBackendGateway && globalGatewayName != GCSBackendGateway) {
		return
	}

	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	m, err := objLayer.GetMetrics(GlobalContext)
	if err != nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "bytes_received"),
			"Total number of bytes received by current MinIO Gateway "+globalGatewayName+" backend",
			nil, nil),
		prometheus.CounterValue,
		float64(m.GetBytesReceived()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "bytes_sent"),
			"Total number of bytes sent by current MinIO Gateway to "+globalGatewayName+" backend",
			nil, nil),
		prometheus.CounterValue,
		float64(m.GetBytesSent()),
	)
	s := m.GetRequests()
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&s.Get)),
		http.MethodGet,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&s.Head)),
		http.MethodHead,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&s.Put)),
		http.MethodPut,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(gatewayNamespace, globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(atomic.LoadUint64(&s.Post)),
		http.MethodPost,
	)
}

// collects cache metrics for MinIO server in Prometheus specific format
// and sends to given channel
func cacheMetricsPrometheus(ch chan<- prometheus.Metric) {
	cacheObjLayer := newCachedObjectLayerFn()
	// Service not initialized yet
	if cacheObjLayer == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(cacheNamespace, "hits", "total"),
			"Total number of disk cache hits in current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getHits()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(cacheNamespace, "misses", "total"),
			"Total number of disk cache misses in current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getMisses()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(cacheNamespace, "data", "served"),
			"Total number of bytes served from cache of current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getBytesServed()),
	)
	for _, cdStats := range cacheObjLayer.CacheStats().GetDiskStats() {
		// Cache disk usage percentage
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(cacheNamespace, "usage", "percent"),
				"Total percentage cache usage",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(cdStats.UsagePercent),
			cdStats.Dir,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(cacheNamespace, "usage", "high"),
				"Indicates cache usage is high or low, relative to current cache 'quota' settings",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(cdStats.UsageState),
			cdStats.Dir,
		)

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("cache", "usage", "size"),
				"Indicates current cache usage in bytes",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(cdStats.UsageSize),
			cdStats.Dir,
		)

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("cache", "total", "size"),
				"Indicates total size of cache disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(cdStats.TotalCapacity),
			cdStats.Dir,
		)
	}
}

// collects http metrics for MinIO server in Prometheus specific format
// and sends to given channel
func httpMetricsPrometheus(ch chan<- prometheus.Metric) {
	httpStats := globalHTTPStats.toServerHTTPStats()

	for api, value := range httpStats.CurrentS3Requests.APIStats {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(s3Namespace, "requests", "current"),
				"Total number of running s3 requests in current MinIO server instance",
				[]string{"api"}, nil),
			prometheus.CounterValue,
			float64(value),
			api,
		)
	}

	for api, value := range httpStats.TotalS3Requests.APIStats {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(s3Namespace, "requests", "total"),
				"Total number of s3 requests in current MinIO server instance",
				[]string{"api"}, nil),
			prometheus.CounterValue,
			float64(value),
			api,
		)
	}

	for api, value := range httpStats.TotalS3Errors.APIStats {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(s3Namespace, "errors", "total"),
				"Total number of s3 errors in current MinIO server instance",
				[]string{"api"}, nil),
			prometheus.CounterValue,
			float64(value),
			api,
		)
	}

	for api, value := range httpStats.TotalS3Canceled.APIStats {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(s3Namespace, "canceled", "total"),
				"Total number of client canceled s3 request in current MinIO server instance",
				[]string{"api"}, nil),
			prometheus.CounterValue,
			float64(value),
			api,
		)
	}
}

// collects network metrics for MinIO server in Prometheus specific format
// and sends to given channel
func networkMetricsPrometheus(ch chan<- prometheus.Metric) {
	connStats := globalConnStats.toServerConnStats()

	// Network Sent/Received Bytes (internode)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(interNodeNamespace, "tx", "bytes_total"),
			"Total number of bytes sent to the other peer nodes by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.TotalOutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(interNodeNamespace, "rx", "bytes_total"),
			"Total number of internode bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.TotalInputBytes),
	)

	// Network Sent/Received Bytes (Outbound)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(s3Namespace, "tx", "bytes_total"),
			"Total number of s3 bytes sent by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.S3OutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(s3Namespace, "rx", "bytes_total"),
			"Total number of s3 bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.S3InputBytes),
	)
}

// get the most current of in-memory replication stats  and data usage info from crawler.
func getLatestReplicationStats(bucket string, u madmin.BucketUsageInfo) (s BucketReplicationStats) {
	bucketStats := globalNotificationSys.GetClusterBucketStats(GlobalContext, bucket)

	replStats := BucketReplicationStats{}
	for _, bucketStat := range bucketStats {
		replStats.FailedCount += bucketStat.ReplicationStats.FailedCount
		replStats.FailedSize += bucketStat.ReplicationStats.FailedSize
		replStats.ReplicaSize += bucketStat.ReplicationStats.ReplicaSize
		replStats.ReplicatedSize += bucketStat.ReplicationStats.ReplicatedSize
	}
	usageStat := globalReplicationStats.GetInitialUsage(bucket)
	replStats.ReplicaSize += usageStat.ReplicaSize
	replStats.ReplicatedSize += usageStat.ReplicatedSize

	// use in memory replication stats if it is ahead of usage info.
	s.ReplicatedSize = u.ReplicatedSize
	if replStats.ReplicatedSize >= u.ReplicatedSize {
		s.ReplicatedSize = replStats.ReplicatedSize
	}
	// Reset FailedSize and FailedCount to 0 for negative overflows which can
	// happen since data usage picture can lag behind actual usage state at the time of cluster start
	s.FailedSize = uint64(math.Max(float64(replStats.FailedSize), 0))
	s.FailedCount = uint64(math.Max(float64(replStats.FailedCount), 0))
	s.ReplicaSize = uint64(math.Max(float64(replStats.ReplicaSize), float64(u.ReplicaSize)))
	return s
}

// Populates prometheus with bucket usage metrics, this metrics
// is only enabled if scanner is enabled.
func bucketUsageMetricsPrometheus(ch chan<- prometheus.Metric) {
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	if globalIsGateway {
		return
	}

	dataUsageInfo, err := loadDataUsageFromBackend(GlobalContext, objLayer)
	if err != nil {
		return
	}
	// data usage has not captured any data yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return
	}

	for bucket, usageInfo := range dataUsageInfo.BucketsUsage {
		stat := getLatestReplicationStats(bucket, usageInfo)
		// Total space used by bucket
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(bucketNamespace, "usage", "size"),
				"Total bucket size",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(usageInfo.Size),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(bucketNamespace, "objects", "count"),
				"Total number of objects in a bucket",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(usageInfo.ObjectsCount),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("bucket", "replication", "failed_size"),
				"Total capacity failed to replicate at least once",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.FailedSize),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("bucket", "replication", "successful_size"),
				"Total capacity replicated to destination",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.ReplicatedSize),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("bucket", "replication", "received_size"),
				"Total capacity replicated to this instance",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.ReplicaSize),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("bucket", "replication", "failed_count"),
				"Total replication operations failed",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.FailedCount),
			bucket,
		)
		for k, v := range usageInfo.ObjectSizesHistogram {
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					prometheus.BuildFQName(bucketNamespace, "objects", "histogram"),
					"Total number of objects of different sizes in a bucket",
					[]string{"bucket", "object_size"}, nil),
				prometheus.GaugeValue,
				float64(v),
				bucket,
				k,
			)
		}
	}
}

// collects storage metrics for MinIO server in Prometheus specific format
// and sends to given channel
func storageMetricsPrometheus(ch chan<- prometheus.Metric) {
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	if globalIsGateway {
		return
	}

	server := getLocalServerProperty(globalEndpoints, &http.Request{
		Host: globalLocalNodeName,
	})

	onlineDisks, offlineDisks := getOnlineOfflineDisksStats(server.Disks)
	totalDisks := offlineDisks.Merge(onlineDisks)

	// Report total capacity
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_raw", "total"),
			"Total capacity online in the cluster",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalCapacity(server.Disks)),
	)

	// Report total capacity free
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_raw_free", "total"),
			"Total free capacity online in the cluster",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalCapacityFree(server.Disks)),
	)

	s, _ := objLayer.StorageInfo(GlobalContext)
	// Report total usable capacity
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_usable", "total"),
			"Total usable capacity online in the cluster",
			nil, nil),
		prometheus.GaugeValue,
		GetTotalUsableCapacity(server.Disks, s),
	)
	// Report total usable capacity free
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_usable_free", "total"),
			"Total free usable capacity online in the cluster",
			nil, nil),
		prometheus.GaugeValue,
		GetTotalUsableCapacityFree(server.Disks, s),
	)

	// MinIO Offline Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "disks", "offline"),
			"Total number of offline disks in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(offlineDisks.Sum()),
	)

	// MinIO Total Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "disks", "total"),
			"Total number of disks for current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(totalDisks.Sum()),
	)

	for _, disk := range server.Disks {
		// Total disk usage by the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "used"),
				"Total disk storage used on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.UsedSpace),
			disk.DrivePath,
		)

		// Total available space in the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "available"),
				"Total available space left on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.AvailableSpace),
			disk.DrivePath,
		)

		// Total storage space of the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "total"),
				"Total space on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.TotalSpace),
			disk.DrivePath,
		)
	}
}

func metricsHandler() http.Handler {

	registry := prometheus.NewRegistry()

	err := registry.Register(minioVersionInfo)
	logger.LogIf(GlobalContext, err)

	err = registry.Register(httpRequestsDuration)
	logger.LogIf(GlobalContext, err)

	err = registry.Register(newMinioCollector())
	logger.LogIf(GlobalContext, err)

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		registry,
	}
	// Delegate http serving to Prometheus client library, which will call collector.Collect.
	return promhttp.InstrumentMetricHandler(
		registry,
		promhttp.HandlerFor(gatherers,
			promhttp.HandlerOpts{
				ErrorHandling: promhttp.ContinueOnError,
			}),
	)

}

// AuthMiddleware checks if the bearer token is valid and authorized.
func AuthMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, owner, authErr := webRequestAuthenticate(r)
		if authErr != nil || !claims.VerifyIssuer("prometheus", true) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// For authenticated users apply IAM policy.
		if !globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     claims.AccessKey,
			Action:          iampolicy.PrometheusAdminAction,
			ConditionValues: getConditionValues(r, "", claims.AccessKey, claims.Map()),
			IsOwner:         owner,
			Claims:          claims.Map(),
		}) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	})
}
