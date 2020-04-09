/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
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
	minioVersionInfo.WithLabelValues(Version, CommitID).Set(float64(1.0))

	storageMetricsPrometheus(ch)
	networkMetricsPrometheus(ch)
	httpMetricsPrometheus(ch)
	gatewayMetricsPrometheus(ch)
	healingMetricsPrometheus(ch)
}

// collects healing specific metrics for MinIO instance in Prometheus specific format
// and sends to given channel
func healingMetricsPrometheus(ch chan<- prometheus.Metric) {
	if !globalIsXL {
		return
	}
	bgSeq, exists := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !exists {
		return
	}
	healMetricsNamespace := "self_heal"

	dur := time.Duration(-1)
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
			float64(v), string(s[0]), string(s[1]),
		)
	}
}

// collects gateway specific metrics for MinIO instance in Prometheus specific format
// and sends to given channel
func gatewayMetricsPrometheus(ch chan<- prometheus.Metric) {
	if !globalIsGateway || (globalGatewayName != "s3" && globalGatewayName != "azure" && globalGatewayName != "gcs") {
		return
	}

	objLayer := newObjectLayerWithoutSafeModeFn()
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
			prometheus.BuildFQName("gateway", globalGatewayName, "bytes_received"),
			"Total number of bytes received by current MinIO Gateway "+globalGatewayName+" backend",
			nil, nil),
		prometheus.CounterValue,
		float64(m.GetBytesReceived()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("gateway", globalGatewayName, "bytes_sent"),
			"Total number of bytes sent by current MinIO Gateway to "+globalGatewayName+" backend",
			nil, nil),
		prometheus.CounterValue,
		float64(m.GetBytesSent()),
	)
	s := m.GetRequests()
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("gateway", globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(s.Get.Load()),
		http.MethodGet,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("gateway", globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(s.Head.Load()),
		http.MethodHead,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("gateway", globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(s.Put.Load()),
		http.MethodPut,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("gateway", globalGatewayName, "requests"),
			"Total number of requests made to "+globalGatewayName+" by current MinIO Gateway",
			[]string{"method"}, nil),
		prometheus.CounterValue,
		float64(s.Post.Load()),
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
			prometheus.BuildFQName("cache", "hits", "total"),
			"Total number of disk cache hits in current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getHits()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("cache", "misses", "total"),
			"Total number of disk cache misses in current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getMisses()),
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("cache", "data", "served"),
			"Total number of bytes served from cache of current MinIO instance",
			nil, nil),
		prometheus.CounterValue,
		float64(cacheObjLayer.CacheStats().getBytesServed()),
	)
}

// collects http metrics for MinIO server in Prometheus specific format
// and sends to given channel
func httpMetricsPrometheus(ch chan<- prometheus.Metric) {
	httpStats := globalHTTPStats.toServerHTTPStats()

	for api, value := range httpStats.CurrentS3Requests.APIStats {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("s3", "requests", "current"),
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
				prometheus.BuildFQName("s3", "requests", "total"),
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
				prometheus.BuildFQName("s3", "errors", "total"),
				"Total number of s3 errors in current MinIO server instance",
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
			prometheus.BuildFQName("internode", "tx", "bytes_total"),
			"Total number of bytes sent to the other peer nodes by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.TotalOutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("internode", "rx", "bytes_total"),
			"Total number of internode bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.TotalInputBytes),
	)

	// Network Sent/Received Bytes (Outbound)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("s3", "tx", "bytes_total"),
			"Total number of s3 bytes sent by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.S3OutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("s3", "rx", "bytes_total"),
			"Total number of s3 bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.S3InputBytes),
	)
}

// collects storage metrics for MinIO server in Prometheus specific format
// and sends to given channel
func storageMetricsPrometheus(ch chan<- prometheus.Metric) {
	objLayer := newObjectLayerWithoutSafeModeFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	// Fetch disk space info
	storageInfo := objLayer.StorageInfo(GlobalContext, true)

	offlineDisks := storageInfo.Backend.OfflineDisks
	onlineDisks := storageInfo.Backend.OnlineDisks
	totalDisks := offlineDisks.Merge(onlineDisks)

	// MinIO Offline Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "disks", "offline"),
			"Total number of offline disks in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(offlineDisks.Sum()),
	)

	// MinIO Total Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName("minio", "disks", "total"),
			"Total number of disks for current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(totalDisks.Sum()),
	)

	for i := 0; i < len(storageInfo.Total); i++ {
		mountPath, total, free := storageInfo.MountPaths[i], storageInfo.Total[i],
			storageInfo.Available[i]

		// Total disk usage by the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "used"),
				"Total disk storage used on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(total-free),
			mountPath,
		)

		// Total available space in the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "available"),
				"Total available space left on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(free),
			mountPath,
		)

		// Total storage space of the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "total"),
				"Total space on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(total),
			mountPath,
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
		claims, _, authErr := webRequestAuthenticate(r)
		if authErr != nil || !claims.VerifyIssuer("prometheus", true) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	})
}
