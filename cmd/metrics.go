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
	"net/http"
	"time"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
	"github.com/minio/pkg/v3/policy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
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
	bucketHTTPRequestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3_ttfb_seconds",
			Help:    "Time taken by requests served by current MinIO server instance per bucket",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"api", "bucket"},
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
	healingMetricsPrometheus(ch)
}

func nodeHealthMetricsPrometheus(ch chan<- prometheus.Metric) {
	nodesUp, nodesDown := globalNotificationSys.GetPeerOnlineCount()
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
				"Objects scanned since uptime",
				[]string{"type"}, nil),
			prometheus.CounterValue,
			float64(v), string(k),
		)
	}
	for k, v := range bgSeq.getHealedItemsMap() {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(healMetricsNamespace, "objects", "healed"),
				"Objects healed since uptime",
				[]string{"type"}, nil),
			prometheus.CounterValue,
			float64(v), string(k),
		)
	}
	for k, v := range bgSeq.getHealFailedItemsMap() {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(healMetricsNamespace, "objects", "heal_failed"),
				"Objects for which healing failed since uptime",
				[]string{"type"}, nil),
			prometheus.CounterValue,
			float64(v), string(k),
		)
	}
}

// collects http metrics for MinIO server in Prometheus specific format
// and sends to given channel
func httpMetricsPrometheus(ch chan<- prometheus.Metric) {
	httpStats := globalHTTPStats.toServerHTTPStats(true)

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
		float64(connStats.internodeOutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(interNodeNamespace, "rx", "bytes_total"),
			"Total number of internode bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.internodeInputBytes),
	)

	// Network Sent/Received Bytes (Outbound)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(s3Namespace, "tx", "bytes_total"),
			"Total number of s3 bytes sent by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.s3OutputBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(s3Namespace, "rx", "bytes_total"),
			"Total number of s3 bytes received by current MinIO server instance",
			nil, nil),
		prometheus.CounterValue,
		float64(connStats.s3InputBytes),
	)
}

// Populates prometheus with bucket usage metrics, this metrics
// is only enabled if scanner is enabled.
func bucketUsageMetricsPrometheus(ch chan<- prometheus.Metric) {
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
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
		stat := globalReplicationStats.Load().getLatestReplicationStats(bucket)
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
				prometheus.BuildFQName("bucket", "replication", "successful_size"),
				"Total capacity replicated to destination",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.ReplicationStats.ReplicatedSize),
			bucket,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("bucket", "replication", "received_size"),
				"Total capacity replicated to this instance",
				[]string{"bucket"}, nil),
			prometheus.GaugeValue,
			float64(stat.ReplicationStats.ReplicaSize),
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
		for k, v := range usageInfo.ObjectVersionsHistogram {
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					prometheus.BuildFQName(bucketNamespace, "objects", "histogram"),
					"Total number of versions of objects in a bucket",
					[]string{"bucket", "object_versions"}, nil),
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

	server := getLocalServerProperty(globalEndpoints, &http.Request{
		Host: globalLocalNodeName,
	}, true)

	onlineDisks, offlineDisks := getOnlineOfflineDisksStats(server.Disks)
	totalDisks := offlineDisks.Merge(onlineDisks)

	// Report total capacity
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_raw", "total"),
			"Total capacity online in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalCapacity(server.Disks)),
	)

	// Report total capacity free
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_raw_free", "total"),
			"Total free capacity online in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalCapacityFree(server.Disks)),
	)

	sinfo := objLayer.StorageInfo(GlobalContext, true)

	// Report total usable capacity
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_usable", "total"),
			"Total usable capacity online in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalUsableCapacity(server.Disks, sinfo)),
	)

	// Report total usable capacity free
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "capacity_usable_free", "total"),
			"Total free usable capacity online in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(GetTotalUsableCapacityFree(server.Disks, sinfo)),
	)

	// MinIO Offline Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "disks", "offline"),
			"Total number of offline drives in current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(offlineDisks.Sum()),
	)

	// MinIO Total Disks per node
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(minioNamespace, "drives", "total"),
			"Total number of drives for current MinIO server instance",
			nil, nil),
		prometheus.GaugeValue,
		float64(totalDisks.Sum()),
	)

	for _, disk := range server.Disks {
		// Total disk usage by the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "used"),
				"Total disk storage used on the drive",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.UsedSpace),
			disk.DrivePath,
		)

		// Total available space in the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "available"),
				"Total available space left on the drive",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.AvailableSpace),
			disk.DrivePath,
		)

		// Total storage space of the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(diskNamespace, "storage", "total"),
				"Total space on the drive",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(disk.TotalSpace),
			disk.DrivePath,
		)
	}
}

func metricsHandler() http.Handler {
	registry := prometheus.NewRegistry()

	logger.CriticalIf(GlobalContext, registry.Register(minioVersionInfo))

	logger.CriticalIf(GlobalContext, registry.Register(newMinioCollector()))

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		registry,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsLegacy"
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

// NoAuthMiddleware no auth middle ware.
func NoAuthMiddleware(h http.Handler) http.Handler {
	return h
}

// AuthMiddleware checks if the bearer token is valid and authorized.
func AuthMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)

		claims, groups, owner, authErr := metricsRequestAuthenticate(r)
		if authErr != nil || (claims != nil && !claims.VerifyIssuer("prometheus", true)) {
			if ok {
				tc.FuncName = "handler.MetricsAuth"
				tc.ResponseRecorder.LogErrBody = true
			}

			writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), errAuthentication), r.URL)
			return
		}

		cred := auth.Credentials{
			AccessKey: claims.AccessKey,
			Claims:    claims.Map(),
			Groups:    groups,
		}

		// For authenticated users apply IAM policy.
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.PrometheusAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
		}) {
			if ok {
				tc.FuncName = "handler.MetricsAuth"
				tc.ResponseRecorder.LogErrBody = true
			}

			writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), errAuthentication), r.URL)
			return
		}
		h.ServeHTTP(w, r)
	})
}
