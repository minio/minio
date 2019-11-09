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
	"context"
	"net/http"
	"strings"

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

	// Fetch disk space info
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	storageAPIs := []StorageAPI{}
	for _, endpoint := range globalEndpoints {
		if endpoint.IsLocal {
			// Construct storageAPIs.
			sAPI, _ := newStorageAPI(endpoint)
			storageAPIs = append(storageAPIs, sAPI)
		}
	}

	disksInfo, onlineDisks, offlineDisks := getDisksInfo(storageAPIs)
	totalDisks := offlineDisks.Merge(onlineDisks)

	for _, offDisks := range offlineDisks {
		// MinIO Offline Disks per node
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("minio", "disks", "offline"),
				"Total number of offline disks in current MinIO server instance",
				nil, nil),
			prometheus.GaugeValue,
			float64(offDisks),
		)
	}

	for _, totDisks := range totalDisks {
		// MinIO Total Disks per node
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("minio", "disks", "total"),
				"Total number of disks for current MinIO server instance",
				nil, nil),
			prometheus.GaugeValue,
			float64(totDisks),
		)
	}

	localPeer := GetLocalPeer(globalEndpoints)
	for _, di := range disksInfo {
		// Trim the host
		absPath := strings.TrimPrefix(di.RelativePath, localPeer)

		// Total disk usage by the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "used"),
				"Total disk storage used on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(di.Total-di.Free),
			absPath,
		)

		// Total available space in the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "available"),
				"Total available space left on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(di.Free),
			absPath,
		)

		// Total storage space of the disk
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("disk", "storage", "total"),
				"Total space on the disk",
				[]string{"disk"}, nil),
			prometheus.GaugeValue,
			float64(di.Total),
			absPath,
		)
	}

	connStats := globalConnStats.toServerConnStats()
	httpStats := globalHTTPStats.toServerHTTPStats()

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

func metricsHandler() http.Handler {

	registry := prometheus.NewRegistry()

	err := registry.Register(minioVersionInfo)
	logger.LogIf(context.Background(), err)

	err = registry.Register(httpRequestsDuration)
	logger.LogIf(context.Background(), err)

	err = registry.Register(newMinioCollector())
	logger.LogIf(context.Background(), err)

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
