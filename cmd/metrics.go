/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "minio_http_requests_total",
			Help: "Total number of requests served by current Minio server instance",
		},
		[]string{"request_type"},
	)
	httpRequestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "minio_http_requests_duration_seconds",
			Help:    "Time taken by requests served by current Minio server instance",
			Buckets: []float64{.001, .003, .005, .1, .5, 1},
		},
		[]string{"request_type"},
	)
	networkSentBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_network_sent_bytes",
			Help: "Total number of bytes sent by current Minio server instance",
		},
	)
	networkReceivedBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_network_received_bytes",
			Help: "Total number of bytes received by current Minio server instance",
		},
	)
	onlineMinioDisks = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_online_disks_total",
			Help: "Total number of online disks for current Minio server instance",
		},
	)
	offlineMinioDisks = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_offline_disks_total",
			Help: "Total number of offline disks for current Minio server instance",
		},
	)
	serverUptime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_server_uptime_seconds",
			Help: "Time elapsed since current Minio server instance started",
		},
	)
	minioStorageTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_disk_storage_bytes",
			Help: "Total disk storage available to current Minio server instance",
		},
	)
	minioStorageFree = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "minio_disk_storage_free_bytes",
			Help: "Total free disk storage available to current Minio server instance",
		},
	)
)

func init() {
	prometheus.MustRegister(httpRequests)
	prometheus.MustRegister(httpRequestsDuration)
	prometheus.MustRegister(networkSentBytes)
	prometheus.MustRegister(networkReceivedBytes)
	prometheus.MustRegister(onlineMinioDisks)
	prometheus.MustRegister(offlineMinioDisks)
	prometheus.MustRegister(serverUptime)
	prometheus.MustRegister(minioStorageTotal)
	prometheus.MustRegister(minioStorageFree)
}

func updateGeneralMetrics() {
	// Increment server uptime
	serverUptime.Set(UTCNow().Sub(globalBootTime).Seconds())

	// Fetch disk space info
	objLayer := newObjectLayerFn()
	// Service not initialized yet
	if objLayer == nil {
		return
	}

	// Update total/free disk space
	s := objLayer.StorageInfo(context.Background())
	minioStorageTotal.Set(float64(s.Total))
	minioStorageFree.Set(float64(s.Free))

	// Update online/offline disks
	onlineMinioDisks.Set(float64(s.Backend.OnlineDisks))
	offlineMinioDisks.Set(float64(s.Backend.OfflineDisks))

	// Update prometheus metric
	networkSentBytes.Set(float64(globalConnStats.getTotalOutputBytes()))
	networkReceivedBytes.Set(float64(globalConnStats.getTotalInputBytes()))
}

func metricsHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Update generic metrics before handling the prometheus scrape request
		updateGeneralMetrics()
		prometheus.Handler().ServeHTTP(w, r)
	})
}
