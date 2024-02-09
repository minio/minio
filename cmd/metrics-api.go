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
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	apiCollector           *minioAPICollector
	apiBucketCollector     *minioAPICollector
	apiBucketReplCollector *minioAPICollector
	apiObjectCollector     *minioAPICollector
	apiMetricsGroup        []*MetricsGroup
	apiBucketMetrics       []*MetricsGroup
	apiObjectMetrics       []*MetricsGroup
	apiBucketReplMetrics   []*MetricsGroup
)

func init() {
	apiMetricsGroup = []*MetricsGroup{
		getHTTPMetrics(MetricsGroupOpts{}, apiMetricNamespace),
		getS3TTFBMetric(apiMetricNamespace),
		getNetworkMetrics(MetricsGroupOpts{ioStatsOnly: true}, apiMetricNamespace),
	}
	apiBucketMetrics = []*MetricsGroup{
		getHTTPMetrics(MetricsGroupOpts{bucketOnly: true}, bucketMetricNamespace),
		getBucketTTFBMetric(bucketMetricNamespace),
		getBucketUsageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
	}
	apiBucketReplMetrics = []*MetricsGroup{
		getBucketUsageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, replicationOnlyV3: true}),
	}

	apiCollector = newMinioAPICollector(apiMetricsGroup)
	apiBucketCollector = newMinioAPICollector(apiBucketMetrics)
	apiObjectCollector = newMinioAPICollector(apiObjectMetrics)
	apiBucketReplCollector = newMinioAPICollector(apiBucketReplMetrics)
}

// minioAPICollector is the Collector for API metrics
type minioAPICollector struct {
	metricsGroups []*MetricsGroup
	desc          *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioAPICollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioAPICollector) Collect(ch chan<- prometheus.Metric) {
	// Expose MinIO's version information
	minioVersionInfo.WithLabelValues(Version, CommitID).Set(1.0)

	populateAndPublish(c.metricsGroups, func(metric Metric) bool {
		fmt.Println("Collecting metric", metric.Description.Name, metric.Description.Help, metric.Description.Type, metric.Value, metric.VariableLabels, metric.StaticLabels)
		labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
		values = append(values, globalLocalNodeName)
		labels = append(labels, serverName)
		values = append(values, strconv.Itoa(globalLocalPoolIdx))
		labels = append(labels, poolIndex)
		if metric.Description.Type == histogramMetric {
			if metric.Histogram == nil {
				return true
			}
			for _, v := range metric.Histogram {
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

// newMinioAPICollector describes the collector
// and returns reference of minio api Collector
// It creates the Prometheus Description which is used
// to define Metric and  help string
func newMinioAPICollector(metricsGroups []*MetricsGroup) *minioAPICollector {
	return &minioAPICollector{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_api_stats", "API statistics exposed by MinIO server", nil, nil),
	}
}

// metricsV3APIHandler is the prometheus handler for resource metrics
func metricsV3APIHandler() http.Handler {
	fmt.Println("metricsAPI handler  setup to apiCollect")

	return metricsHTTPHandler(apiCollector, "handler.MetricsV3API")
}

// metricsV3APIObjectHandler is the prometheus handler for resource metrics
func metricsV3APIObjectHandler() http.Handler {
	return metricsHTTPHandler(apiObjectCollector, "handler.MetricsV3APIObject")
}

// metricsV3APIBucketHandler is the prometheus handler for bucket metrics
func metricsV3APIBucketHandler() http.Handler {
	fmt.Println("metricsAPIBucket handler  setup to apiBucketCollect")
	return metricsHTTPHandler(apiBucketCollector, "handler.MetricsV3APIBucket")
}

// metricsV3BucketReplHandler is the prometheus handler for bucket replication metrics
func metricsV3BucketReplHandler() http.Handler {
	fmt.Println("metricsAPIBucketRepl handler  setup to apiBucketReplCollect")
	return metricsHTTPHandler(apiBucketReplCollector, "handler.MetricsV3APIBucketRepl")
}
