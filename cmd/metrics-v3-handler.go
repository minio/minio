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
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/mcontext"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/env"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type promLogger struct{}

func (p promLogger) Println(v ...any) {
	metricsLogIf(GlobalContext, fmt.Errorf("metrics handler error: %v", v))
}

type metricsV3Server struct {
	registry *prometheus.Registry
	opts     promhttp.HandlerOpts
	auth     func(http.Handler) http.Handler

	metricsData *metricsV3Collection
}

var (
	globalMetricsV3CollectorPaths []collectorPath
	globalMetricsV3Once           sync.Once
)

func newMetricsV3Server(auth func(h http.Handler) http.Handler) *metricsV3Server {
	registry := prometheus.NewRegistry()
	metricGroups := newMetricGroups(registry)
	globalMetricsV3Once.Do(func() {
		globalMetricsV3CollectorPaths = metricGroups.collectorPaths
	})
	return &metricsV3Server{
		registry: registry,
		opts: promhttp.HandlerOpts{
			ErrorLog:            promLogger{},
			ErrorHandling:       promhttp.ContinueOnError,
			Registry:            registry,
			MaxRequestsInFlight: 2,
			EnableOpenMetrics:   env.Get(EnvPrometheusOpenMetrics, config.EnableOff) == config.EnableOn,
			ProcessStartTime:    globalBootTime,
		},
		auth:        auth,
		metricsData: metricGroups,
	}
}

// metricDisplay - contains info on a metric for display purposes.
type metricDisplay struct {
	Name   string   `json:"name"`
	Help   string   `json:"help"`
	Type   string   `json:"type"`
	Labels []string `json:"labels"`
}

func (md metricDisplay) String() string {
	return fmt.Sprintf("Name: %s\nType: %s\nHelp: %s\nLabels: {%s}\n", md.Name, md.Type, md.Help, strings.Join(md.Labels, ","))
}

func (md metricDisplay) TableRow() string {
	labels := strings.Join(md.Labels, ",")
	if labels == "" {
		labels = ""
	} else {
		labels = "`" + labels + "`"
	}
	return fmt.Sprintf("| `%s` | `%s` | %s | %s |\n", md.Name, md.Type, md.Help, labels)
}

// listMetrics - returns a handler that lists all the metrics that could be
// returned for the requested path.
//
// FIXME: It currently only lists `minio_` prefixed metrics.
func (h *metricsV3Server) listMetrics(path string) http.Handler {
	// First collect all matching MetricsGroup's
	matchingMG := make(map[collectorPath]*MetricsGroup)
	for _, collPath := range h.metricsData.collectorPaths {
		if collPath.isDescendantOf(path) {
			if v, ok := h.metricsData.mgMap[collPath]; ok {
				matchingMG[collPath] = v
			} else if v, ok := h.metricsData.bucketMGMap[collPath]; ok {
				matchingMG[collPath] = v
			}
		}
	}

	if len(matchingMG) == 0 {
		return nil
	}

	var metrics []metricDisplay
	for _, collectorPath := range h.metricsData.collectorPaths {
		if mg, ok := matchingMG[collectorPath]; ok {
			var commonLabels []string
			for k := range mg.ExtraLabels {
				commonLabels = append(commonLabels, k)
			}
			for _, d := range mg.Descriptors {
				labels := slices.Clone(d.VariableLabels)
				labels = append(labels, commonLabels...)
				metric := metricDisplay{
					Name:   mg.MetricFQN(d.Name),
					Help:   d.Help,
					Type:   d.Type.String(),
					Labels: labels,
				}
				metrics = append(metrics, metric)
			}
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		if contentType == "application/json" {
			w.Header().Set("Content-Type", "application/json")
			jsonEncoder := json.NewEncoder(w)
			jsonEncoder.Encode(metrics)
			return
		}

		// If not JSON, return plain text. We format it as a markdown table for
		// readability.
		w.Header().Set("Content-Type", "text/plain")
		var b strings.Builder
		b.WriteString("| Name | Type | Help | Labels |\n")
		b.WriteString("| ---- | ---- | ---- | ------ |\n")
		for _, metric := range metrics {
			b.WriteString(metric.TableRow())
		}
		w.Write([]byte(b.String()))
	})
}

func (h *metricsV3Server) handle(path string, isListingRequest bool, buckets []string) http.Handler {
	var notFoundHandler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Metrics Resource Not found", http.StatusNotFound)
	})

	// Require that metrics path has one component at least.
	if path == "/" {
		return notFoundHandler
	}

	if isListingRequest {
		handler := h.listMetrics(path)
		if handler == nil {
			return notFoundHandler
		}
		return handler
	}

	// In each of the following cases, we check if the collect path is a
	// descendant of `path`, and if so, we add the corresponding gatherer to
	// the list of gatherers. This way, /api/a will return all metrics returned
	// by /api/a/b and /api/a/c (and any other matching descendant collector
	// paths).

	var gatherers []prometheus.Gatherer
	for _, collectorPath := range h.metricsData.collectorPaths {
		if collectorPath.isDescendantOf(path) {
			gatherer := h.metricsData.mgGatherers[collectorPath]

			// For Bucket metrics we need to set the buckets argument inside the
			// metric group, so that it will affect collection. If no buckets
			// are provided, we will not return bucket metrics.
			if bmg, ok := h.metricsData.bucketMGMap[collectorPath]; ok {
				if len(buckets) == 0 {
					continue
				}
				unLocker := bmg.LockAndSetBuckets(buckets)
				defer unLocker()
			}
			gatherers = append(gatherers, gatherer)
		}
	}

	if len(gatherers) == 0 {
		return notFoundHandler
	}

	return promhttp.HandlerFor(prometheus.Gatherers(gatherers), h.opts)
}

// ServeHTTP - implements http.Handler interface.
//
// When the `list` query parameter is provided (its value is ignored), the
// server lists all metrics that could be returned for the requested path.
//
// The (repeatable) `buckets` query parameter is a list of bucket names (or it
// could be a comma separated value) to return metrics with a bucket label.
// Bucket metrics will be returned only for the provided buckets. If no buckets
// parameter is provided, no bucket metrics are returned.
func (h *metricsV3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pathComponents := mux.Vars(r)["pathComps"]
	isListingRequest := r.Form.Has("list")

	var buckets []string
	if strings.HasPrefix(pathComponents, "/bucket/") {
		// bucket specific metrics, extract the bucket name from the path.
		// it's the last part of the path. e.g. /bucket/api/<bucket-name>
		bucketIdx := strings.LastIndex(pathComponents, "/")
		buckets = append(buckets, pathComponents[bucketIdx+1:])
		// remove bucket from pathComponents as it is dynamic and
		// hence not included in the collector path.
		pathComponents = pathComponents[:bucketIdx]
	}

	innerHandler := h.handle(pathComponents, isListingRequest, buckets)

	// Add tracing to the prom. handler
	tracedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsV3"
			tc.ResponseRecorder.LogErrBody = true
		}

		innerHandler.ServeHTTP(w, r)
	})

	// Add authentication
	h.auth(tracedHandler).ServeHTTP(w, r)
}
