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
	"slices"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/logger"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type collectorPath string

// metricPrefix converts a collector path to a metric name prefix. The path is
// converted to snake-case (by replaced '/' and '-' with '_') and prefixed with
// `minio_`.
func (cp collectorPath) metricPrefix() string {
	s := strings.TrimPrefix(string(cp), SlashSeparator)
	s = strings.ReplaceAll(s, SlashSeparator, "_")
	s = strings.ReplaceAll(s, "-", "_")
	return "minio_" + s
}

// isDescendantOf returns true if it is a descendant of (or the same as)
// `ancestor`.
//
// For example:
//
//	 	/a, /a/b, /a/b/c are all descendants of /a.
//		/abc or /abd/a are not descendants of /ab.
func (cp collectorPath) isDescendantOf(arg string) bool {
	descendant := string(cp)
	if descendant == arg {
		return true
	}
	if len(arg) >= len(descendant) {
		return false
	}
	if !strings.HasSuffix(arg, SlashSeparator) {
		arg += SlashSeparator
	}
	return strings.HasPrefix(descendant, arg)
}

// MetricType - represents the type of a metric.
type MetricType int

const (
	// CounterMT - represents a counter metric.
	CounterMT MetricType = iota
	// GaugeMT - represents a gauge metric.
	GaugeMT
	// HistogramMT - represents a histogram metric.
	HistogramMT
)

// rangeL - represents a range label.
const rangeL = "range"

func (mt MetricType) String() string {
	switch mt {
	case CounterMT:
		return "counter"
	case GaugeMT:
		return "gauge"
	case HistogramMT:
		return "histogram"
	default:
		return "*unknown*"
	}
}

func (mt MetricType) toProm() prometheus.ValueType {
	switch mt {
	case CounterMT:
		return prometheus.CounterValue
	case GaugeMT:
		return prometheus.GaugeValue
	case HistogramMT:
		return prometheus.CounterValue
	default:
		panic(fmt.Sprintf("unknown metric type: %d", mt))
	}
}

// MetricDescriptor - represents a metric descriptor.
type MetricDescriptor struct {
	Name           MetricName
	Type           MetricType
	Help           string
	VariableLabels []string

	// managed values follow:
	labelSet map[string]struct{}
}

func (md *MetricDescriptor) getLabelSet() map[string]struct{} {
	if md.labelSet != nil {
		return md.labelSet
	}
	md.labelSet = make(map[string]struct{}, len(md.VariableLabels))
	for _, label := range md.VariableLabels {
		md.labelSet[label] = struct{}{}
	}
	return md.labelSet
}

func (md *MetricDescriptor) toPromName(namePrefix string) string {
	return prometheus.BuildFQName(namePrefix, "", string(md.Name))
}

func (md *MetricDescriptor) toPromDesc(namePrefix string, extraLabels map[string]string) *prometheus.Desc {
	return prometheus.NewDesc(
		md.toPromName(namePrefix),
		md.Help,
		md.VariableLabels, extraLabels,
	)
}

// NewCounterMD - creates a new counter metric descriptor.
func NewCounterMD(name MetricName, help string, labels ...string) MetricDescriptor {
	return MetricDescriptor{
		Name:           name,
		Type:           CounterMT,
		Help:           help,
		VariableLabels: labels,
	}
}

// NewGaugeMD - creates a new gauge metric descriptor.
func NewGaugeMD(name MetricName, help string, labels ...string) MetricDescriptor {
	return MetricDescriptor{
		Name:           name,
		Type:           GaugeMT,
		Help:           help,
		VariableLabels: labels,
	}
}

type metricValue struct {
	Labels map[string]string
	Value  float64
}

// MetricValues - type to set metric values retrieved while loading metrics. A
// value of this type is passed to the `MetricsLoaderFn`.
type MetricValues struct {
	values      map[MetricName][]metricValue
	descriptors map[MetricName]MetricDescriptor
}

func newMetricValues(d map[MetricName]MetricDescriptor) MetricValues {
	return MetricValues{
		values:      make(map[MetricName][]metricValue, len(d)),
		descriptors: d,
	}
}

// ToPromMetrics - converts the internal metric values to Prometheus
// adding the given name prefix. The extraLabels are added to each metric as
// constant labels.
func (m *MetricValues) ToPromMetrics(namePrefix string, extraLabels map[string]string,
) []prometheus.Metric {
	metrics := make([]prometheus.Metric, 0, len(m.values))
	for metricName, mv := range m.values {
		desc := m.descriptors[metricName]
		promDesc := desc.toPromDesc(namePrefix, extraLabels)
		for _, v := range mv {
			// labelValues is in the same order as the variable labels in the
			// descriptor.
			labelValues := make([]string, 0, len(v.Labels))
			for _, k := range desc.VariableLabels {
				labelValues = append(labelValues, v.Labels[k])
			}
			metrics = append(metrics,
				prometheus.MustNewConstMetric(promDesc, desc.Type.toProm(), v.Value,
					labelValues...))
		}
	}
	return metrics
}

// Set - sets a metric value along with any provided labels. It is used only
// with Gauge and Counter metrics.
//
// If the MetricName given here is not present in the `MetricsGroup`'s
// descriptors, this function panics.
//
// Panics if `labels` is not a list of ordered label name and label value pairs
// or if all labels for the metric are not provided.
func (m *MetricValues) Set(name MetricName, value float64, labels ...string) {
	desc, ok := m.descriptors[name]
	if !ok {
		panic(fmt.Sprintf("metric has no description: %s", name))
	}

	if len(labels)%2 != 0 {
		panic("labels must be a list of ordered key-value pairs")
	}

	validLabels := desc.getLabelSet()
	labelMap := make(map[string]string, len(labels)/2)
	for i := 0; i < len(labels); i += 2 {
		if _, ok := validLabels[labels[i]]; !ok {
			panic(fmt.Sprintf("invalid label: %s (metric: %s)", labels[i], name))
		}
		labelMap[labels[i]] = labels[i+1]
	}

	if len(labels)/2 != len(validLabels) {
		panic("not all labels were given values")
	}

	v, ok := m.values[name]
	if !ok {
		v = make([]metricValue, 0, 1)
	}
	// If valid non zero value set the metrics
	if value > 0 {
		m.values[name] = append(v, metricValue{
			Labels: labelMap,
			Value:  value,
		})
	}
}

// SetHistogram - sets values for the given MetricName using the provided
// histogram.
//
// `filterByLabels` is a map of label names to list of allowed label values to
// filter by. Note that this filtering happens before any renaming of labels.
//
// `renameLabels` is a map of label names to rename. The keys are the original
// label names and the values are the new label names.
//
// `bucketFilter` is a list of bucket values to filter. If this is non-empty,
// only metrics for the given buckets are added.
//
// `extraLabels` are additional labels to add to each metric. They are ordered
// label name and value pairs.
func (m *MetricValues) SetHistogram(name MetricName, hist *prometheus.HistogramVec,
	filterByLabels map[string]set.StringSet, renameLabels map[string]string, bucketFilter []string,
	extraLabels ...string,
) {
	if _, ok := m.descriptors[name]; !ok {
		panic(fmt.Sprintf("metric has no description: %s", name))
	}
	dummyDesc := MetricDescription{}
	metricsV2 := getHistogramMetrics(hist, dummyDesc, false, false)
mainLoop:
	for _, metric := range metricsV2 {
		for label, allowedValues := range filterByLabels {
			if !allowedValues.Contains(metric.VariableLabels[label]) {
				continue mainLoop
			}
		}

		// If a bucket filter is provided, only add metrics for the given
		// buckets.
		if len(bucketFilter) > 0 && !slices.Contains(bucketFilter, metric.VariableLabels["bucket"]) {
			continue
		}

		labels := make([]string, 0, len(metric.VariableLabels)*2)
		for k, v := range metric.VariableLabels {
			if newLabel, ok := renameLabels[k]; ok {
				labels = append(labels, newLabel, v)
			} else {
				labels = append(labels, k, v)
			}
		}
		labels = append(labels, extraLabels...)
		// If valid non zero value set the metrics
		if metric.Value > 0 {
			m.Set(name, metric.Value, labels...)
		}
	}
}

// SetHistogramValues - sets values for the given MetricName using the provided map of
// range to value.
func SetHistogramValues[V uint64 | int64 | float64](m MetricValues, name MetricName, values map[string]V, labels ...string) {
	for rng, val := range values {
		m.Set(name, float64(val), append(labels, rangeL, rng)...)
	}
}

// MetricsLoaderFn - represents a function to load metrics from the
// metricsCache.
//
// Note that returning an error here will cause the Metrics handler to return a
// 500 Internal Server Error.
type MetricsLoaderFn func(context.Context, MetricValues, *metricsCache) error

// JoinLoaders - joins multiple loaders into a single loader. The returned
// loader will call each of the given loaders in order. If any of the loaders
// return an error, the returned loader will return that error.
func JoinLoaders(loaders ...MetricsLoaderFn) MetricsLoaderFn {
	return func(ctx context.Context, m MetricValues, c *metricsCache) error {
		for _, loader := range loaders {
			if err := loader(ctx, m, c); err != nil {
				return err
			}
		}
		return nil
	}
}

// BucketMetricsLoaderFn - represents a function to load metrics from the
// metricsCache and the system for a given list of buckets.
//
// Note that returning an error here will cause the Metrics handler to return a
// 500 Internal Server Error.
type BucketMetricsLoaderFn func(context.Context, MetricValues, *metricsCache, []string) error

// JoinBucketLoaders - joins multiple bucket loaders into a single loader,
// similar to `JoinLoaders`.
func JoinBucketLoaders(loaders ...BucketMetricsLoaderFn) BucketMetricsLoaderFn {
	return func(ctx context.Context, m MetricValues, c *metricsCache, b []string) error {
		for _, loader := range loaders {
			if err := loader(ctx, m, c, b); err != nil {
				return err
			}
		}
		return nil
	}
}

// MetricsGroup - represents a group of metrics. It includes a `MetricsLoaderFn`
// function that provides a way to load the metrics from the system. The metrics
// are cached and refreshed after a given timeout.
//
// For metrics with a `bucket` dimension, a list of buckets argument is required
// to collect the metrics.
//
// It implements the prometheus.Collector interface for metric groups without a
// bucket dimension. For metric groups with a bucket dimension, use the
// `GetBucketCollector` method to get a `BucketCollector` that implements the
// prometheus.Collector interface.
type MetricsGroup struct {
	// Path (relative to the Metrics v3 base endpoint) at which this group of
	// metrics is served. This value is converted into a metric name prefix
	// using `.metricPrefix()` and is added to each metric returned.
	CollectorPath collectorPath
	// List of all metric descriptors that could be returned by the loader.
	Descriptors []MetricDescriptor
	// (Optional) Extra (constant) label KV pairs to be added to each metric in
	// the group.
	ExtraLabels map[string]string

	// Loader functions to load metrics. Only one of these will be set. Metrics
	// returned by these functions must be present in the `Descriptors` list.
	loader       MetricsLoaderFn
	bucketLoader BucketMetricsLoaderFn

	// Cache for all metrics groups. Set via `.SetCache` method.
	cache *metricsCache

	// managed values follow:

	// map of metric descriptors by metric name.
	descriptorMap map[MetricName]MetricDescriptor

	// For bucket metrics, the list of buckets is stored here. It is used in the
	// Collect() call. This is protected by the `bucketsLock`.
	bucketsLock sync.Mutex
	buckets     []string
}

// NewMetricsGroup creates a new MetricsGroup. To create a metrics group for
// metrics with a `bucket` dimension (label), use `NewBucketMetricsGroup`.
//
// The `loader` function loads metrics from the cache and the system.
func NewMetricsGroup(path collectorPath, descriptors []MetricDescriptor,
	loader MetricsLoaderFn,
) *MetricsGroup {
	mg := &MetricsGroup{
		CollectorPath: path,
		Descriptors:   descriptors,
		loader:        loader,
	}
	mg.validate()
	return mg
}

// NewBucketMetricsGroup creates a new MetricsGroup for metrics with a `bucket`
// dimension (label).
//
// The `loader` function loads metrics from the cache and the system for a given
// list of buckets.
func NewBucketMetricsGroup(path collectorPath, descriptors []MetricDescriptor,
	loader BucketMetricsLoaderFn,
) *MetricsGroup {
	mg := &MetricsGroup{
		CollectorPath: path,
		Descriptors:   descriptors,
		bucketLoader:  loader,
	}
	mg.validate()
	return mg
}

// AddExtraLabels - adds extra (constant) label KV pairs to the metrics group.
// This is a helper to initialize the `ExtraLabels` field. The argument is a
// list of ordered label name and value pairs.
func (mg *MetricsGroup) AddExtraLabels(labels ...string) {
	if len(labels)%2 != 0 {
		panic("Labels must be an ordered list of name value pairs")
	}
	if mg.ExtraLabels == nil {
		mg.ExtraLabels = make(map[string]string, len(labels))
	}
	for i := 0; i < len(labels); i += 2 {
		mg.ExtraLabels[labels[i]] = labels[i+1]
	}
}

// IsBucketMetricsGroup - returns true if the given MetricsGroup is a bucket
// metrics group.
func (mg *MetricsGroup) IsBucketMetricsGroup() bool {
	return mg.bucketLoader != nil
}

// Describe - implements prometheus.Collector interface.
func (mg *MetricsGroup) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range mg.Descriptors {
		ch <- desc.toPromDesc(mg.CollectorPath.metricPrefix(), mg.ExtraLabels)
	}
}

// Collect - implements prometheus.Collector interface.
func (mg *MetricsGroup) Collect(ch chan<- prometheus.Metric) {
	metricValues := newMetricValues(mg.descriptorMap)

	var err error
	if mg.IsBucketMetricsGroup() {
		err = mg.bucketLoader(GlobalContext, metricValues, mg.cache, mg.buckets)
	} else {
		err = mg.loader(GlobalContext, metricValues, mg.cache)
	}

	// There is no way to handle errors here, so we panic the current goroutine
	// and the Metrics API handler returns a 500 HTTP status code. This should
	// normally not happen, and usually indicates a bug.
	logger.CriticalIf(GlobalContext, errors.Wrap(err, "failed to get metrics"))

	promMetrics := metricValues.ToPromMetrics(mg.CollectorPath.metricPrefix(),
		mg.ExtraLabels)
	for _, metric := range promMetrics {
		ch <- metric
	}
}

// LockAndSetBuckets - locks the buckets and sets the given buckets. It returns
// a function to unlock the buckets.
func (mg *MetricsGroup) LockAndSetBuckets(buckets []string) func() {
	mg.bucketsLock.Lock()
	mg.buckets = buckets
	return func() {
		mg.bucketsLock.Unlock()
	}
}

// MetricFQN - returns the fully qualified name for the given metric name.
func (mg *MetricsGroup) MetricFQN(name MetricName) string {
	v, ok := mg.descriptorMap[name]
	if !ok {
		// This should never happen.
		return ""
	}
	return v.toPromName(mg.CollectorPath.metricPrefix())
}

func (mg *MetricsGroup) validate() {
	if len(mg.Descriptors) == 0 {
		panic("Descriptors must be set")
	}

	// For bools A and B, A XOR B <=> A != B.
	isExactlyOneSet := (mg.loader == nil) != (mg.bucketLoader == nil)
	if !isExactlyOneSet {
		panic("Exactly one Loader function must be set")
	}

	mg.descriptorMap = make(map[MetricName]MetricDescriptor, len(mg.Descriptors))
	for _, desc := range mg.Descriptors {
		mg.descriptorMap[desc.Name] = desc
	}
}

// SetCache is a helper to initialize MetricsGroup. It sets the cache object.
func (mg *MetricsGroup) SetCache(c *metricsCache) {
	mg.cache = c
}
