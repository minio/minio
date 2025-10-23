// Copyright (c) 2015-2023 MinIO, Inc.
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
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	resourceMetricsCollectionInterval = time.Minute
	resourceMetricsCacheInterval      = time.Minute

	// drive stats
	totalInodes    MetricName = "total_inodes"
	readsPerSec    MetricName = "reads_per_sec"
	writesPerSec   MetricName = "writes_per_sec"
	readsKBPerSec  MetricName = "reads_kb_per_sec"
	writesKBPerSec MetricName = "writes_kb_per_sec"
	readsAwait     MetricName = "reads_await"
	writesAwait    MetricName = "writes_await"
	percUtil       MetricName = "perc_util"
	usedInodes     MetricName = "used_inodes"

	// network stats
	interfaceRxBytes  MetricName = "rx_bytes"
	interfaceRxErrors MetricName = "rx_errors"
	interfaceTxBytes  MetricName = "tx_bytes"
	interfaceTxErrors MetricName = "tx_errors"

	// cpu stats
	cpuUser       MetricName = "user"
	cpuSystem     MetricName = "system"
	cpuIOWait     MetricName = "iowait"
	cpuIdle       MetricName = "idle"
	cpuNice       MetricName = "nice"
	cpuSteal      MetricName = "steal"
	cpuLoad1      MetricName = "load1"
	cpuLoad5      MetricName = "load5"
	cpuLoad15     MetricName = "load15"
	cpuLoad1Perc  MetricName = "load1_perc"
	cpuLoad5Perc  MetricName = "load5_perc"
	cpuLoad15Perc MetricName = "load15_perc"
)

var (
	resourceCollector *minioResourceCollector
	// resourceMetricsMap is a map of subsystem to its metrics
	resourceMetricsMap   map[MetricSubsystem]ResourceMetrics
	resourceMetricsMapMu sync.RWMutex
	// resourceMetricsHelpMap maps metric name to its help string
	resourceMetricsHelpMap map[MetricName]string
	resourceMetricsGroups  []*MetricsGroupV2
	// initial values for drives (at the time  of server startup)
	// used for calculating avg values for drive metrics
	latestDriveStats      map[string]madmin.DiskIOStats
	latestDriveStatsMu    sync.RWMutex
	lastDriveStatsRefresh time.Time
)

// PeerResourceMetrics represents the resource metrics
// retrieved from a peer, along with errors if any
type PeerResourceMetrics struct {
	Metrics map[MetricSubsystem]ResourceMetrics
	Errors  []string
}

// ResourceMetrics is a map of unique key identifying
// a resource metric (e.g. reads_per_sec_{node}_{drive})
// to its data
type ResourceMetrics map[string]ResourceMetric

// ResourceMetric represents a single resource metric
// The metrics are collected from all servers periodically
// and stored in the resource metrics map.
// It also maintains the count of number of times this metric
// was collected since the server started, and the sum,
// average and max values across the same.
type ResourceMetric struct {
	Name   MetricName
	Labels map[string]string

	// value captured in current cycle
	Current float64

	// Used when system provides cumulative (since uptime) values
	// helps in calculating the current value by comparing the new
	// cumulative value with previous one
	Cumulative float64

	Max   float64
	Avg   float64
	Sum   float64
	Count uint64
}

func init() {
	interval := fmt.Sprintf("%ds", int(resourceMetricsCollectionInterval.Seconds()))
	resourceMetricsHelpMap = map[MetricName]string{
		interfaceRxBytes:  "Bytes received on the interface in " + interval,
		interfaceRxErrors: "Receive errors in " + interval,
		interfaceTxBytes:  "Bytes transmitted in " + interval,
		interfaceTxErrors: "Transmit errors in " + interval,
		total:             "Total memory on the node",
		memUsed:           "Used memory on the node",
		memUsedPerc:       "Used memory percentage on the node",
		memFree:           "Free memory on the node",
		memShared:         "Shared memory on the node",
		memBuffers:        "Buffers memory on the node",
		memCache:          "Cache memory on the node",
		memAvailable:      "Available memory on the node",
		readsPerSec:       "Reads per second on a drive",
		writesPerSec:      "Writes per second on a drive",
		readsKBPerSec:     "Kilobytes read per second on a drive",
		writesKBPerSec:    "Kilobytes written per second on a drive",
		readsAwait:        "Average time for read requests to be served on a drive",
		writesAwait:       "Average time for write requests to be served on a drive",
		percUtil:          "Percentage of time the disk was busy",
		usedBytes:         "Used bytes on a drive",
		totalBytes:        "Total bytes on a drive",
		usedInodes:        "Total inodes used on a drive",
		totalInodes:       "Total inodes on a drive",
		cpuUser:           "CPU user time",
		cpuSystem:         "CPU system time",
		cpuIdle:           "CPU idle time",
		cpuIOWait:         "CPU ioWait time",
		cpuSteal:          "CPU steal time",
		cpuNice:           "CPU nice time",
		cpuLoad1:          "CPU load average 1min",
		cpuLoad5:          "CPU load average 5min",
		cpuLoad15:         "CPU load average 15min",
		cpuLoad1Perc:      "CPU load average 1min (percentage)",
		cpuLoad5Perc:      "CPU load average 5min (percentage)",
		cpuLoad15Perc:     "CPU load average 15min (percentage)",
	}
	resourceMetricsGroups = []*MetricsGroupV2{
		getResourceMetrics(),
	}

	resourceCollector = newMinioResourceCollector(resourceMetricsGroups)
}

func getResourceKey(name MetricName, labels map[string]string) string {
	// labels are used to uniquely identify a metric
	// e.g. reads_per_sec_{drive} inside the map
	sfx := ""
	for _, v := range labels {
		if len(sfx) > 0 {
			sfx += "_"
		}
		sfx += v
	}

	return string(name) + "_" + sfx
}

func updateResourceMetrics(subSys MetricSubsystem, name MetricName, val float64, labels map[string]string, isCumulative bool) {
	resourceMetricsMapMu.Lock()
	defer resourceMetricsMapMu.Unlock()
	subsysMetrics, found := resourceMetricsMap[subSys]
	if !found {
		subsysMetrics = ResourceMetrics{}
	}

	key := getResourceKey(name, labels)
	metric, found := subsysMetrics[key]
	if !found {
		metric = ResourceMetric{
			Name:   name,
			Labels: labels,
		}
	}

	if isCumulative {
		metric.Current = val - metric.Cumulative
		metric.Cumulative = val
	} else {
		metric.Current = val
	}

	if metric.Current > metric.Max {
		metric.Max = val
	}

	metric.Sum += metric.Current
	metric.Count++

	metric.Avg = metric.Sum / float64(metric.Count)
	subsysMetrics[key] = metric

	resourceMetricsMap[subSys] = subsysMetrics
}

// updateDriveIOStats - Updates the drive IO stats by calculating the difference between the current and latest updated values.
func updateDriveIOStats(currentStats madmin.DiskIOStats, latestStats madmin.DiskIOStats, labels map[string]string) {
	sectorSize := uint64(512)
	kib := float64(1 << 10)
	diffInSeconds := time.Now().UTC().Sub(lastDriveStatsRefresh).Seconds()
	if diffInSeconds == 0 {
		// too soon to update the stats
		return
	}
	diffStats := getDiffStats(latestStats, currentStats)

	updateResourceMetrics(driveSubsystem, readsPerSec, float64(diffStats.ReadIOs)/diffInSeconds, labels, false)
	readKib := float64(diffStats.ReadSectors*sectorSize) / kib
	updateResourceMetrics(driveSubsystem, readsKBPerSec, readKib/diffInSeconds, labels, false)

	updateResourceMetrics(driveSubsystem, writesPerSec, float64(diffStats.WriteIOs)/diffInSeconds, labels, false)
	writeKib := float64(diffStats.WriteSectors*sectorSize) / kib
	updateResourceMetrics(driveSubsystem, writesKBPerSec, writeKib/diffInSeconds, labels, false)

	rdAwait := 0.0
	if diffStats.ReadIOs > 0 {
		rdAwait = float64(diffStats.ReadTicks) / float64(diffStats.ReadIOs)
	}
	updateResourceMetrics(driveSubsystem, readsAwait, rdAwait, labels, false)

	wrAwait := 0.0
	if diffStats.WriteIOs > 0 {
		wrAwait = float64(diffStats.WriteTicks) / float64(diffStats.WriteIOs)
	}
	updateResourceMetrics(driveSubsystem, writesAwait, wrAwait, labels, false)
	updateResourceMetrics(driveSubsystem, percUtil, float64(diffStats.TotalTicks)/(diffInSeconds*10), labels, false)
}

func collectDriveMetrics(m madmin.RealtimeMetrics) {
	latestDriveStatsMu.Lock()
	for d, dm := range m.ByDisk {
		labels := map[string]string{"drive": d}
		latestStats, ok := latestDriveStats[d]
		if !ok {
			latestDriveStats[d] = dm.IOStats
			continue
		}
		updateDriveIOStats(dm.IOStats, latestStats, labels)
		latestDriveStats[d] = dm.IOStats
	}
	lastDriveStatsRefresh = time.Now().UTC()
	latestDriveStatsMu.Unlock()

	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	for _, d := range localDrives {
		di, err := d.DiskInfo(GlobalContext, DiskInfoOptions{})
		labels := map[string]string{"drive": di.Endpoint}
		if err == nil {
			updateResourceMetrics(driveSubsystem, usedBytes, float64(di.Used), labels, false)
			updateResourceMetrics(driveSubsystem, totalBytes, float64(di.Total), labels, false)
			updateResourceMetrics(driveSubsystem, usedInodes, float64(di.UsedInodes), labels, false)
			updateResourceMetrics(driveSubsystem, totalInodes, float64(di.FreeInodes+di.UsedInodes), labels, false)
		}
	}
}

func collectLocalResourceMetrics() {
	types := madmin.MetricsDisk | madmin.MetricNet | madmin.MetricsMem | madmin.MetricsCPU

	m := collectLocalMetrics(types, collectMetricsOpts{})
	for _, hm := range m.ByHost {
		if hm.Net != nil && len(hm.Net.NetStats.Name) > 0 {
			stats := hm.Net.NetStats
			labels := map[string]string{"interface": stats.Name}
			updateResourceMetrics(interfaceSubsystem, interfaceRxBytes, float64(stats.RxBytes), labels, true)
			updateResourceMetrics(interfaceSubsystem, interfaceRxErrors, float64(stats.RxErrors), labels, true)
			updateResourceMetrics(interfaceSubsystem, interfaceTxBytes, float64(stats.TxBytes), labels, true)
			updateResourceMetrics(interfaceSubsystem, interfaceTxErrors, float64(stats.TxErrors), labels, true)
		}
		if hm.Mem != nil && len(hm.Mem.Info.Addr) > 0 {
			labels := map[string]string{}
			stats := hm.Mem.Info
			updateResourceMetrics(memSubsystem, total, float64(stats.Total), labels, false)
			updateResourceMetrics(memSubsystem, memUsed, float64(stats.Used), labels, false)
			perc := math.Round(float64(stats.Used*100*100)/float64(stats.Total)) / 100
			updateResourceMetrics(memSubsystem, memUsedPerc, perc, labels, false)
			updateResourceMetrics(memSubsystem, memFree, float64(stats.Free), labels, false)
			updateResourceMetrics(memSubsystem, memShared, float64(stats.Shared), labels, false)
			updateResourceMetrics(memSubsystem, memBuffers, float64(stats.Buffers), labels, false)
			updateResourceMetrics(memSubsystem, memAvailable, float64(stats.Available), labels, false)
			updateResourceMetrics(memSubsystem, memCache, float64(stats.Cache), labels, false)
		}
		if hm.CPU != nil {
			labels := map[string]string{}
			ts := hm.CPU.TimesStat
			if ts != nil {
				tot := ts.User + ts.System + ts.Idle + ts.Iowait + ts.Nice + ts.Steal
				cpuUserVal := math.Round(ts.User/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuUser, cpuUserVal, labels, false)
				cpuSystemVal := math.Round(ts.System/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuSystem, cpuSystemVal, labels, false)
				cpuIdleVal := math.Round(ts.Idle/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuIdle, cpuIdleVal, labels, false)
				cpuIOWaitVal := math.Round(ts.Iowait/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuIOWait, cpuIOWaitVal, labels, false)
				cpuNiceVal := math.Round(ts.Nice/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuNice, cpuNiceVal, labels, false)
				cpuStealVal := math.Round(ts.Steal/tot*100*100) / 100
				updateResourceMetrics(cpuSubsystem, cpuSteal, cpuStealVal, labels, false)
			}
			ls := hm.CPU.LoadStat
			if ls != nil {
				updateResourceMetrics(cpuSubsystem, cpuLoad1, ls.Load1, labels, false)
				updateResourceMetrics(cpuSubsystem, cpuLoad5, ls.Load5, labels, false)
				updateResourceMetrics(cpuSubsystem, cpuLoad15, ls.Load15, labels, false)
				if hm.CPU.CPUCount > 0 {
					perc := math.Round(ls.Load1*100*100/float64(hm.CPU.CPUCount)) / 100
					updateResourceMetrics(cpuSubsystem, cpuLoad1Perc, perc, labels, false)
					perc = math.Round(ls.Load5*100*100/float64(hm.CPU.CPUCount)) / 100
					updateResourceMetrics(cpuSubsystem, cpuLoad5Perc, perc, labels, false)
					perc = math.Round(ls.Load15*100*100/float64(hm.CPU.CPUCount)) / 100
					updateResourceMetrics(cpuSubsystem, cpuLoad15Perc, perc, labels, false)
				}
			}
		}
		break // only one host expected
	}

	collectDriveMetrics(m)
}

func initLatestValues() {
	m := collectLocalMetrics(madmin.MetricsDisk, collectMetricsOpts{})
	latestDriveStatsMu.Lock()
	latestDriveStats = map[string]madmin.DiskIOStats{}
	for d, dm := range m.ByDisk {
		latestDriveStats[d] = dm.IOStats
	}
	lastDriveStatsRefresh = time.Now().UTC()
	latestDriveStatsMu.Unlock()
}

// startResourceMetricsCollection - starts the job for collecting resource metrics
func startResourceMetricsCollection() {
	initLatestValues()

	resourceMetricsMapMu.Lock()
	resourceMetricsMap = map[MetricSubsystem]ResourceMetrics{}
	resourceMetricsMapMu.Unlock()
	metricsTimer := time.NewTimer(resourceMetricsCollectionInterval)
	defer metricsTimer.Stop()

	collectLocalResourceMetrics()

	for {
		select {
		case <-GlobalContext.Done():
			return
		case <-metricsTimer.C:
			collectLocalResourceMetrics()

			// Reset the timer for next cycle.
			metricsTimer.Reset(resourceMetricsCollectionInterval)
		}
	}
}

// minioResourceCollector is the Collector for resource metrics
type minioResourceCollector struct {
	metricsGroups []*MetricsGroupV2
	desc          *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioResourceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioResourceCollector) Collect(out chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	publish := func(in <-chan MetricV2) {
		defer wg.Done()
		for metric := range in {
			labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
			collectMetric(metric, labels, values, "resource", out)
		}
	}

	// Call peer api to fetch metrics
	wg.Add(2)
	go publish(ReportMetrics(GlobalContext, c.metricsGroups))
	go publish(globalNotificationSys.GetResourceMetrics(GlobalContext))
	wg.Wait()
}

// newMinioResourceCollector describes the collector
// and returns reference of minio resource Collector
// It creates the Prometheus Description which is used
// to define Metric and  help string
func newMinioResourceCollector(metricsGroups []*MetricsGroupV2) *minioResourceCollector {
	return &minioResourceCollector{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_resource_stats", "Resource statistics exposed by MinIO server", nil, nil),
	}
}

func prepareResourceMetrics(rm ResourceMetric, subSys MetricSubsystem, requireAvgMax bool) []MetricV2 {
	help := resourceMetricsHelpMap[rm.Name]
	name := rm.Name
	metrics := make([]MetricV2, 0, 3)
	metrics = append(metrics, MetricV2{
		Description:    getResourceMetricDescription(subSys, name, help),
		Value:          rm.Current,
		VariableLabels: cloneMSS(rm.Labels),
	})

	if requireAvgMax {
		avgName := MetricName(fmt.Sprintf("%s_avg", name))
		avgHelp := fmt.Sprintf("%s (avg)", help)
		metrics = append(metrics, MetricV2{
			Description:    getResourceMetricDescription(subSys, avgName, avgHelp),
			Value:          math.Round(rm.Avg*100) / 100,
			VariableLabels: cloneMSS(rm.Labels),
		})

		maxName := MetricName(fmt.Sprintf("%s_max", name))
		maxHelp := fmt.Sprintf("%s (max)", help)
		metrics = append(metrics, MetricV2{
			Description:    getResourceMetricDescription(subSys, maxName, maxHelp),
			Value:          rm.Max,
			VariableLabels: cloneMSS(rm.Labels),
		})
	}

	return metrics
}

func getResourceMetricDescription(subSys MetricSubsystem, name MetricName, help string) MetricDescription {
	return MetricDescription{
		Namespace: nodeMetricNamespace,
		Subsystem: subSys,
		Name:      name,
		Help:      help,
		Type:      gaugeMetric,
	}
}

func getResourceMetrics() *MetricsGroupV2 {
	mg := &MetricsGroupV2{
		cacheInterval: resourceMetricsCacheInterval,
	}
	mg.RegisterRead(func(ctx context.Context) []MetricV2 {
		metrics := []MetricV2{}

		subSystems := []MetricSubsystem{interfaceSubsystem, memSubsystem, driveSubsystem, cpuSubsystem}
		resourceMetricsMapMu.RLock()
		defer resourceMetricsMapMu.RUnlock()
		for _, subSys := range subSystems {
			stats, found := resourceMetricsMap[subSys]
			if found {
				requireAvgMax := true
				if subSys == driveSubsystem {
					requireAvgMax = false
				}
				for _, m := range stats {
					metrics = append(metrics, prepareResourceMetrics(m, subSys, requireAvgMax)...)
				}
			}
		}

		return metrics
	})
	return mg
}

// metricsResourceHandler is the prometheus handler for resource metrics
func metricsResourceHandler() http.Handler {
	return metricsHTTPHandler(resourceCollector, "handler.MetricsResource")
}
