package cmd

import (
	"strings"
	"sync/atomic"
	"time"
)

//go:generate stringer -type=scannerMetric -trimprefix=scannerMetric $GOFILE

type scannerMetric uint8

type scannerMetricCollecter struct {
	operations [scannerMetricLastTrace]uint64
	latency    [scannerMetricLastRealtime]lockedLastMinuteLatency
}

var globalScannerMetrics = &scannerMetricCollecter{}

const (
	scannerMetricReadDir scannerMetric = iota
	scannerMetricReadXL
	scannerMetricCheckMissing
	scannerMetricSaveUsage

	// END realtime metrics:
	scannerMetricLastRealtime

	// START Trace only metrics:
	scannerMetricScanCycle       // Full cycle, cluster global
	scannerMetricScanBucketCycle // Singe bucket, full cluster scan.
	scannerMetricScanBucketDisk  // Single bucket on one disk
	scannerMetricScanFolder      // Scan a folder on disk, recursively.

	// Must be last:
	scannerMetricLastTrace
)

// Update storage metrics
func (p *scannerMetricCollecter) log(s scannerMetric, paths ...string) func() {
	startTime := time.Now()
	return func() {
		duration := time.Since(startTime)

		atomic.AddUint64(&p.operations[s], 1)
		if s < scannerMetricLastRealtime {
			p.latency[s].add(duration)
		}

		if globalTrace.NumSubscribers() > 0 {
			globalTrace.Publish(scannerTrace(s, startTime, duration, strings.Join(paths, " ")))
		}
	}
}
