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
	"net/url"
	"runtime"
	"sort"
	"time"

	"github.com/minio/dperf/pkg/dperf"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	xioutil "github.com/minio/minio/internal/ioutil"
)

const speedTest = "speedtest"

type speedTestOpts struct {
	objectSize       int
	concurrencyStart int
	concurrency      int
	duration         time.Duration
	autotune         bool
	storageClass     string
	bucketName       string
	enableSha256     bool
	enableMultipart  bool
	creds            auth.Credentials
}

// Get the max throughput and iops numbers.
func objectSpeedTest(ctx context.Context, opts speedTestOpts) chan madmin.SpeedTestResult {
	ch := make(chan madmin.SpeedTestResult, 1)
	go func() {
		defer xioutil.SafeClose(ch)

		concurrency := opts.concurrencyStart

		if opts.autotune {
			// if we have less drives than concurrency then choose
			// only the concurrency to be number of drives to start
			// with - since default '32' might be big and may not
			// complete in total time of 10s.
			if globalEndpoints.NEndpoints() < concurrency {
				concurrency = globalEndpoints.NEndpoints()
			}

			// Check if we have local disks per pool less than
			// the concurrency make sure we choose only the "start"
			// concurrency to be equal to the lowest number of
			// local disks per server.
			for _, localDiskCount := range globalEndpoints.NLocalDisksPathsPerPool() {
				if localDiskCount < concurrency {
					concurrency = localDiskCount
				}
			}

			// Any concurrency less than '4' just stick to '4' concurrent
			// operations for now to begin with.
			if concurrency < 4 {
				concurrency = 4
			}

			// if GOMAXPROCS is set to a lower value then choose to use
			// concurrency == GOMAXPROCS instead.
			if runtime.GOMAXPROCS(0) < concurrency {
				concurrency = runtime.GOMAXPROCS(0)
			}
		}

		throughputHighestGet := uint64(0)
		throughputHighestPut := uint64(0)
		var throughputHighestResults []SpeedTestResult

		sendResult := func() {
			var result madmin.SpeedTestResult

			durationSecs := opts.duration.Seconds()

			result.GETStats.ThroughputPerSec = throughputHighestGet / uint64(durationSecs)
			result.GETStats.ObjectsPerSec = throughputHighestGet / uint64(opts.objectSize) / uint64(durationSecs)
			result.PUTStats.ThroughputPerSec = throughputHighestPut / uint64(durationSecs)
			result.PUTStats.ObjectsPerSec = throughputHighestPut / uint64(opts.objectSize) / uint64(durationSecs)
			var totalUploadTimes madmin.TimeDurations
			var totalDownloadTimes madmin.TimeDurations
			var totalDownloadTTFB madmin.TimeDurations
			for i := range len(throughputHighestResults) {
				errStr := ""
				if throughputHighestResults[i].Error != "" {
					errStr = throughputHighestResults[i].Error
				}

				// if the default concurrency yields zero results, throw an error.
				if throughputHighestResults[i].Downloads == 0 && opts.concurrencyStart == concurrency {
					errStr = fmt.Sprintf("no results for downloads upon first attempt, concurrency %d and duration %s",
						opts.concurrencyStart, opts.duration)
				}

				// if the default concurrency yields zero results, throw an error.
				if throughputHighestResults[i].Uploads == 0 && opts.concurrencyStart == concurrency {
					errStr = fmt.Sprintf("no results for uploads upon first attempt, concurrency %d and duration %s",
						opts.concurrencyStart, opts.duration)
				}

				result.PUTStats.Servers = append(result.PUTStats.Servers, madmin.SpeedTestStatServer{
					Endpoint:         throughputHighestResults[i].Endpoint,
					ThroughputPerSec: throughputHighestResults[i].Uploads / uint64(durationSecs),
					ObjectsPerSec:    throughputHighestResults[i].Uploads / uint64(opts.objectSize) / uint64(durationSecs),
					Err:              errStr,
				})

				result.GETStats.Servers = append(result.GETStats.Servers, madmin.SpeedTestStatServer{
					Endpoint:         throughputHighestResults[i].Endpoint,
					ThroughputPerSec: throughputHighestResults[i].Downloads / uint64(durationSecs),
					ObjectsPerSec:    throughputHighestResults[i].Downloads / uint64(opts.objectSize) / uint64(durationSecs),
					Err:              errStr,
				})

				totalUploadTimes = append(totalUploadTimes, throughputHighestResults[i].UploadTimes...)
				totalDownloadTimes = append(totalDownloadTimes, throughputHighestResults[i].DownloadTimes...)
				totalDownloadTTFB = append(totalDownloadTTFB, throughputHighestResults[i].DownloadTTFB...)
			}

			result.PUTStats.Response = totalUploadTimes.Measure()
			result.GETStats.Response = totalDownloadTimes.Measure()
			result.GETStats.TTFB = totalDownloadTTFB.Measure()

			result.Size = opts.objectSize
			result.Disks = globalEndpoints.NEndpoints()
			result.Servers = len(globalNotificationSys.peerClients) + 1
			result.Version = Version
			result.Concurrent = concurrency

			select {
			case ch <- result:
			case <-ctx.Done():
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				// If the client got disconnected stop the speedtest.
				return
			default:
			}

			sopts := speedTestOpts{
				objectSize:      opts.objectSize,
				concurrency:     concurrency,
				duration:        opts.duration,
				storageClass:    opts.storageClass,
				bucketName:      opts.bucketName,
				enableSha256:    opts.enableSha256,
				enableMultipart: opts.enableMultipart,
				creds:           opts.creds,
			}

			results := globalNotificationSys.SpeedTest(ctx, sopts)
			sort.Slice(results, func(i, j int) bool {
				return results[i].Endpoint < results[j].Endpoint
			})

			totalPut := uint64(0)
			totalGet := uint64(0)
			for _, result := range results {
				totalPut += result.Uploads
				totalGet += result.Downloads
			}

			if totalGet < throughputHighestGet {
				// Following check is for situations
				// when Writes() scale higher than Reads()
				// - practically speaking this never happens
				// and should never happen - however it has
				// been seen recently due to hardware issues
				// causes Reads() to go slower than Writes().
				//
				// Send such results anyways as this shall
				// expose a problem underneath.
				if totalPut > throughputHighestPut {
					throughputHighestResults = results
					throughputHighestPut = totalPut
					// let the client see lower value as well
					throughputHighestGet = totalGet
				}
				sendResult()
				break
			}

			// We break if we did not see 2.5% growth rate in total GET
			// requests, we have reached our peak at this point.
			doBreak := float64(totalGet-throughputHighestGet)/float64(totalGet) < 0.025

			throughputHighestGet = totalGet
			throughputHighestResults = results
			throughputHighestPut = totalPut

			if doBreak {
				sendResult()
				break
			}

			for _, result := range results {
				if result.Error != "" {
					// Break out on errors.
					sendResult()
					return
				}
			}

			sendResult()
			if !opts.autotune {
				break
			}

			// Try with a higher concurrency to see if we get better throughput
			concurrency += (concurrency + 1) / 2
		}
	}()
	return ch
}

func driveSpeedTest(ctx context.Context, opts madmin.DriveSpeedTestOpts) madmin.DriveSpeedTestResult {
	perf := &dperf.DrivePerf{
		Serial:    opts.Serial,
		BlockSize: opts.BlockSize,
		FileSize:  opts.FileSize,
	}

	localPaths := globalEndpoints.LocalDisksPaths()
	var ignoredPaths []string
	paths := func() (tmpPaths []string) {
		for _, lp := range localPaths {
			if _, err := Lstat(pathJoin(lp, minioMetaBucket, formatConfigFile)); err == nil {
				tmpPaths = append(tmpPaths, pathJoin(lp, minioMetaTmpBucket))
			} else {
				// Use dperf on only formatted drives.
				ignoredPaths = append(ignoredPaths, lp)
			}
		}
		return tmpPaths
	}()

	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	u := &url.URL{
		Scheme: scheme,
		Host:   globalLocalNodeName,
	}

	perfs, err := perf.Run(ctx, paths...)
	return madmin.DriveSpeedTestResult{
		Endpoint: u.String(),
		Version:  Version,
		DrivePerf: func() (results []madmin.DrivePerf) {
			for idx, r := range perfs {
				result := madmin.DrivePerf{
					Path:            localPaths[idx],
					ReadThroughput:  r.ReadThroughput,
					WriteThroughput: r.WriteThroughput,
					Error: func() string {
						if r.Error != nil {
							return r.Error.Error()
						}
						return ""
					}(),
				}
				results = append(results, result)
			}
			for _, inp := range ignoredPaths {
				results = append(results, madmin.DrivePerf{
					Path:  inp,
					Error: errFaultyDisk.Error(),
				})
			}
			return results
		}(),
		Error: func() string {
			if err != nil {
				return err.Error()
			}
			return ""
		}(),
	}
}
