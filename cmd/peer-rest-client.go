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
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/http"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/rest"
	xnet "github.com/minio/pkg/net"
	"github.com/tinylib/msgp/msgp"
)

// client to talk to peer Nodes.
type peerRESTClient struct {
	host       *xnet.Host
	restClient *rest.Client
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(GlobalContext, method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// IsOnline returns true if the peer client is online.
func (client *peerRESTClient) IsOnline() bool {
	return client.restClient.IsOnline()
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// GetLocks - fetch older locks for a remote node.
func (client *peerRESTClient) GetLocks() (lockMap map[string][]lockRequesterInfo, err error) {
	respBody, err := client.call(peerRESTMethodGetLocks, nil, nil, -1)
	if err != nil {
		return
	}
	lockMap = map[string][]lockRequesterInfo{}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&lockMap)
	return lockMap, err
}

// ServerInfo - fetch server information for a remote node.
func (client *peerRESTClient) ServerInfo() (info madmin.ServerProperties, err error) {
	respBody, err := client.call(peerRESTMethodServerInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

type networkOverloadedErr struct{}

var networkOverloaded networkOverloadedErr

func (n networkOverloadedErr) Error() string {
	return "network overloaded"
}

type nullReader struct{}

func (r *nullReader) Read(b []byte) (int, error) {
	return len(b), nil
}

func (client *peerRESTClient) doNetTest(ctx context.Context, dataSize int64, threadCount uint) (info madmin.PeerNetPerfInfo, err error) {
	var mu sync.Mutex // mutex used to protect these slices in go-routines
	latencies := []float64{}
	throughputs := []float64{}

	buflimiter := make(chan struct{}, threadCount)
	errChan := make(chan error, threadCount)

	var totalTransferred int64

	// ensure enough samples to obtain normal distribution
	maxSamples := int(10 * threadCount)

	innerCtx, cancel := context.WithCancel(ctx)

	slowSamples := int32(0)
	maxSlowSamples := int32(maxSamples / 20)
	slowSample := func() {
		if slowSamples > maxSlowSamples { // 5% of total
			return
		}
		if atomic.AddInt32(&slowSamples, 1) >= maxSlowSamples {
			errChan <- networkOverloaded
			cancel()
		}
	}

	var wg sync.WaitGroup
	finish := func() {
		<-buflimiter
		wg.Done()
	}

	for i := 0; i < maxSamples; i++ {
		select {
		case <-ctx.Done():
			return info, ctx.Err()
		case err = <-errChan:
		case buflimiter <- struct{}{}:
			wg.Add(1)

			if innerCtx.Err() != nil {
				finish()
				continue
			}

			go func(i int) {
				start := time.Now()
				before := atomic.LoadInt64(&totalTransferred)

				ctx, cancel := context.WithTimeout(innerCtx, 10*time.Second)
				defer cancel()

				progress := io.LimitReader(&nullReader{}, dataSize)

				// Turn off healthCheckFn for health tests to cater for higher load on the peers.
				clnt := newPeerRESTClient(client.host)
				clnt.restClient.HealthCheckFn = nil

				respBody, err := clnt.callWithContext(ctx, peerRESTMethodNetInfo, nil, progress, dataSize)
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						slowSample()
						finish()
						return
					}

					errChan <- err
					finish()
					return
				}
				http.DrainBody(respBody)

				finish()
				atomic.AddInt64(&totalTransferred, dataSize)
				after := atomic.LoadInt64(&totalTransferred)
				end := time.Now()

				latency := end.Sub(start).Seconds()

				if latency > maxLatencyForSizeThreads(dataSize, threadCount) {
					slowSample()
				}

				/* Throughput = (total data transferred across all threads / time taken) */
				throughput := float64((after - before)) / latency

				// Protect updating latencies and throughputs slices from
				// multiple go-routines.
				mu.Lock()
				latencies = append(latencies, latency)
				throughputs = append(throughputs, throughput)
				mu.Unlock()
			}(i)
		}
	}
	wg.Wait()

	if err != nil {
		return info, err
	}

	latency, throughput, err := xnet.ComputePerfStats(latencies, throughputs)
	return madmin.PeerNetPerfInfo{
		Latency: madmin.Latency{
			Avg:          round(latency.Avg, 3),
			Max:          round(latency.Max, 3),
			Min:          round(latency.Min, 3),
			Percentile50: round(latency.Percentile50, 3),
			Percentile90: round(latency.Percentile90, 3),
			Percentile99: round(latency.Percentile99, 3),
		},
		Throughput: madmin.Throughput{
			Avg:          uint64(round(throughput.Avg, 0)),
			Max:          uint64(round(throughput.Max, 0)),
			Min:          uint64(round(throughput.Min, 0)),
			Percentile50: uint64(round(throughput.Percentile50, 0)),
			Percentile90: uint64(round(throughput.Percentile90, 0)),
			Percentile99: uint64(round(throughput.Percentile99, 0)),
		},
	}, nil
}

func maxLatencyForSizeThreads(size int64, threadCount uint) float64 {
	Gbit100 := 12.5 * float64(humanize.GiByte)
	Gbit40 := 5.00 * float64(humanize.GiByte)
	Gbit25 := 3.25 * float64(humanize.GiByte)
	Gbit10 := 1.25 * float64(humanize.GiByte)
	// Gbit1 := 0.25 * float64(humanize.GiByte)

	// Given the current defaults, each combination of size/thread
	// is supposed to fully saturate the intended pipe when all threads are active
	// i.e. if the test is performed in a perfectly controlled environment, i.e. without
	// CPU scheduling latencies and/or network jitters, then all threads working
	// simultaneously should result in each of them completing in 1s
	//
	// In reality, I've assumed a normal distribution of latency with expected mean of 1s and min of 0s
	// Then, 95% of threads should complete within 2 seconds (2 std. deviations from the mean). The 2s comes
	// from fitting the normal curve such that the mean is 1.
	//
	// i.e. we expect that no more than 5% of threads to take longer than 2s to push the data.
	//
	// throughput  |  max latency
	//   100 Gbit  |  2s
	//    40 Gbit  |  2s
	//    25 Gbit  |  2s
	//    10 Gbit  |  2s
	//     1 Gbit  |  inf

	throughput := float64(size * int64(threadCount))
	if throughput >= Gbit100 {
		return 2.0
	} else if throughput >= Gbit40 {
		return 2.0
	} else if throughput >= Gbit25 {
		return 2.0
	} else if throughput >= Gbit10 {
		return 2.0
	}
	return math.MaxFloat64
}

// GetNetPerfInfo - fetch network information for a remote node.
func (client *peerRESTClient) GetNetPerfInfo(ctx context.Context) (info madmin.PeerNetPerfInfo, err error) {

	// 100 Gbit ->  256 MiB  *  50 threads
	// 40 Gbit  ->  256 MiB  *  20 threads
	// 25 Gbit  ->  128 MiB  *  25 threads
	// 10 Gbit  ->  128 MiB  *  10 threads
	// 1 Gbit   ->  64  MiB  *  2  threads

	type step struct {
		size    int64
		threads uint
	}
	steps := []step{
		{ // 100 Gbit
			size:    256 * humanize.MiByte,
			threads: 50,
		},
		{ // 40 Gbit
			size:    256 * humanize.MiByte,
			threads: 20,
		},
		{ // 25 Gbit
			size:    128 * humanize.MiByte,
			threads: 25,
		},
		{ // 10 Gbit
			size:    128 * humanize.MiByte,
			threads: 10,
		},
		{ // 1 Gbit
			size:    64 * humanize.MiByte,
			threads: 2,
		},
	}

	for i := range steps {
		size := steps[i].size
		threads := steps[i].threads

		if info, err = client.doNetTest(ctx, size, threads); err != nil {
			if err == networkOverloaded {
				continue
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
		}
		return info, err
	}
	return info, err
}

// DispatchNetInfo - dispatch other nodes to run Net info.
func (client *peerRESTClient) DispatchNetInfo(ctx context.Context) (info madmin.NetPerfInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDispatchNetInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	waitReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return
	}
	err = gob.NewDecoder(waitReader).Decode(&info)
	return
}

// GetDrivePerfInfos - fetch all disk's serial/parallal performance information for a remote node.
func (client *peerRESTClient) GetDrivePerfInfos(ctx context.Context) (info madmin.DrivePerfInfos, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDriveInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetCPUs - fetch CPU information for a remote node.
func (client *peerRESTClient) GetCPUs(ctx context.Context) (info madmin.CPUs, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodCPUInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetPartitions - fetch disk partition information for a remote node.
func (client *peerRESTClient) GetPartitions(ctx context.Context) (info madmin.Partitions, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDiskHwInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetOSInfo - fetch OS information for a remote node.
func (client *peerRESTClient) GetOSInfo(ctx context.Context) (info madmin.OSInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodOsInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetMemInfo - fetch memory information for a remote node.
func (client *peerRESTClient) GetMemInfo(ctx context.Context) (info madmin.MemInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodMemInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetProcInfo - fetch MinIO process information for a remote node.
func (client *peerRESTClient) GetProcInfo(ctx context.Context) (info madmin.ProcInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodProcInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// StartProfiling - Issues profiling command on the peer node.
func (client *peerRESTClient) StartProfiling(profiler string) error {
	values := make(url.Values)
	values.Set(peerRESTProfiler, profiler)
	respBody, err := client.call(peerRESTMethodStartProfiling, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DownloadProfileData - download profiled data from a remote node.
func (client *peerRESTClient) DownloadProfileData() (data map[string][]byte, err error) {
	respBody, err := client.call(peerRESTMethodDownloadProfilingData, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&data)
	return data, err
}

// GetBucketStats - load bucket statistics
func (client *peerRESTClient) GetBucketStats(bucket string) (BucketStats, error) {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodGetBucketStats, values, nil, -1)
	if err != nil {
		return BucketStats{}, err
	}

	var bs BucketStats
	defer http.DrainBody(respBody)
	return bs, msgp.Decode(respBody, &bs)
}

// LoadBucketMetadata - load bucket metadata
func (client *peerRESTClient) LoadBucketMetadata(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodLoadBucketMetadata, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteBucketMetadata - Delete bucket metadata
func (client *peerRESTClient) DeleteBucketMetadata(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodDeleteBucketMetadata, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// cycleServerBloomFilter will cycle the bloom filter to start recording to index y if not already.
// The response will contain a bloom filter starting at index x up to, but not including index y.
// If y is 0, the response will not update y, but return the currently recorded information
// from the current x to y-1.
func (client *peerRESTClient) cycleServerBloomFilter(ctx context.Context, req bloomFilterRequest) (*bloomFilterResponse, error) {
	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(req)
	if err != nil {
		return nil, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodCycleBloom, nil, &reader, -1)
	if err != nil {
		return nil, err
	}
	var resp bloomFilterResponse
	defer http.DrainBody(respBody)
	return &resp, gob.NewDecoder(respBody).Decode(&resp)
}

// DeletePolicy - delete a specific canned policy.
func (client *peerRESTClient) DeletePolicy(policyName string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTPolicy, policyName)

	respBody, err := client.call(peerRESTMethodDeletePolicy, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadPolicy - reload a specific canned policy.
func (client *peerRESTClient) LoadPolicy(policyName string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTPolicy, policyName)

	respBody, err := client.call(peerRESTMethodLoadPolicy, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadPolicyMapping - reload a specific policy mapping
func (client *peerRESTClient) LoadPolicyMapping(userOrGroup string, isGroup bool) error {
	values := make(url.Values)
	values.Set(peerRESTUserOrGroup, userOrGroup)
	if isGroup {
		values.Set(peerRESTIsGroup, "")
	}

	respBody, err := client.call(peerRESTMethodLoadPolicyMapping, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteUser - delete a specific user.
func (client *peerRESTClient) DeleteUser(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodDeleteUser, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteServiceAccount - delete a specific service account.
func (client *peerRESTClient) DeleteServiceAccount(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodDeleteServiceAccount, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadUser - reload a specific user.
func (client *peerRESTClient) LoadUser(accessKey string, temp bool) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)
	values.Set(peerRESTUserTemp, strconv.FormatBool(temp))

	respBody, err := client.call(peerRESTMethodLoadUser, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadServiceAccount - reload a specific service account.
func (client *peerRESTClient) LoadServiceAccount(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodLoadServiceAccount, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadGroup - send load group command to peers.
func (client *peerRESTClient) LoadGroup(group string) error {
	values := make(url.Values)
	values.Set(peerRESTGroup, group)
	respBody, err := client.call(peerRESTMethodLoadGroup, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

type serverUpdateInfo struct {
	URL         *url.URL
	Sha256Sum   []byte
	Time        time.Time
	ReleaseInfo string
}

// ServerUpdate - sends server update message to remote peers.
func (client *peerRESTClient) ServerUpdate(ctx context.Context, u *url.URL, sha256Sum []byte, lrTime time.Time, releaseInfo string) error {
	values := make(url.Values)
	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(serverUpdateInfo{
		URL:         u,
		Sha256Sum:   sha256Sum,
		Time:        lrTime,
		ReleaseInfo: releaseInfo,
	}); err != nil {
		return err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodServerUpdate, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// SignalService - sends signal to peer nodes.
func (client *peerRESTClient) SignalService(sig serviceSignal) error {
	values := make(url.Values)
	values.Set(peerRESTSignal, strconv.Itoa(int(sig)))
	respBody, err := client.call(peerRESTMethodSignalService, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) BackgroundHealStatus() (madmin.BgHealState, error) {
	respBody, err := client.call(peerRESTMethodBackgroundHealStatus, nil, nil, -1)
	if err != nil {
		return madmin.BgHealState{}, err
	}
	defer http.DrainBody(respBody)

	state := madmin.BgHealState{}
	err = gob.NewDecoder(respBody).Decode(&state)
	return state, err
}

// GetLocalDiskIDs - get a peer's local disks' IDs.
func (client *peerRESTClient) GetLocalDiskIDs(ctx context.Context) (diskIDs []string) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetLocalDiskIDs, nil, nil, -1)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil
	}
	defer http.DrainBody(respBody)
	if err = gob.NewDecoder(respBody).Decode(&diskIDs); err != nil {
		logger.LogIf(ctx, err)
		return nil
	}
	return diskIDs
}

// GetMetacacheListing - get a new or existing metacache.
func (client *peerRESTClient) GetMetacacheListing(ctx context.Context, o listPathOptions) (*metacache, error) {
	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(o)
	if err != nil {
		return nil, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetMetacacheListing, nil, &reader, int64(reader.Len()))
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	var resp metacache
	defer http.DrainBody(respBody)
	return &resp, msgp.Decode(respBody, &resp)
}

// UpdateMetacacheListing - update an existing metacache it will unconditionally be updated to the new state.
func (client *peerRESTClient) UpdateMetacacheListing(ctx context.Context, m metacache) (metacache, error) {
	b, err := m.MarshalMsg(nil)
	if err != nil {
		return m, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodUpdateMetacacheListing, nil, bytes.NewBuffer(b), int64(len(b)))
	if err != nil {
		logger.LogIf(ctx, err)
		return m, err
	}
	defer http.DrainBody(respBody)
	var resp metacache
	return resp, msgp.Decode(respBody, &resp)

}

func (client *peerRESTClient) LoadTransitionTierConfig(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodLoadTransitionTierConfig, nil, nil, 0)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) doTrace(traceCh chan interface{}, doneCh <-chan struct{}, traceOpts madmin.ServiceTraceOpts) {
	values := make(url.Values)
	values.Set(peerRESTTraceErr, strconv.FormatBool(traceOpts.OnlyErrors))
	values.Set(peerRESTTraceS3, strconv.FormatBool(traceOpts.S3))
	values.Set(peerRESTTraceStorage, strconv.FormatBool(traceOpts.Storage))
	values.Set(peerRESTTraceOS, strconv.FormatBool(traceOpts.OS))
	values.Set(peerRESTTraceInternal, strconv.FormatBool(traceOpts.Internal))
	values.Set(peerRESTTraceThreshold, traceOpts.Threshold.String())

	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(GlobalContext)

	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		select {
		case <-doneCh:
		case <-cancelCh:
			// There was an error in the REST request.
		}
		cancel()
	}()

	respBody, err := client.callWithContext(ctx, peerRESTMethodTrace, values, nil, -1)
	defer http.DrainBody(respBody)

	if err != nil {
		return
	}

	dec := gob.NewDecoder(respBody)
	for {
		var info madmin.TraceInfo
		if err = dec.Decode(&info); err != nil {
			return
		}
		if len(info.NodeName) > 0 {
			select {
			case traceCh <- info:
			default:
				// Do not block on slow receivers.
			}
		}
	}
}

func (client *peerRESTClient) doListen(listenCh chan interface{}, doneCh <-chan struct{}, v url.Values) {
	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(GlobalContext)

	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		select {
		case <-doneCh:
		case <-cancelCh:
			// There was an error in the REST request.
		}
		cancel()
	}()

	respBody, err := client.callWithContext(ctx, peerRESTMethodListen, v, nil, -1)
	defer http.DrainBody(respBody)

	if err != nil {
		return
	}

	dec := gob.NewDecoder(respBody)
	for {
		var ev event.Event
		if err := dec.Decode(&ev); err != nil {
			return
		}
		if len(ev.EventVersion) > 0 {
			select {
			case listenCh <- ev:
			default:
				// Do not block on slow receivers.
			}
		}
	}
}

// Listen - listen on peers.
func (client *peerRESTClient) Listen(listenCh chan interface{}, doneCh <-chan struct{}, v url.Values) {
	go func() {
		for {
			client.doListen(listenCh, doneCh, v)
			select {
			case <-doneCh:
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

// Trace - send http trace request to peer nodes
func (client *peerRESTClient) Trace(traceCh chan interface{}, doneCh <-chan struct{}, traceOpts madmin.ServiceTraceOpts) {
	go func() {
		for {
			client.doTrace(traceCh, doneCh, traceOpts)
			select {
			case <-doneCh:
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

// ConsoleLog - sends request to peer nodes to get console logs
func (client *peerRESTClient) ConsoleLog(logCh chan interface{}, doneCh <-chan struct{}) {
	go func() {
		for {
			// get cancellation context to properly unsubscribe peers
			ctx, cancel := context.WithCancel(GlobalContext)
			respBody, err := client.callWithContext(ctx, peerRESTMethodLog, nil, nil, -1)
			if err != nil {
				// Retry the failed request.
				time.Sleep(5 * time.Second)
			} else {
				dec := gob.NewDecoder(respBody)

				go func() {
					<-doneCh
					cancel()
				}()

				for {
					var log madmin.LogInfo
					if err = dec.Decode(&log); err != nil {
						break
					}
					select {
					case logCh <- log:
					default:
					}
				}
			}

			select {
			case <-doneCh:
				cancel()
				http.DrainBody(respBody)
				return
			default:
				// There was error in the REST request, retry.
			}
		}
	}()
}

// newPeerRestClients creates new peer clients.
// The two slices will point to the same clients,
// but 'all' will contain nil entry for local client.
// The 'all' slice will be in the same order across the cluster.
func newPeerRestClients(endpoints EndpointServerPools) (remote, all []*peerRESTClient) {
	if !globalIsDistErasure {
		// Only useful in distributed setups
		return nil, nil
	}
	hosts := endpoints.hostsSorted()
	remote = make([]*peerRESTClient, 0, len(hosts))
	all = make([]*peerRESTClient, len(hosts))
	for i, host := range hosts {
		if host == nil {
			continue
		}
		all[i] = newPeerRESTClient(host)
		remote = append(remote, all[i])
	}
	if len(all) != len(remote)+1 {
		logger.LogIf(context.Background(), fmt.Errorf("WARNING: Expected number of all hosts (%v) to be remote +1 (%v)", len(all), len(remote)))
	}
	return remote, all
}

// Returns a peer rest client.
func newPeerRESTClient(peer *xnet.Host) *peerRESTClient {
	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   peerRESTPath,
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newAuthToken)
	// Use a separate client to avoid recursive calls.
	healthClient := rest.NewClient(serverURL, globalInternodeTransport, newAuthToken)
	healthClient.ExpectTimeouts = true
	healthClient.NoMetrics = true

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, peerRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}

	return &peerRESTClient{host: peer, restClient: restClient}
}

// MonitorBandwidth - send http trace request to peer nodes
func (client *peerRESTClient) MonitorBandwidth(ctx context.Context, buckets []string) (*madmin.BucketBandwidthReport, error) {
	values := make(url.Values)
	values.Set(peerRESTBuckets, strings.Join(buckets, ","))
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetBandwidth, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)

	dec := gob.NewDecoder(respBody)
	var bandwidthReport madmin.BucketBandwidthReport
	err = dec.Decode(&bandwidthReport)
	return &bandwidthReport, err
}

func (client *peerRESTClient) GetPeerMetrics(ctx context.Context) (<-chan Metric, error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetPeerMetrics, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(respBody)
	ch := make(chan Metric)
	go func(ch chan<- Metric) {
		for {
			var metric Metric
			if err := dec.Decode(&metric); err != nil {
				http.DrainBody(respBody)
				close(ch)
				return
			}
			ch <- metric
		}
	}(ch)
	return ch, nil
}
