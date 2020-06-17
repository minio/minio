/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/http"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	trace "github.com/minio/minio/pkg/trace"
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

	respBody, err = client.restClient.CallWithContext(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// GetLocksResp stores various info from the client for each lock that is requested.
type GetLocksResp []map[string][]lockRequesterInfo

// GetLocks - fetch older locks for a remote node.
func (client *peerRESTClient) GetLocks() (locks GetLocksResp, err error) {
	respBody, err := client.call(peerRESTMethodGetLocks, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&locks)
	return locks, err
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

type progressReader struct {
	r            io.Reader
	progressChan chan int64
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if err != nil && err != io.EOF {
		return n, err
	}
	p.progressChan <- int64(n)
	return n, err
}

func (client *peerRESTClient) doNetOBDTest(ctx context.Context, dataSize int64, threadCount uint) (info madmin.NetOBDInfo, err error) {
	latencies := []float64{}
	throughputs := []float64{}

	buf := make([]byte, dataSize)

	buflimiter := make(chan struct{}, threadCount)
	errChan := make(chan error, threadCount)

	totalTransferred := int64(0)
	transferChan := make(chan int64, threadCount)
	go func() {
		for v := range transferChan {
			atomic.AddInt64(&totalTransferred, v)
		}
	}()

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

	wg := sync.WaitGroup{}
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
				bufReader := bytes.NewReader(buf)
				bufReadCloser := ioutil.NopCloser(&progressReader{
					r:            bufReader,
					progressChan: transferChan,
				})
				start := time.Now()
				before := atomic.LoadInt64(&totalTransferred)

				ctx, cancel := context.WithTimeout(innerCtx, 10*time.Second)
				defer cancel()
				respBody, err := client.callWithContext(ctx, peerRESTMethodNetOBDInfo, nil, bufReadCloser, dataSize)
				if err != nil {

					if netErr, ok := err.(*rest.NetworkError); ok {
						if urlErr, ok := netErr.Err.(*url.Error); ok {
							if urlErr.Err.Error() == context.DeadlineExceeded.Error() {
								slowSample()
								finish()
								return
							}
						}
					}

					errChan <- err
					finish()
					return
				}
				http.DrainBody(respBody)

				after := atomic.LoadInt64(&totalTransferred)
				finish()
				end := time.Now()

				latency := float64(end.Sub(start).Seconds())

				if latency > maxLatencyForSizeThreads(dataSize, threadCount) {
					slowSample()
				}

				/* Throughput = (total data transferred across all threads / time taken) */
				throughput := float64(float64((after - before)) / latency)

				latencies = append(latencies, latency)
				throughputs = append(throughputs, throughput)
			}(i)
		}
	}
	wg.Wait()

	if err != nil {
		return info, err
	}

	latency, throughput, err := xnet.ComputeOBDStats(latencies, throughputs)
	info = madmin.NetOBDInfo{
		Latency:    latency,
		Throughput: throughput,
	}
	return info, err

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

	throughput := float64(int64(size) * int64(threadCount))
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

// NetOBDInfo - fetch Net OBD information for a remote node.
func (client *peerRESTClient) NetOBDInfo(ctx context.Context) (info madmin.NetOBDInfo, err error) {

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

		if info, err = client.doNetOBDTest(ctx, size, threads); err != nil {
			if err == networkOverloaded {
				continue
			}

			if netErr, ok := err.(*rest.NetworkError); ok {
				if urlErr, ok := netErr.Err.(*url.Error); ok {
					if urlErr.Err.Error() == context.Canceled.Error() {
						continue
					}
					if urlErr.Err.Error() == context.DeadlineExceeded.Error() {
						continue
					}
				}
			}
		}
		return info, err
	}
	return info, err
}

// DispatchNetOBDInfo - dispatch other nodes to run Net OBD.
func (client *peerRESTClient) DispatchNetOBDInfo(ctx context.Context) (info madmin.ServerNetOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDispatchNetOBDInfo, nil, nil, -1)
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

// DriveOBDInfo - fetch Drive OBD information for a remote node.
func (client *peerRESTClient) DriveOBDInfo(ctx context.Context) (info madmin.ServerDrivesOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDriveOBDInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// CPUOBDInfo - fetch CPU OBD information for a remote node.
func (client *peerRESTClient) CPUOBDInfo(ctx context.Context) (info madmin.ServerCPUOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodCPUOBDInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// DiskHwOBDInfo - fetch Disk HW OBD information for a remote node.
func (client *peerRESTClient) DiskHwOBDInfo(ctx context.Context) (info madmin.ServerDiskHwOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDiskHwOBDInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// OsOBDInfo - fetch OsInfo OBD information for a remote node.
func (client *peerRESTClient) OsOBDInfo(ctx context.Context) (info madmin.ServerOsOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodOsInfoOBDInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// MemOBDInfo - fetch MemInfo OBD information for a remote node.
func (client *peerRESTClient) MemOBDInfo(ctx context.Context) (info madmin.ServerMemOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodMemOBDInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// ProcOBDInfo - fetch ProcInfo OBD information for a remote node.
func (client *peerRESTClient) ProcOBDInfo(ctx context.Context) (info madmin.ServerProcOBDInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodProcOBDInfo, nil, nil, -1)
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

// ReloadFormat - reload format on the peer node.
func (client *peerRESTClient) ReloadFormat(dryRun bool) error {
	values := make(url.Values)
	if dryRun {
		values.Set(peerRESTDryRun, "true")
	} else {
		values.Set(peerRESTDryRun, "false")
	}

	respBody, err := client.call(peerRESTMethodReloadFormat, values, nil, -1)
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

// ServerUpdate - sends server update message to remote peers.
func (client *peerRESTClient) ServerUpdate(updateURL, sha256Hex string, latestReleaseTime time.Time) error {
	values := make(url.Values)
	values.Set(peerRESTUpdateURL, updateURL)
	values.Set(peerRESTSha256Hex, sha256Hex)
	if !latestReleaseTime.IsZero() {
		values.Set(peerRESTLatestRelease, latestReleaseTime.Format(time.RFC3339))
	} else {
		values.Set(peerRESTLatestRelease, "")
	}
	respBody, err := client.call(peerRESTMethodServerUpdate, values, nil, -1)
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

func (client *peerRESTClient) doTrace(traceCh chan interface{}, doneCh <-chan struct{}, trcAll, trcErr bool) {
	values := make(url.Values)
	values.Set(peerRESTTraceAll, strconv.FormatBool(trcAll))
	values.Set(peerRESTTraceErr, strconv.FormatBool(trcErr))

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
		var info trace.Info
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
		if err = dec.Decode(&ev); err != nil {
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
func (client *peerRESTClient) Trace(traceCh chan interface{}, doneCh <-chan struct{}, trcAll, trcErr bool) {
	go func() {
		for {
			client.doTrace(traceCh, doneCh, trcAll, trcErr)
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

func getRemoteHosts(endpointZones EndpointZones) []*xnet.Host {
	var remoteHosts []*xnet.Host
	for _, hostStr := range GetRemotePeers(endpointZones) {
		host, err := xnet.ParseHost(hostStr)
		if err != nil {
			logger.LogIf(GlobalContext, err)
			continue
		}
		remoteHosts = append(remoteHosts, host)
	}

	return remoteHosts
}

// newPeerRestClients creates new peer clients.
func newPeerRestClients(endpoints EndpointZones) []*peerRESTClient {
	peerHosts := getRemoteHosts(endpoints)
	restClients := make([]*peerRESTClient, len(peerHosts))
	for i, host := range peerHosts {
		client, err := newPeerRESTClient(host)
		if err != nil {
			logger.LogIf(GlobalContext, err)
			continue
		}
		restClients[i] = client
	}

	return restClients
}

// Returns a peer rest client.
func newPeerRESTClient(peer *xnet.Host) (*peerRESTClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   peerRESTPath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: peer.Name,
			RootCAs:    globalRootCAs,
		}
	}

	trFn := newCustomHTTPTransport(tlsConfig, rest.DefaultRESTTimeout)
	restClient, err := rest.NewClient(serverURL, trFn, newAuthToken)
	if err != nil {
		return nil, err
	}

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(GlobalContext, restClient.HealthCheckTimeout)
		respBody, err := restClient.CallWithContext(ctx, peerRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		cancel()
		var ne *rest.NetworkError
		return !errors.Is(err, context.DeadlineExceeded) && !errors.As(err, &ne)
	}

	return &peerRESTClient{host: peer, restClient: restClient}, nil
}
