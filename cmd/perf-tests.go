// Copyright (c) 2022 MinIO, Inc.
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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/randreader"
)

// SpeedTestResult return value of the speedtest function
type SpeedTestResult struct {
	Endpoint      string
	Uploads       uint64
	Downloads     uint64
	UploadTimes   madmin.TimeDurations
	DownloadTimes madmin.TimeDurations
	DownloadTTFB  madmin.TimeDurations
	Error         string
}

func newRandomReader(size int) io.Reader {
	return io.LimitReader(randreader.New(), int64(size))
}

type firstByteRecorder struct {
	t *time.Time
	r io.Reader
}

func (f *firstByteRecorder) Read(p []byte) (n int, err error) {
	if f.t != nil || len(p) == 0 {
		return f.r.Read(p)
	}
	// Read a single byte.
	n, err = f.r.Read(p[:1])
	if n > 0 {
		t := time.Now()
		f.t = &t
	}
	return n, err
}

// Runs the speedtest on local MinIO process.
func selfSpeedTest(ctx context.Context, opts speedTestOpts) (res SpeedTestResult, err error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return SpeedTestResult{}, errServerNotInitialized
	}

	var wg sync.WaitGroup
	var errOnce sync.Once
	var retError string
	var totalBytesWritten uint64
	var totalBytesRead uint64

	objCountPerThread := make([]uint64, opts.concurrency)

	uploadsCtx, uploadsCancel := context.WithTimeout(ctx, opts.duration)
	defer uploadsCancel()

	objNamePrefix := pathJoin(speedTest, mustGetUUID())

	userMetadata := make(map[string]string)
	userMetadata[globalObjectPerfUserMetadata] = "true" // Bypass S3 API freeze
	popts := minio.PutObjectOptions{
		UserMetadata:         userMetadata,
		DisableContentSha256: !opts.enableSha256,
		DisableMultipart:     !opts.enableMultipart,
	}

	clnt := globalMinioClient
	if !globalAPIConfig.permitRootAccess() {
		region := globalSite.Region()
		if region == "" {
			region = "us-east-1"
		}
		clnt, err = minio.New(globalLocalNodeName, &minio.Options{
			Creds:     credentials.NewStaticV4(opts.creds.AccessKey, opts.creds.SecretKey, opts.creds.SessionToken),
			Secure:    globalIsTLS,
			Transport: globalRemoteTargetTransport,
			Region:    region,
		})
		if err != nil {
			return res, err
		}
	}

	var mu sync.Mutex
	var uploadTimes madmin.TimeDurations
	wg.Add(opts.concurrency)
	for i := 0; i < opts.concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				t := time.Now()
				reader := newRandomReader(opts.objectSize)
				tmpObjName := pathJoin(objNamePrefix, fmt.Sprintf("%d/%d", i, objCountPerThread[i]))
				info, err := clnt.PutObject(uploadsCtx, opts.bucketName, tmpObjName, reader, int64(opts.objectSize), popts)
				if err != nil {
					if !contextCanceled(uploadsCtx) && !errors.Is(err, context.Canceled) {
						errOnce.Do(func() {
							retError = err.Error()
						})
					}
					uploadsCancel()
					return
				}
				response := time.Since(t)
				atomic.AddUint64(&totalBytesWritten, uint64(info.Size))
				objCountPerThread[i]++
				mu.Lock()
				uploadTimes = append(uploadTimes, response)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// We already saw write failures, no need to proceed into read's
	if retError != "" {
		return SpeedTestResult{
			Uploads:     totalBytesWritten,
			Downloads:   totalBytesRead,
			UploadTimes: uploadTimes,
			Error:       retError,
		}, nil
	}

	downloadsCtx, downloadsCancel := context.WithTimeout(ctx, opts.duration)
	defer downloadsCancel()

	gopts := minio.GetObjectOptions{}
	gopts.Set(globalObjectPerfUserMetadata, "true") // Bypass S3 API freeze

	var downloadTimes madmin.TimeDurations
	var downloadTTFB madmin.TimeDurations
	wg.Add(opts.concurrency)

	c := minio.Core{Client: clnt}
	for i := 0; i < opts.concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			var j uint64
			if objCountPerThread[i] == 0 {
				return
			}
			for {
				if objCountPerThread[i] == j {
					j = 0
				}
				tmpObjName := pathJoin(objNamePrefix, fmt.Sprintf("%d/%d", i, j))
				t := time.Now()

				r, _, _, err := c.GetObject(downloadsCtx, opts.bucketName, tmpObjName, gopts)
				if err != nil {
					errResp, ok := err.(minio.ErrorResponse)
					if ok && errResp.StatusCode == http.StatusNotFound {
						continue
					}
					if !contextCanceled(downloadsCtx) && !errors.Is(err, context.Canceled) {
						errOnce.Do(func() {
							retError = err.Error()
						})
					}
					downloadsCancel()
					return
				}
				fbr := firstByteRecorder{
					r: r,
				}
				n, err := xioutil.Copy(xioutil.Discard, &fbr)
				r.Close()
				if err == nil {
					response := time.Since(t)
					ttfb := time.Since(*fbr.t)
					// Only capture success criteria - do not
					// have to capture failed reads, truncated
					// reads etc.
					atomic.AddUint64(&totalBytesRead, uint64(n))
					mu.Lock()
					downloadTimes = append(downloadTimes, response)
					downloadTTFB = append(downloadTTFB, ttfb)
					mu.Unlock()
				}
				if err != nil {
					if !contextCanceled(downloadsCtx) && !errors.Is(err, context.Canceled) {
						errOnce.Do(func() {
							retError = err.Error()
						})
					}
					downloadsCancel()
					return
				}
				j++
			}
		}(i)
	}
	wg.Wait()

	return SpeedTestResult{
		Uploads:       totalBytesWritten,
		Downloads:     totalBytesRead,
		UploadTimes:   uploadTimes,
		DownloadTimes: downloadTimes,
		DownloadTTFB:  downloadTTFB,
		Error:         retError,
	}, nil
}

// To collect RX stats during "mc support perf net"
// RXSample holds the RX bytes for the duration between
// the last peer to connect and the first peer to disconnect.
// This is to improve the RX throughput accuracy.
type netPerfRX struct {
	RX                uint64    // RX bytes
	lastToConnect     time.Time // time at which last peer to connect to us
	firstToDisconnect time.Time // time at which the first peer disconnects from us
	RXSample          uint64    // RX bytes between lastToConnect and firstToDisconnect
	activeConnections uint64
	sync.RWMutex
}

func (n *netPerfRX) Connect() {
	n.Lock()
	defer n.Unlock()
	n.activeConnections++
	atomic.StoreUint64(&n.RX, 0)
	n.lastToConnect = time.Now()
}

func (n *netPerfRX) Disconnect() {
	n.Lock()
	defer n.Unlock()
	n.activeConnections--
	if n.firstToDisconnect.IsZero() {
		n.RXSample = atomic.LoadUint64(&n.RX)
		n.firstToDisconnect = time.Now()
	}
}

func (n *netPerfRX) ActiveConnections() uint64 {
	n.RLock()
	defer n.RUnlock()
	return n.activeConnections
}

func (n *netPerfRX) Reset() {
	n.Lock()
	defer n.Unlock()
	n.RX = 0
	n.RXSample = 0
	n.lastToConnect = time.Time{}
	n.firstToDisconnect = time.Time{}
}

// Reader to read random data.
type netperfReader struct {
	n   uint64
	eof chan struct{}
	buf []byte
}

func (m *netperfReader) Read(b []byte) (int, error) {
	select {
	case <-m.eof:
		return 0, io.EOF
	default:
	}
	n := copy(b, m.buf)
	atomic.AddUint64(&m.n, uint64(n))
	return n, nil
}

func netperf(ctx context.Context, duration time.Duration) madmin.NetperfNodeResult {
	r := &netperfReader{eof: make(chan struct{})}
	r.buf = make([]byte, 128*humanize.KiByte)
	rand.Read(r.buf)

	connectionsPerPeer := 16

	if len(globalNotificationSys.peerClients) > 16 {
		// For a large cluster it's enough to have 1 connection per peer to saturate the network.
		connectionsPerPeer = 1
	}

	errStr := ""
	var wg sync.WaitGroup
	for index := range globalNotificationSys.peerClients {
		if globalNotificationSys.peerClients[index] == nil {
			continue
		}
		go func(index int) {
			for i := 0; i < connectionsPerPeer; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := globalNotificationSys.peerClients[index].DevNull(ctx, r)
					if err != nil {
						errStr = fmt.Sprintf("error with %s: %s", globalNotificationSys.peerClients[index].String(), err.Error())
					}
				}()
			}
		}(index)
	}

	time.Sleep(duration)
	xioutil.SafeClose(r.eof)
	wg.Wait()
	for globalNetPerfRX.ActiveConnections() != 0 {
		time.Sleep(time.Second)
	}
	rx := float64(globalNetPerfRX.RXSample)
	delta := globalNetPerfRX.firstToDisconnect.Sub(globalNetPerfRX.lastToConnect)
	if delta < 0 {
		rx = 0
		errStr = "network disconnection issues detected"
	}

	globalNetPerfRX.Reset()
	return madmin.NetperfNodeResult{Endpoint: "", TX: r.n / uint64(duration.Seconds()), RX: uint64(rx / delta.Seconds()), Error: errStr}
}

func siteNetperf(ctx context.Context, duration time.Duration) madmin.SiteNetPerfNodeResult {
	r := &netperfReader{eof: make(chan struct{})}
	r.buf = make([]byte, 128*humanize.KiByte)
	rand.Read(r.buf)

	clusterInfos, err := globalSiteReplicationSys.GetClusterInfo(ctx)
	if err != nil {
		return madmin.SiteNetPerfNodeResult{Error: err.Error()}
	}

	// Scale the number of connections from 32 -> 4 from small to large clusters.
	connectionsPerPeer := 3 + (29+len(clusterInfos.Sites)-1)/len(clusterInfos.Sites)

	errStr := ""
	var wg sync.WaitGroup

	for _, info := range clusterInfos.Sites {
		// skip self
		if globalDeploymentID() == info.DeploymentID {
			continue
		}
		info := info
		wg.Add(connectionsPerPeer)
		for range connectionsPerPeer {
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, duration+10*time.Second)
				defer cancel()
				perfNetRequest(
					ctx,
					info.DeploymentID,
					adminPathPrefix+adminAPIVersionPrefix+adminAPISiteReplicationDevNull,
					r,
				)
			}()
		}
	}

	time.Sleep(duration)
	xioutil.SafeClose(r.eof)
	wg.Wait()
	for globalSiteNetPerfRX.ActiveConnections() != 0 && !contextCanceled(ctx) {
		time.Sleep(time.Second)
	}
	rx := float64(globalSiteNetPerfRX.RXSample)
	delta := globalSiteNetPerfRX.firstToDisconnect.Sub(globalSiteNetPerfRX.lastToConnect)
	// If the first disconnected before the last connected, we likely had a network issue.
	if delta <= 0 {
		rx = 0
		errStr = "detected network disconnections, possibly an unstable network"
	}

	globalSiteNetPerfRX.Reset()
	return madmin.SiteNetPerfNodeResult{
		Endpoint:        "",
		TX:              r.n,
		TXTotalDuration: duration,
		RX:              uint64(rx),
		RXTotalDuration: delta,
		Error:           errStr,
		TotalConn:       uint64(connectionsPerPeer),
	}
}

// perfNetRequest - reader for http.request.body
func perfNetRequest(ctx context.Context, deploymentID, reqPath string, reader io.Reader) (result madmin.SiteNetPerfNodeResult) {
	result = madmin.SiteNetPerfNodeResult{}
	cli, err := globalSiteReplicationSys.getAdminClient(ctx, deploymentID)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	rp := cli.GetEndpointURL()
	reqURL := &url.URL{
		Scheme: rp.Scheme,
		Host:   rp.Host,
		Path:   reqPath,
	}
	result.Endpoint = rp.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), reader)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	client := &http.Client{
		Transport: globalRemoteTargetTransport,
	}
	resp, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer xhttp.DrainBody(resp.Body)
	err = gob.NewDecoder(resp.Body).Decode(&result)
	// endpoint have been overwritten
	result.Endpoint = rp.String()
	if err != nil {
		result.Error = err.Error()
	}
	return result
}
