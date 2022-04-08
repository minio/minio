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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/randreader"
)

// SpeedtestResult return value of the speedtest function
type SpeedtestResult struct {
	Endpoint  string
	Uploads   uint64
	Downloads uint64
	Error     string
}

func newRandomReader(size int) io.Reader {
	return io.LimitReader(randreader.New(), int64(size))
}

// Runs the speedtest on local MinIO process.
func selfSpeedtest(ctx context.Context, size, concurrent int, duration time.Duration, storageClass string) (SpeedtestResult, error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return SpeedtestResult{}, errServerNotInitialized
	}

	var errOnce sync.Once
	var retError string
	var wg sync.WaitGroup
	var totalBytesWritten uint64
	var totalBytesRead uint64

	region := globalSite.Region
	if region == "" {
		region = "us-east-1"
	}

	client, err := minio.New(globalLocalNodeName, &minio.Options{
		Creds:     credentials.NewStaticV4(globalActiveCred.AccessKey, globalActiveCred.SecretKey, ""),
		Secure:    globalIsTLS,
		Transport: globalProxyTransport,
		Region:    region,
	})
	if err != nil {
		return SpeedtestResult{}, err
	}

	objCountPerThread := make([]uint64, concurrent)

	uploadsCtx, uploadsCancel := context.WithCancel(context.Background())
	defer uploadsCancel()

	go func() {
		time.Sleep(duration)
		uploadsCancel()
	}()

	objNamePrefix := uuid.New().String() + "/"

	userMetadata := make(map[string]string)
	userMetadata[globalObjectPerfUserMetadata] = "true"

	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				reader := newRandomReader(size)
				info, err := client.PutObject(uploadsCtx, globalObjectPerfBucket, fmt.Sprintf("%s%d.%d", objNamePrefix, i, objCountPerThread[i]), reader, int64(size), minio.PutObjectOptions{UserMetadata: userMetadata, DisableMultipart: true}) // Bypass S3 API freeze
				if err != nil {
					if !contextCanceled(uploadsCtx) && !errors.Is(err, context.Canceled) {
						errOnce.Do(func() {
							retError = err.Error()
						})
					}
					uploadsCancel()
					return
				}
				atomic.AddUint64(&totalBytesWritten, uint64(info.Size))
				objCountPerThread[i]++
			}
		}(i)
	}
	wg.Wait()

	// We already saw write failures, no need to proceed into read's
	if retError != "" {
		return SpeedtestResult{Uploads: totalBytesWritten, Downloads: totalBytesRead, Error: retError}, nil
	}

	downloadsCtx, downloadsCancel := context.WithCancel(context.Background())
	defer downloadsCancel()
	go func() {
		time.Sleep(duration)
		downloadsCancel()
	}()

	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
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
				opts := minio.GetObjectOptions{}
				opts.Set(globalObjectPerfUserMetadata, "true") // Bypass S3 API freeze
				r, err := client.GetObject(downloadsCtx, globalObjectPerfBucket, fmt.Sprintf("%s%d.%d", objNamePrefix, i, j), opts)
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
				n, err := io.Copy(ioutil.Discard, r)
				r.Close()
				if err == nil {
					// Only capture success criteria - do not
					// have to capture failed reads, truncated
					// reads etc.
					atomic.AddUint64(&totalBytesRead, uint64(n))
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

	return SpeedtestResult{Uploads: totalBytesWritten, Downloads: totalBytesRead, Error: retError}, nil
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
	atomic.StoreUint64(&globalNetPerfRX.RX, 0)
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
	n.RLock()
	defer n.RUnlock()
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
						errStr = err.Error()
					}
				}()
			}
		}(index)
	}

	time.Sleep(duration)
	close(r.eof)
	wg.Wait()
	for {
		if globalNetPerfRX.ActiveConnections() == 0 {
			break
		}
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
