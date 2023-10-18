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

package grid

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/logger/target/testlogger"
)

func BenchmarkGrid(b *testing.B) {
	for n := 2; n <= 32; n *= 2 {
		b.Run("servers="+strconv.Itoa(n), func(b *testing.B) {
			benchmarkGridServers(b, n)
		})
	}
}

func benchmarkGridServers(b *testing.B, n int) {
	defer testlogger.T.SetErrorTB(b)()
	errFatal := func(err error) {
		b.Helper()
		if err != nil {
			b.Fatal(err)
		}
	}
	grid, err := SetupTestGrid(n)
	errFatal(err)
	b.Cleanup(grid.Cleanup)
	// Create n managers.
	for _, remote := range grid.Managers {
		// Register a single handler which echos the payload.
		errFatal(remote.RegisterSingleHandler(handlerTest, func(payload []byte) ([]byte, *RemoteErr) {
			return append(GetByteBuffer()[:0], payload...), nil
		}))
		errFatal(err)
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	payload := make([]byte, 512)
	_, err = rng.Read(payload)
	errFatal(err)

	// Wait for all to connect
	// Parallel writes per server.
	for par := 1; par <= 32; par *= 2 {
		b.Run("par="+strconv.Itoa(par), func(b *testing.B) {
			defer timeout(30 * time.Second)()

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			t := time.Now()
			var ops int64
			b.SetParallelism(n * par)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				n := 0
				managers := grid.Managers
				hosts := grid.Hosts
				for pb.Next() {
					// Pick a random manager.
					src, dst := rng.Intn(len(managers)), rng.Intn(len(managers))
					if src == dst {
						dst = (dst + 1) % len(managers)
					}
					local := managers[src]
					conn := local.Connection(hosts[dst])
					if conn == nil {
						b.Fatal("No connection")
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					// Send the payload.
					resp, err := conn.Request(ctx, handlerTest, payload)
					cancel()
					if err != nil {
						if !errors.Is(err, context.DeadlineExceeded) {
							b.Fatal(err)
						}
					}
					PutByteBuffer(resp)
					n++
				}
				atomic.AddInt64(&ops, int64(n))
			})
			spent := time.Since(t)
			if spent > 0 {
				// Since we are benchmarking n parallel servers we need to multiply by n.
				// This will give an estimate of the total ops/s.
				b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
			}
		})
	}
}
