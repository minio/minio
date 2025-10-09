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
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/logger/target/testlogger"
)

func BenchmarkRequests(b *testing.B) {
	for n := 2; n <= 32; n *= 2 {
		b.Run("servers="+strconv.Itoa(n), func(b *testing.B) {
			benchmarkGridRequests(b, n)
		})
	}
}

func benchmarkGridRequests(b *testing.B, n int) {
	defer testlogger.T.SetErrorTB(b)()
	errFatal := func(err error) {
		b.Helper()
		if err != nil {
			b.Fatal(err)
		}
	}
	rpc := NewSingleHandler[*testRequest, *testResponse](handlerTest2, newTestRequest, newTestResponse)
	grid, err := SetupTestGrid(n)
	errFatal(err)
	b.Cleanup(grid.Cleanup)
	// Create n managers.
	for _, remote := range grid.Managers {
		// Register a single handler which echos the payload.
		errFatal(remote.RegisterSingleHandler(handlerTest, func(payload []byte) ([]byte, *RemoteErr) {
			defer PutByteBuffer(payload)
			return append(GetByteBuffer()[:0], payload...), nil
		}))
		errFatal(rpc.Register(remote, func(req *testRequest) (resp *testResponse, err *RemoteErr) {
			return &testResponse{
				OrgNum:    req.Num,
				OrgString: req.String,
				Embedded:  *req,
			}, nil
		}))
		errFatal(err)
	}
	const payloadSize = 512
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	payload := make([]byte, payloadSize)
	_, err = rng.Read(payload)
	errFatal(err)

	// Wait for all to connect
	// Parallel writes per server.
	b.Run("bytes", func(b *testing.B) {
		for par := 1; par <= 32; par *= 2 {
			b.Run("par="+strconv.Itoa(par*runtime.GOMAXPROCS(0)), func(b *testing.B) {
				defer timeout(60 * time.Second)()
				ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
				defer cancel()
				b.ReportAllocs()
				b.SetBytes(int64(len(payload) * 2))
				b.ResetTimer()
				t := time.Now()
				var ops int64
				var lat int64
				b.SetParallelism(par)
				b.RunParallel(func(pb *testing.PB) {
					rng := rand.New(rand.NewSource(time.Now().UnixNano()))
					n := 0
					var latency int64
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
						// Send the payload.
						t := time.Now()
						resp, err := conn.Request(ctx, handlerTest, payload)
						latency += time.Since(t).Nanoseconds()
						if err != nil {
							if debugReqs {
								fmt.Println(err.Error())
							}
							b.Fatal(err.Error())
						}
						PutByteBuffer(resp)
						n++
					}
					atomic.AddInt64(&ops, int64(n))
					atomic.AddInt64(&lat, latency)
				})
				spent := time.Since(t)
				if spent > 0 && n > 0 {
					// Since we are benchmarking n parallel servers we need to multiply by n.
					// This will give an estimate of the total ops/s.
					latency := float64(atomic.LoadInt64(&lat)) / float64(time.Millisecond)
					b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
					b.ReportMetric(latency/float64(ops), "ms/op")
				}
			})
		}
	})
	b.Run("rpc", func(b *testing.B) {
		for par := 1; par <= 32; par *= 2 {
			b.Run("par="+strconv.Itoa(par*runtime.GOMAXPROCS(0)), func(b *testing.B) {
				defer timeout(60 * time.Second)()
				ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
				defer cancel()
				b.ReportAllocs()
				b.ResetTimer()
				t := time.Now()
				var ops int64
				var lat int64
				b.SetParallelism(par)
				b.RunParallel(func(pb *testing.PB) {
					rng := rand.New(rand.NewSource(time.Now().UnixNano()))
					n := 0
					var latency int64
					managers := grid.Managers
					hosts := grid.Hosts
					req := testRequest{
						Num:    rng.Int(),
						String: "hello",
					}
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
						// Send the payload.
						t := time.Now()
						resp, err := rpc.Call(ctx, conn, &req)
						latency += time.Since(t).Nanoseconds()
						if err != nil {
							if debugReqs {
								fmt.Println(err.Error())
							}
							b.Fatal(err.Error())
						}
						rpc.PutResponse(resp)
						n++
					}
					atomic.AddInt64(&ops, int64(n))
					atomic.AddInt64(&lat, latency)
				})
				spent := time.Since(t)
				if spent > 0 && n > 0 {
					// Since we are benchmarking n parallel servers we need to multiply by n.
					// This will give an estimate of the total ops/s.
					latency := float64(atomic.LoadInt64(&lat)) / float64(time.Millisecond)
					b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
					b.ReportMetric(latency/float64(ops), "ms/op")
				}
			})
		}
	})
}

func BenchmarkStream(b *testing.B) {
	tests := []struct {
		name string
		fn   func(b *testing.B, n int)
	}{
		{name: "request", fn: benchmarkGridStreamReqOnly},
		{name: "responses", fn: benchmarkGridStreamRespOnly},
		{name: "twoway", fn: benchmarkGridStreamTwoway},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for n := 2; n <= 32; n *= 2 {
				b.Run("servers="+strconv.Itoa(n), func(b *testing.B) {
					test.fn(b, n)
				})
			}
		})
	}
}

func benchmarkGridStreamRespOnly(b *testing.B, n int) {
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
	const responses = 10
	// Create n managers.
	for _, remote := range grid.Managers {
		// Register a single handler which echos the payload.
		errFatal(remote.RegisterStreamingHandler(handlerTest, StreamHandler{
			// Send 10x response.
			Handle: func(ctx context.Context, payload []byte, _ <-chan []byte, out chan<- []byte) *RemoteErr {
				for i := range responses {
					toSend := GetByteBuffer()[:0]
					toSend = append(toSend, byte(i))
					toSend = append(toSend, payload...)
					select {
					case <-ctx.Done():
						return nil
					case out <- toSend:
					}
				}
				return nil
			},

			Subroute:    "some-subroute",
			OutCapacity: 1, // Only one message buffered.
			InCapacity:  0,
		}))
		errFatal(err)
	}
	const payloadSize = 512
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	payload := make([]byte, payloadSize)
	_, err = rng.Read(payload)
	errFatal(err)

	// Wait for all to connect
	// Parallel writes per server.
	for par := 1; par <= 32; par *= 2 {
		b.Run("par="+strconv.Itoa(par*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			defer timeout(30 * time.Second)()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * (responses + 1)))
			b.ResetTimer()
			t := time.Now()
			var ops int64
			var lat int64
			b.SetParallelism(par)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				n := 0
				var latency int64
				managers := grid.Managers
				hosts := grid.Hosts
				for pb.Next() {
					// Pick a random manager.
					src, dst := rng.Intn(len(managers)), rng.Intn(len(managers))
					if src == dst {
						dst = (dst + 1) % len(managers)
					}
					local := managers[src]
					conn := local.Connection(hosts[dst]).Subroute("some-subroute")
					if conn == nil {
						b.Fatal("No connection")
					}
					ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
					// Send the payload.
					t := time.Now()
					st, err := conn.NewStream(ctx, handlerTest, payload)
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					got := 0
					err = st.Results(func(b []byte) error {
						got++
						PutByteBuffer(b)
						return nil
					})
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					latency += time.Since(t).Nanoseconds()
					cancel()
					n += got
				}
				atomic.AddInt64(&ops, int64(n))
				atomic.AddInt64(&lat, latency)
			})
			spent := time.Since(t)
			if spent > 0 && n > 0 {
				// Since we are benchmarking n parallel servers we need to multiply by n.
				// This will give an estimate of the total ops/s.
				latency := float64(atomic.LoadInt64(&lat)) / float64(time.Millisecond)
				b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
				b.ReportMetric(latency/float64(ops), "ms/op")
			}
		})
	}
}

func benchmarkGridStreamReqOnly(b *testing.B, n int) {
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
	const requests = 10
	// Create n managers.
	for _, remote := range grid.Managers {
		// Register a single handler which echos the payload.
		errFatal(remote.RegisterStreamingHandler(handlerTest, StreamHandler{
			// Send 10x requests.
			Handle: func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr {
				got := 0
				for b := range in {
					PutByteBuffer(b)
					got++
				}
				if got != requests {
					return NewRemoteErrf("wrong number of requests. want %d, got %d", requests, got)
				}
				return nil
			},

			Subroute:    "some-subroute",
			OutCapacity: 1,
			InCapacity:  1, // Only one message buffered.
		}))
		errFatal(err)
	}
	const payloadSize = 512
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	payload := make([]byte, payloadSize)
	_, err = rng.Read(payload)
	errFatal(err)

	// Wait for all to connect
	// Parallel writes per server.
	for par := 1; par <= 32; par *= 2 {
		b.Run("par="+strconv.Itoa(par*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			defer timeout(30 * time.Second)()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * (requests + 1)))
			b.ResetTimer()
			t := time.Now()
			var ops int64
			var lat int64
			b.SetParallelism(par)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				n := 0
				var latency int64
				managers := grid.Managers
				hosts := grid.Hosts
				for pb.Next() {
					// Pick a random manager.
					src, dst := rng.Intn(len(managers)), rng.Intn(len(managers))
					if src == dst {
						dst = (dst + 1) % len(managers)
					}
					local := managers[src]
					conn := local.Connection(hosts[dst]).Subroute("some-subroute")
					if conn == nil {
						b.Fatal("No connection")
					}
					ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
					// Send the payload.
					t := time.Now()
					st, err := conn.NewStream(ctx, handlerTest, payload)
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					got := 0
					for range requests {
						got++
						st.Requests <- append(GetByteBuffer()[:0], payload...)
					}
					close(st.Requests)
					err = st.Results(func(b []byte) error {
						return nil
					})
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					latency += time.Since(t).Nanoseconds()
					cancel()
					n += got
				}
				atomic.AddInt64(&ops, int64(n))
				atomic.AddInt64(&lat, latency)
			})
			spent := time.Since(t)
			if spent > 0 && n > 0 {
				// Since we are benchmarking n parallel servers we need to multiply by n.
				// This will give an estimate of the total ops/s.
				latency := float64(atomic.LoadInt64(&lat)) / float64(time.Millisecond)
				b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
				b.ReportMetric(latency/float64(ops), "ms/op")
			}
		})
	}
}

func benchmarkGridStreamTwoway(b *testing.B, n int) {
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
	const messages = 10
	// Create n managers.
	const payloadSize = 512
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	payload := make([]byte, payloadSize)
	_, err = rng.Read(payload)
	errFatal(err)

	for _, remote := range grid.Managers {
		// Register a single handler which echos the payload.
		errFatal(remote.RegisterStreamingHandler(handlerTest, StreamHandler{
			// Send 10x requests.
			Handle: func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr {
				got := 0
				for b := range in {
					out <- b
					got++
				}
				if got != messages {
					return NewRemoteErrf("wrong number of requests. want %d, got %d", messages, got)
				}
				return nil
			},

			Subroute:    "some-subroute",
			OutCapacity: 1,
			InCapacity:  1, // Only one message buffered.
		}))
		errFatal(err)
	}

	// Wait for all to connect
	// Parallel writes per server.
	for par := 1; par <= 32; par *= 2 {
		b.Run("par="+strconv.Itoa(par*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			defer timeout(30 * time.Second)()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * (2*messages + 1)))
			b.ResetTimer()
			t := time.Now()
			var ops int64
			var lat int64
			b.SetParallelism(par)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				n := 0
				var latency int64
				managers := grid.Managers
				hosts := grid.Hosts
				for pb.Next() {
					// Pick a random manager.
					src, dst := rng.Intn(len(managers)), rng.Intn(len(managers))
					if src == dst {
						dst = (dst + 1) % len(managers)
					}
					local := managers[src]
					conn := local.Connection(hosts[dst]).Subroute("some-subroute")
					if conn == nil {
						b.Fatal("No connection")
					}
					ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
					// Send the payload.
					t := time.Now()
					st, err := conn.NewStream(ctx, handlerTest, payload)
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					got := 0
					sent := 0
					go func() {
						for range messages {
							st.Requests <- append(GetByteBuffer()[:0], payload...)
							if sent++; sent == messages {
								close(st.Requests)
								return
							}
						}
					}()
					err = st.Results(func(b []byte) error {
						got++
						PutByteBuffer(b)
						return nil
					})
					if err != nil {
						if debugReqs {
							fmt.Println(err.Error())
						}
						b.Fatal(err.Error())
					}
					if got != messages {
						b.Fatalf("wrong number of responses. want %d, got %d", messages, got)
					}
					latency += time.Since(t).Nanoseconds()
					cancel()
					n += got
				}
				atomic.AddInt64(&ops, int64(n*2))
				atomic.AddInt64(&lat, latency)
			})
			spent := time.Since(t)
			if spent > 0 && n > 0 {
				// Since we are benchmarking n parallel servers we need to multiply by n.
				// This will give an estimate of the total ops/s.
				latency := float64(atomic.LoadInt64(&lat)) / float64(time.Millisecond)
				b.ReportMetric(float64(n)*float64(ops)/spent.Seconds(), "vops/s")
				b.ReportMetric(latency/float64(ops), "ms/op")
			}
		})
	}
}
