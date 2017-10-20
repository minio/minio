fastrand
--------

[![GoDoc](https://godoc.org/github.com/NebulousLabs/fastrand?status.svg)](https://godoc.org/github.com/NebulousLabs/fastrand)
[![Go Report Card](http://goreportcard.com/badge/github.com/NebulousLabs/fastrand)](https://goreportcard.com/report/github.com/NebulousLabs/fastrand)

```
go get github.com/NebulousLabs/fastrand
```

`fastrand` implements a cryptographically secure pseudorandom number generator.
The generator is seeded using the system's default entropy source, and
thereafter produces random values via repeated hashing. As a result, `fastrand`
can generate randomness much faster than `crypto/rand`, and generation cannot
fail beyond a potential panic during `init()`.

`fastrand` also scales better than `crypto/rand` and `math/rand` when called in
parallel. In fact, `fastrand` can even outperform `math/rand` when using enough threads.


## Benchmarks ##

```
// 32 byte reads
BenchmarkRead32                     	10000000	       175 ns/op	 181.86 MB/s
BenchmarkReadCrypto32               	  500000	      2733 ns/op	  11.71 MB/s

// 512 kb reads
BenchmarkRead512kb                   	    1000	   1336217 ns/op	 383.17 MB/s
BenchmarkReadCrypto512kb             	      50	  33423693 ns/op	  15.32 MB/s

// 32 byte reads using 4 threads
BenchmarkRead4Threads32               	 3000000	       392 ns/op	 326.46 MB/s
BenchmarkReadCrypto4Threads32       	  200000	      7579 ns/op	  16.89 MB/s

// 512 kb reads using 4 threads
BenchmarkRead4Threads512kb           	    1000	   1899048 ns/op	1078.43 MB/s
BenchmarkReadCrypto4Threads512kb    	      20	  97423380 ns/op	  21.02 MB/s
```

## Security ##

`fastrand` uses an algorithm similar to Fortuna, which is the basis for the
`/dev/random` device in FreeBSD. However, although the techniques used by
`fastrand` are known to be secure, the specific implementation has not been
reviewed by a security professional. Use with caution.

The general strategy is to use `crypto/rand` at init to get 32 bytes of strong
entropy. From there, the entropy is concatenated to a counter and hashed
repeatedly, providing 64 bytes of random output each time the counter is
incremented. The counter is 16 bytes, which provides strong guarantees that a
cycle will not be seen throughout the lifetime of the program.

The `sync/atomic` package is used to ensure that multiple threads calling
`fastrand` concurrently are always guaranteed to end up with unique counters.
