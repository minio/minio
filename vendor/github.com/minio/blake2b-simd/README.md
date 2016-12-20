BLAKE2b-SIMD
============

Pure Go implementation of BLAKE2b using SIMD optimizations.

Introduction
------------

This package was initially based on the pure go [BLAKE2b](https://github.com/dchest/blake2b) implementation of Dmitry Chestnykh and merged with the (`cgo` dependent) AVX optimized [BLAKE2](https://github.com/codahale/blake2) implementation (which in turn is based on the [official implementation](https://github.com/BLAKE2/BLAKE2). It does so by using [Go's Assembler](https://golang.org/doc/asm) for amd64 architectures with a golang only fallback for other architectures.

In addition to AVX there is also support for AVX2 as well as SSE. Best performance is obtained with AVX2 which gives roughly a **4X** performance increase approaching hashing speeds of **1GB/sec** on a single core.

Benchmarks
----------

This is a summary of the performance improvements. Full details are shown below.

| Technology |  128K |
| ---------- |:-----:|
| AVX2       | 3.94x |
| AVX        | 3.28x |
| SSE        | 2.85x |

asm2plan9s
----------

In order to be able to work more easily with AVX2/AVX instructions, a separate tool was developed to convert AVX2/AVX instructions into the corresponding BYTE sequence as accepted by Go assembly. See [asm2plan9s](https://github.com/minio/asm2plan9s) for more information.

bt2sum
------

[bt2sum](https://github.com/s3git/bt2sum) is a utility that takes advantages of the BLAKE2b SIMD optimizations to compute check sums using the BLAKE2 Tree hashing mode in so called 'unlimited fanout' mode.

Technical details
-----------------

BLAKE2b is a hashing algorithm that operates on 64-bit integer values. The AVX2 version uses the 256-bit wide YMM registers in order to essentially process four operations in parallel. AVX and SSE operate on 128-bit values simultaneously (two operations in parallel). Below are excerpts from `compressAvx2_amd64.s`, `compressAvx_amd64.s`, and `compress_generic.go` respectively.

```
    VPADDQ  YMM0,YMM0,YMM1   /* v0 += v4, v1 += v5, v2 += v6, v3 += v7 */
```

```
    VPADDQ  XMM0,XMM0,XMM2   /* v0 += v4, v1 += v5 */
    VPADDQ  XMM1,XMM1,XMM3   /* v2 += v6, v3 += v7 */
```

```
    v0 += v4
    v1 += v5
    v2 += v6
    v3 += v7
```

Detailed benchmarks
-------------------

Example performance metrics were generated on  Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz - 6 physical cores, 12 logical cores running Ubuntu GNU/Linux with kernel version 4.4.0-24-generic (vanilla with no optimizations).

### AVX2

```
$ benchcmp go.txt avx2.txt
benchmark                old ns/op     new ns/op     delta
BenchmarkHash64-12       1481          849           -42.67%
BenchmarkHash128-12      1428          746           -47.76%
BenchmarkHash1K-12       6379          2227          -65.09%
BenchmarkHash8K-12       37219         11714         -68.53%
BenchmarkHash32K-12      140716        35935         -74.46%
BenchmarkHash128K-12     561656        142634        -74.60%

benchmark                old MB/s     new MB/s     speedup
BenchmarkHash64-12       43.20        75.37        1.74x
BenchmarkHash128-12      89.64        171.35       1.91x
BenchmarkHash1K-12       160.52       459.69       2.86x
BenchmarkHash8K-12       220.10       699.32       3.18x
BenchmarkHash32K-12      232.87       911.85       3.92x
BenchmarkHash128K-12     233.37       918.93       3.94x
```

### AVX2: Comparison to other hashing techniques

```
$ go test -bench=Comparison
BenchmarkComparisonMD5-12    	    1000	   1726121 ns/op	 607.48 MB/s
BenchmarkComparisonSHA1-12   	     500	   2005164 ns/op	 522.94 MB/s
BenchmarkComparisonSHA256-12 	     300	   5531036 ns/op	 189.58 MB/s
BenchmarkComparisonSHA512-12 	     500	   3423030 ns/op	 306.33 MB/s
BenchmarkComparisonBlake2B-12	    1000	   1232690 ns/op	 850.64 MB/s
```

Benchmarks below were generated on a MacBook Pro with a 2.7 GHz Intel Core i7.

### AVX

```
$ benchcmp go.txt  avx.txt 
benchmark               old ns/op     new ns/op     delta
BenchmarkHash64-8       813           458           -43.67%
BenchmarkHash128-8      766           401           -47.65%
BenchmarkHash1K-8       4881          1763          -63.88%
BenchmarkHash8K-8       36127         12273         -66.03%
BenchmarkHash32K-8      140582        43155         -69.30%
BenchmarkHash128K-8     567850        173246        -69.49%

benchmark               old MB/s     new MB/s     speedup
BenchmarkHash64-8       78.63        139.57       1.78x
BenchmarkHash128-8      166.98       318.73       1.91x
BenchmarkHash1K-8       209.76       580.68       2.77x
BenchmarkHash8K-8       226.76       667.46       2.94x
BenchmarkHash32K-8      233.09       759.29       3.26x
BenchmarkHash128K-8     230.82       756.56       3.28x
```

### SSE

```
$ benchcmp go.txt sse.txt 
benchmark               old ns/op     new ns/op     delta
BenchmarkHash64-8       813           478           -41.21%
BenchmarkHash128-8      766           411           -46.34%
BenchmarkHash1K-8       4881          1870          -61.69%
BenchmarkHash8K-8       36127         12427         -65.60%
BenchmarkHash32K-8      140582        49512         -64.78%
BenchmarkHash128K-8     567850        199040        -64.95%

benchmark               old MB/s     new MB/s     speedup
BenchmarkHash64-8       78.63        133.78       1.70x
BenchmarkHash128-8      166.98       311.23       1.86x
BenchmarkHash1K-8       209.76       547.37       2.61x
BenchmarkHash8K-8       226.76       659.20       2.91x
BenchmarkHash32K-8      233.09       661.81       2.84x
BenchmarkHash128K-8     230.82       658.52       2.85x
```

License
-------

Released under the Apache License v2.0. You can find the complete text in the file LICENSE.

Contributing
------------

Contributions are welcome, please send PRs for any enhancements.
