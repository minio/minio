# sha256-simd

Accelerate SHA256 computations in pure Go for both Intel (AVX2, AVX, SSE) as well as ARM (arm64) platforms.

## Introduction

This package is designed as a drop-in replacement for `crypto/sha256`. For Intel CPUs it has three flavors for AVX2, AVX and SSE whereby the fastest method is automatically chosen depending on CPU capabilities. For ARM CPUs with the Cryptography Extensions advantage is taken of the SHA2 instructions resulting in a massive performance improvement.

This package uses Golang assembly and as such does not depend on cgo. The Intel versions are based on the implementations as described in "Fast SHA-256 Implementations on Intel Architecture Processors" by J. Guilford et al.

## Drop-In Replacement

Following code snippet shows you how you can directly replace wherever `crypto/sha256` is used can be replaced with `github.com/minio/sha256-simd`.

Before:
```go
import "crypto/sha256"

func main() {
        ...
	shaWriter := sha256.New()
	io.Copy(shaWriter, file)
        ...
}
```

After:
```go
import "github.com/minio/sha256-simd"

func main() {
        ...
	shaWriter := sha256.New()
	io.Copy(shaWriter, file)
        ...
}
```

## Performance

Below is the speed in MB/s for a single core (ranked fast to slow) as well as the factor of improvement over `crypto/sha256` (when applicable).

| Processor                         | Package                   |       Speed | Improvement |
| --------------------------------- | ------------------------- | -----------:| -----------:|
| 1.2 GHz ARM Cortex-A53            | minio/sha256-simd (ARM64) |  638.2 MB/s |        105x |
| 2.4 GHz Intel Xeon CPU E5-2620 v3 | minio/sha256-simd (AVX2)  |  355.0 MB/s |       1.88x |
| 2.4 GHz Intel Xeon CPU E5-2620 v3 | minio/sha256-simd (AVX)   |  306.0 MB/s |       1.62x |
| 2.4 GHz Intel Xeon CPU E5-2620 v3 | minio/sha256-simd (SSE)   |  298.7 MB/s |       1.58x |
| 2.4 GHz Intel Xeon CPU E5-2620 v3 | crypto/sha256             |  189.2 MB/s |             |
| 1.2 GHz ARM Cortex-A53            | crypto/sha256             |    6.1 MB/s |             |

Note that the AVX2 version is measured with the "unrolled"/"demacro-ed" version. Due to some Golang assembly restrictions the AVX2 version that uses `defines` loses about 15% performance (you can see the macrofied version, which is a little bit easier to read, [here](https://github.com/minio/sha256-simd/blob/e1b0a493b71bb31e3f1bf82d3b8cbd0d6960dfa6/sha256blockAvx2_amd64.s)).
 
 See further down for detailed performance.
 
## Comparison to other hashing techniques

As measured on Intel Xeon (same as above) with AVX2 version:

| Method  | Package            |    Speed |
| ------- | -------------------| --------:|
| BLAKE2B | [minio/blake2b-simd](https://github.com/minio/blake2b-simd) | 851 MB/s |
| MD5     | crypto/md5         | 607 MB/s |
| SHA1    | crypto/sha1        | 522 MB/s |
| SHA256  | minio/sha256-simd  | 355 MB/s |
| SHA512  | crypto/sha512      | 306 MB/s |

asm2plan9s
----------

In order to be able to work more easily with AVX2/AVX instructions, a separate tool was developed to convert AVX2/AVX instructions into the corresponding BYTE sequence as accepted by Go assembly. See [asm2plan9s](https://github.com/minio/asm2plan9s) for more information.

Why and benefits
----------------

One of the most performance sensitive parts of [Minio](https://minio.io) server (object storage [server](https://github.com/minio/minio) compatible with Amazon S3) is related to SHA256 hash sums calculations. For instance during multi part uploads each part that is uploaded needs to be verified for data integrity by the server. Likewise in order to generated pre-signed URLs check sums must be calculated to ensure their validity.

Other applications that can benefit from enhanced SHA256 performance are deduplication in storage systems, intrusion detection, version control systems, integrity checking, etc.

ARM SHA Extensions
------------------

The 64-bit ARMv8 core has introduced new instructions for SHA1 and SHA2 acceleration as part of the [Cryptography Extensions](http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0501f/CHDFJBCJ.html). Below you can see a small excerpt highlighting one of the rounds as is done for the SHA256 calculation process (for full code see [sha256block_arm64.s](https://github.com/minio/sha256-simd/blob/master/sha256block_arm64.s)).
 
 ```
 sha256h    q2, q3, v9.4s
 sha256h2   q3, q4, v9.4s
 sha256su0  v5.4s, v6.4s
 rev32      v8.16b, v8.16b
 add        v9.4s, v7.4s, v18.4s
 mov        v4.16b, v2.16b
 sha256h    q2, q3, v10.4s
 sha256h2   q3, q4, v10.4s
 sha256su0  v6.4s, v7.4s
 sha256su1  v5.4s, v7.4s, v8.4s
 ```

Detailed benchmarks
-------------------

### ARM64

Benchmarks generated on a 1.2 Ghz Quad-Core ARM Cortex A53 equipped [Pine64](https://www.pine64.com/). 

```
minio@minio-arm:~/gopath/src/github.com/sha256-simd$ benchcmp golang.txt arm64.txt 
benchmark                 old ns/op     new ns/op     delta
BenchmarkHash8Bytes-4     11836         1403          -88.15%
BenchmarkHash1K-4         181143        3138          -98.27%
BenchmarkHash8K-4         1365652       14356         -98.95%
BenchmarkHash1M-4         173192200     1642954       -99.05%

benchmark                 old MB/s     new MB/s     speedup
BenchmarkHash8Bytes-4     0.68         5.70         8.38x
BenchmarkHash1K-4         5.65         326.30       57.75x
BenchmarkHash8K-4         6.00         570.63       95.11x
BenchmarkHash1M-4         6.05         638.23       105.49x
```

Example performance metrics were generated on  Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz - 6 physical cores, 12 logical cores running Ubuntu GNU/Linux with kernel version 4.4.0-24-generic (vanilla with no optimizations).

### AVX2

```
$ benchcmp go.txt avx2.txt
benchmark                  old ns/op     new ns/op     delta
BenchmarkHash8Bytes-12     446           364           -18.39%
BenchmarkHash1K-12         5919          3279          -44.60%
BenchmarkHash8K-12         43791         23655         -45.98%
BenchmarkHash1M-12         5544989       2969305       -46.45%

benchmark                  old MB/s     new MB/s     speedup
BenchmarkHash8Bytes-12     17.93        21.96        1.22x
BenchmarkHash1K-12         172.98       312.27       1.81x
BenchmarkHash8K-12         187.07       346.31       1.85x
BenchmarkHash1M-12         189.10       353.14       1.87x
```

### AVX

```
$ benchcmp go.txt avx.txt 
benchmark                  old ns/op     new ns/op     delta
BenchmarkHash8Bytes-12     446           346           -22.42%
BenchmarkHash1K-12         5919          3701          -37.47%
BenchmarkHash8K-12         43791         27222         -37.84%
BenchmarkHash1M-12         5544989       3426938       -38.20%

benchmark                  old MB/s     new MB/s     speedup
BenchmarkHash8Bytes-12     17.93        23.06        1.29x
BenchmarkHash1K-12         172.98       276.64       1.60x
BenchmarkHash8K-12         187.07       300.93       1.61x
BenchmarkHash1M-12         189.10       305.98       1.62x
```

### SSE

```
$ benchcmp go.txt sse.txt 
benchmark                  old ns/op     new ns/op     delta
BenchmarkHash8Bytes-12     446           362           -18.83%
BenchmarkHash1K-12         5919          3751          -36.63%
BenchmarkHash8K-12         43791         27396         -37.44%
BenchmarkHash1M-12         5544989       3444623       -37.88%

benchmark                  old MB/s     new MB/s     speedup
BenchmarkHash8Bytes-12     17.93        22.05        1.23x
BenchmarkHash1K-12         172.98       272.92       1.58x
BenchmarkHash8K-12         187.07       299.01       1.60x
BenchmarkHash1M-12         189.10       304.41       1.61x
```

License
-------

Released under the Apache License v2.0. You can find the complete text in the file LICENSE.

Contributing
------------

Contributions are welcome, please send PRs for any enhancements.
