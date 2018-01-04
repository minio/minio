[![Godoc Reference](https://godoc.org/github.com/minio/highwayhash?status.svg)](https://godoc.org/github.com/minio/highwayhash)
[![Build Status](https://travis-ci.org/minio/highwayhash.svg?branch=master)](https://travis-ci.org/minio/highwayhash)

## HighwayHash

[HighwayHash](https://github.com/google/highwayhash) is a pseudo-random-function (PRF) developed by Jyrki Alakuijala, Bill Cox and Jan Wassenberg (Google research). HighwayHash takes a 256 bit key and computes 64, 128 or 256 bit hash values of given messages.

It can be used to prevent hash-flooding attacks or authenticate short-lived messages. Additionally it can be used as a fingerprinting function. HighwayHash is not a general purpose cryptographic hash function (such as Blake2b, SHA-3 or SHA-2) and should not be used if strong collision resistance is required. 

This repository contains a native Go version and optimized assembly implementations on both Intel and ARM platforms.  

### High performance

HighwayHash is an approximately 5x faster SIMD hash function as compared to [SipHash](https://www.131002.net/siphash/siphash.pdf) which in itself is a fast and 'cryptographically strong' pseudo-random function designed by Aumasson and Bernstein.

HighwayHash uses a new way of mixing inputs with AVX2 multiply and permute instructions. The multiplications are 32x32 bit giving 64 bits-wide results and are therefore infeasible to reverse. Additionally permuting equalizes the distribution of the resulting bytes. The algorithm outputs digests ranging from 64 bits up to 256 bits at no extra cost.

### Stable

All three output sizes of HighwayHash have been declared [stable](https://github.com/google/highwayhash/#versioning-and-stability) as of January 2018. This means that the hash results for any given input message are guaranteed not to change.

### Installation

Install: `go get -u github.com/minio/highwayhash`

### Intel Performance

Below are the single core results on an Intel Core i7 (3.1 GHz) for 256 bit outputs:

```
BenchmarkSum256_16      		  204.17 MB/s
BenchmarkSum256_64      		 1040.63 MB/s
BenchmarkSum256_1K      		 8653.30 MB/s
BenchmarkSum256_8K      		13476.07 MB/s
BenchmarkSum256_1M      		14928.71 MB/s
BenchmarkSum256_5M      		14180.04 MB/s
BenchmarkSum256_10M     		12458.65 MB/s
BenchmarkSum256_25M     		11927.25 MB/s
```

So for moderately sized messages it tops out at about 15 GB/sec. Also for small messages (1K) the performance is already at approximately 60% of the maximum throughput. 

### ARM Performance

On an 8 core 1.2 GHz ARM Cortex-A53 (running Debian 8.0 Jessie with Go 1.7.4) the following results were obtained:

Platform/CPU      | Write 64         | Write 1024        | Write 8192
----------------- | ---------------- | ----------------- | -----------------
ARM64 NEON        | 384 MB/s         | 955 MB/s          | 1053 MB/s

*Note: For now just the (main) update loop is implemented in assembly, so for small messages there is still considerable overhead due to initialization and finalization.*

### Performance compared to other hashing techniques

On a Skylake CPU (3.0 GHz Xeon Platinum 8124M) the table below shows how HighwayHash compares to other hashing techniques for 5 MB messages (single core performance, all Golang implementations, see [benchmark](https://github.com/fwessels/HashCompare/blob/master/benchmarks_test.go)).

```
BenchmarkHighwayHash      	    	11986.98 MB/s
BenchmarkSHA256_AVX512    	    	 3552.74 MB/s
BenchmarkBlake2b          	    	  972.38 MB/s
BenchmarkSHA1             	    	  950.64 MB/s
BenchmarkMD5              	    	  684.18 MB/s
BenchmarkSHA512           	    	  562.04 MB/s
BenchmarkSHA256           	    	  383.07 MB/s
```

*Note: the AVX512 version of SHA256 uses the [multi-buffer crypto library](https://github.com/intel/intel-ipsec-mb) technique as developed by Intel, more details can be found in [sha256-simd](https://github.com/minio/sha256-simd/).*

### Qualitative assessment

We have performed a 'qualitative' assessment of how HighwayHash compares to Blake2b in terms of the distribution of the checksums for varying numbers of messages. It shows that HighwayHash behaves similarly according to the following graph:

![Hash Comparison Overview](https://s3.amazonaws.com/s3git-assets/hash-comparison-final.png)

More information can be found in [HashCompare](https://github.com/fwessels/HashCompare).

### Requirements

All Go versions >= 1.7 are supported. Notice that the amd64 AVX2 implementation is only available with Go 1.8 and newer.

### Contributing

Contributions are welcome, please send PRs for any enhancements.