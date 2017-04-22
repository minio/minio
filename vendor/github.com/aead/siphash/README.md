[![Godoc Reference](https://godoc.org/github.com/aead/siphash?status.svg)](https://godoc.org/github.com/aead/siphash)
[![Build Status](https://travis-ci.org/aead/siphash.svg?branch=master)](https://travis-ci.org/aead/siphash)

## The SipHash pseudo-random function

SipHash is a family of pseudo-random functions (a.k.a. keyed hash functions) optimized for speed on short messages.  
SipHash computes a 64-bit or 128 bit message authentication code from a variable-length message and 128-bit secret key.
This implementation uses the recommended parameters c=2 and d=4.

### Installation
Install in your GOPATH: `go get -u github.com/aead/siphash`  

### Performance
**AMD64**  
Hardware: Intel i7-6500U 2.50GHz x 2  
System: Linux Ubuntu 16.04 - kernel: 4.4.0-67-generic  
Go version: 1.8.0  
```
name         speed           cpb
Write_8-4     688MB/s ± 0%   3.47
Write_1K-4   2.09GB/s ± 5%   1.11
Sum64_8-4     244MB/s ± 1%   9.77
Sum64_1K-4   2.06GB/s ± 0%   1.13
Sum128_8-4    189MB/s ± 0%  12.62
Sum128_1K-4  2.03GB/s ± 0%   1.15
```

**386**  
Hardware: Intel i7-6500U 2.50GHz x 2 - SSE2 SIMD  
System: Linux Ubuntu 16.04 - kernel: 4.4.0-67-generic  
Go version: 1.8.0  
```
name         speed           cpb
Write_8-4     434MB/s ± 2%   5.44
Write_1K-4   1.24GB/s ± 1%   1.88
Sum64_8-4    92.6MB/s ± 4%  25.92
Sum64_1K-4   1.15GB/s ± 1%   2.03
Sum128_8-4   61.5MB/s ± 5%  39.09
Sum128_1K-4  1.10GB/s ± 0%   2.12
```

**ARM**  
Hardware: ARM-Cortex-A7 (ARMv7) 1GHz (912MHz) x 2  
System:  Linux Ubuntu 14.04.1 - kernel: 3.4.112-sun7i  
Go version: 1.7.4  

```
name         speed           cpb
Write_8-2    43.4MB/s ± 2%  21.97
Write_1K-2    125MB/s ± 1%   7.63
Sum64_8-2    6.51MB/s ± 1% 146.49
Sum64_1K-2    111MB/s ± 1%   8.59 
Sum128_8-2   3.82MB/s ± 2% 249.65
Sum128_1K-2   101MB/s ± 1%   9.44
```
