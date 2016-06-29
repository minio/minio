BLAKE2b-SIMD
============

Pure Go implementation of BLAKE2b using SIMD optimizations.

Introduction
------------

This package is based on the pure go [BLAKE2b](https://github.com/dchest/blake2b) implementation of Dmitry Chestnykh and merges it with the (`cgo` dependent) SSE optimized [BLAKE2](https://github.com/codahale/blake2) implementation (which in turn is based on [official implementation](https://github.com/BLAKE2/BLAKE2). It does so by using [Go's Assembler](https://golang.org/doc/asm) for amd64 architectures with a fallback for other architectures.

It gives roughly a 3x performance improvement over the non-optimized go version.

Benchmarks
----------

| Dura          |  1 GB |
| ------------- |:-----:|
| blake2b-SIMD  | 1.59s |
| blake2b       | 4.66s |

