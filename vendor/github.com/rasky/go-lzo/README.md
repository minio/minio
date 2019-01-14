# go-lzo

[![Build status](https://travis-ci.org/rasky/go-lzo.svg)](https://travis-ci.org/rasky/go-lzo)
[![Coverage Status](https://coveralls.io/repos/rasky/go-lzo/badge.svg?branch=master&service=github)](https://coveralls.io/github/rasky/go-lzo?branch=master)

Native LZO1X implementation in Golang

This code has been written using the original LZO1X source code as a reference,
to study and understand the algorithms. Both the LZO1X-1 and LZO1X-999
algorithms are implemented. These are the most popular of the whole LZO suite
of algorithms.

Being a straightforward port of the original source code, it shares the same
license (GPLv2) as I can't possibly claim any copyright on it.

I plan to eventually reimplement LZO1X-1 from scratch. At that point, I will be
also changing license.

# Benchmarks

These are the benchmarks obtained running the testsuite over the Canterbury
corpus for the available compressor levels:

Compressor | Level | Original | Compressed | Factor | Time | Speed
-----------|-------|----------|------------|--------|------|------
LZO1X-1   | - | 18521760 | 8957481 | 51.6% | 0.16s | 109MiB/s
LZO1X-999 | 1 | 18521760 | 8217347 | 55.6% | 1.38s | 13MiB/s
LZO1X-999 | 2 | 18521760 | 7724879 | 58.3% | 1.50s | 12MiB/s
LZO1X-999 | 3 | 18521760 | 7384377 | 60.1% | 1.68s | 10MiB/s
LZO1X-999 | 4 | 18521760 | 7266674 | 60.8% | 1.69s | 10MiB/s
LZO1X-999 | 5 | 18521760 | 6979879 | 62.3% | 2.75s | 6.4MiB/s
LZO1X-999 | 6 | 18521760 | 6938593 | 62.5% | 4.53s | 3.9MiB/s
LZO1X-999 | 7 | 18521760 | 6905362 | 62.7% | 6.94s | 2.5MiB/s
LZO1X-999 | 8 | 18521760 | 6713477 | 63.8% | 20.96s | 863KiB/s
LZO1X-999 | 9 | 18521760 | 6712069 | 63.8% | 22.82s | 792KiB/s

