# NUID

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![ReportCard](http://goreportcard.com/badge/nats-io/nuid)](http://goreportcard.com/report/nats-io/nuid)
[![Build Status](https://travis-ci.org/nats-io/nuid.svg?branch=master)](http://travis-ci.org/nats-io/nuid)
[![Release](https://img.shields.io/badge/release-v1.0.0-1eb0fc.svg)](https://github.com/nats-io/nuid/releases/tag/v1.0.0)
[![GoDoc](http://godoc.org/github.com/nats-io/nuid?status.png)](http://godoc.org/github.com/nats-io/nuid)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nuid/badge.svg?branch=master)](https://coveralls.io/github/nats-io/nuid?branch=master)

A highly performant unique identifier generator.

## Installation

Use the `go` command:

	$ go get github.com/nats-io/nuid

## Basic Usage
```go

// Utilize the global locked instance
nuid := nuid.Next()

// Create an instance, these are not locked.
n := nuid.New()
nuid = n.Next()

// Generate a new crypto/rand seeded prefix.
// Generally not needed, happens automatically.
n.RandomizePrefix()
```

## Performance
NUID needs to be very fast to generate and be truly unique, all while being entropy pool friendly.
NUID uses 12 bytes of crypto generated data (entropy draining), and 10 bytes of pseudo-random
sequential data that increments with a pseudo-random increment.

Total length of a NUID string is 22 bytes of base 36 ascii text, so 36^22 or
17324272922341479351919144385642496 possibilities.

NUID can generate identifiers as fast as 60ns, or ~16 million per second. There is an associated
benchmark you can use to test performance on your own hardware.

## License

(The MIT License)

Copyright (c) 2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
