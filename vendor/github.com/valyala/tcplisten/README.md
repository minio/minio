[![Build Status](https://travis-ci.org/valyala/tcplisten.svg)](https://travis-ci.org/valyala/tcplisten)
[![GoDoc](https://godoc.org/github.com/valyala/tcplisten?status.svg)](http://godoc.org/github.com/valyala/tcplisten)
[![Go Report](https://goreportcard.com/badge/github.com/valyala/tcplisten)](https://goreportcard.com/report/github.com/valyala/tcplisten)


Package tcplisten provides customizable TCP net.Listener with various
performance-related options:

 * SO_REUSEPORT. This option allows linear scaling server performance
   on multi-CPU servers.
   See https://www.nginx.com/blog/socket-sharding-nginx-release-1-9-1/ for details.

 * TCP_DEFER_ACCEPT. This option expects the server reads from the accepted
   connection before writing to them.

 * TCP_FASTOPEN. See https://lwn.net/Articles/508865/ for details.


[Documentation](https://godoc.org/github.com/valyala/tcplisten).

The package is derived from [go_reuseport](https://github.com/kavu/go_reuseport).
