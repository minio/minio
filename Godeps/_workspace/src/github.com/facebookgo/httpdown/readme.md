httpdown [![Build Status](https://secure.travis-ci.org/facebookgo/httpdown.png)](https://travis-ci.org/facebookgo/httpdown)
========

Documentation: https://godoc.org/github.com/facebookgo/httpdown

Package httpdown provides a library that makes it easy to build a HTTP server
that can be shutdown gracefully (that is, without dropping any connections).

If you want graceful restart and not just graceful shutdown, look at the
[grace](https://github.com/facebookgo/grace) package which uses this package
underneath but also provides graceful restart.

Usage
-----

Demo HTTP Server with graceful termination:
https://github.com/facebookgo/httpdown/blob/master/httpdown_example/main.go

1. Install the demo application

        go get github.com/facebookgo/httpdown/httpdown_example

1. Start it in the first terminal

        httpdown_example

   This will output something like:

        2014/11/18 21:57:50 serving on http://127.0.0.1:8080/ with pid 17

1. In a second terminal start a slow HTTP request

        curl 'http://localhost:8080/?duration=20s'

1. In a third terminal trigger a graceful shutdown (using the pid from your output):

        kill -TERM 17

This will demonstrate that the slow request was served before the server was
shutdown. You could also have used `Ctrl-C` instead of `kill` as the example
application triggers graceful shutdown on TERM or INT signals.
