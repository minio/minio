
[![GoDoc](https://godoc.org/github.com/eclipse/paho.mqtt.golang?status.svg)](https://godoc.org/github.com/eclipse/paho.mqtt.golang)
[![Go Report Card](https://goreportcard.com/badge/github.com/eclipse/paho.mqtt.golang)](https://goreportcard.com/report/github.com/eclipse/paho.mqtt.golang)

Eclipse Paho MQTT Go client
===========================


This repository contains the source code for the [Eclipse Paho](http://eclipse.org/paho) MQTT Go client library. 

This code builds a library which enable applications to connect to an [MQTT](http://mqtt.org) broker to publish messages, and to subscribe to topics and receive published messages.

This library supports a fully asynchronous mode of operation.


Installation and Build
----------------------

This client is designed to work with the standard Go tools, so installation is as easy as:

```
go get github.com/eclipse/paho.mqtt.golang
```

The client depends on Google's [websockets](https://godoc.org/golang.org/x/net/websocket) and [proxy](https://godoc.org/golang.org/x/net/proxy) package, 
also easily installed with the commands:

```
go get golang.org/x/net/websocket
go get golang.org/x/net/proxy
```


Usage and API
-------------

Detailed API documentation is available by using to godoc tool, or can be browsed online
using the [godoc.org](http://godoc.org/github.com/eclipse/paho.mqtt.golang) service.

Make use of the library by importing it in your Go client source code. For example,
```
import "github.com/eclipse/paho.mqtt.golang"
```

Samples are available in the `cmd` directory for reference.


Runtime tracing
---------------

Tracing is enabled by assigning logs (from the Go log package) to the logging endpoints, ERROR, CRITICAL, WARN and DEBUG


Reporting bugs
--------------

Please report bugs by raising issues for this project in github https://github.com/eclipse/paho.mqtt.golang/issues 


More information
----------------

Discussion of the Paho clients takes place on the [Eclipse paho-dev mailing list](https://dev.eclipse.org/mailman/listinfo/paho-dev).

General questions about the MQTT protocol are discussed in the [MQTT Google Group](https://groups.google.com/forum/?hl=en-US&fromgroups#!forum/mqtt).

There is much more information available via the [MQTT community site](http://mqtt.org).
