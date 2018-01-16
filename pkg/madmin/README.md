# Minio Admin Library. [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
The Minio Admin Golang Client SDK provides APIs to manage Minio services.

This quickstart guide will show you how to install the Minio Admin client SDK, connect to Minio admin service, and provide a walkthrough of a simple file uploader. 

This document assumes that you have a working [Golang setup](https://docs.minio.io/docs/how-to-install-golang).

## Download from Github

```sh

go get -u github.com/minio/minio/pkg/madmin

```

## Initialize Minio Admin Client

You need four items to connect to Minio admin services.


| Parameter  | Description|
| :---         |     :---     |
| endpoint   | URL to object storage service.   |
| accessKeyID | Access key is the user ID that uniquely identifies your account. |
| secretAccessKey | Secret key is the password to your account. |
| secure | Set this value to 'true' to enable secure (HTTPS) access. |


```go

package main

import (
	"github.com/minio/minio/pkg/madmin"
	"log"
)

func main() {
	endpoint := "your-minio.example.com:9000"
	accessKeyID := "YOUR-ACCESSKEYID"
	secretAccessKey := "YOUR-SECRETKEY"
	useSSL := true

	// Initialize minio admin client object.
        madmClnt, err := madmin.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("%v", madmClnt) // Minio admin client is now setup


```

## Quick Start Example - Service Status.

This example program connects to minio server, gets the current disk status.

We will use the Minio server running at [https://your-minio.example.com:9000](https://your-minio.example.com:9000) in this example. Feel free to use this service for testing and development. Access credentials shown in this example are open to the public.

#### ServiceStatus.go

```go
package main

import (
	"log"

	"github.com/minio/minio/pkg/madmin"
)

func main() {
	endpoint := "your-minio.example.com:9000"
	accessKeyID := "YOUR-ACCESSKEYID"
	secretAccessKey := "YOUR-SECRETKEY"
	useSSL := true

	// Initialize minio admin client.
	mdmClnt, err := madmin.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	st, err := madmClnt.ServiceStatus()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", st)

}

```

#### Run ServiceStatus

```sh

go run service-status.go
2016/12/20 16:46:01 madmin.ServiceStatusMetadata{Total:177038229504, Free:120365559808, Backend:struct { Type madmin.BackendType; OnlineDisks int; OfflineDisks int; ReadQuorum int; WriteQuorum int }{Type:1, OnlineDisks:0, OfflineDisks:0, StandardSCParity:0, RRSCParity:0}}

```

## API Reference

### API Reference : Service Operations

* [`ServiceStatus`](./API.md#ServiceStatus)
* [`ServiceRestart`](./API.md#ServiceRestart)
* [`ServiceSetCredentials`](./API.md#ServiceSetCredentials)

## Full Examples

#### Full Examples : Service Operations

* [service-status.go](https://github.com/minio/minio/blob/master/pkg/madmin/examples/service-status.go)
* [service-restart.go](https://github.com/minio/minio/blob/master/pkg/madmin/examples/service-restart.go)
* [service-set-credentials.go](https://github.com/minio/minio/blob/master/pkg/madmin/examples/service-set-credentials.go)

## Contribute

[Contributors Guide](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)

