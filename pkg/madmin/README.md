# Minio Admin Library. [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
The Minio Admin Golang Client SDK provides APIs to manage Minio services.

This quickstart guide will show you how to install the Minio Admin client SDK, connect to Minio admin service, and provide a walkthrough of a simple file uploader.

This document assumes that you have a working [Golang setup](https://docs.minio.io/docs/how-to-install-golang).

## Download from GitHub

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
}
```

## Quick Start Example - Server Info

This example program connects to minio server, gets the current disk status and other useful server information.

#### ServiceStatus.go

```go
package main

import (
	"encoding/json"
	"fmt"
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

	si, err := mdmClnt.ServerInfo()
	if err != nil {
		log.Fatalln(err)
	}
	b, err := json.Marshal(si)
	fmt.Printf("%s\n", string(b))
}
```

Replace the endpoint and access credentials above according to an actual setup.

#### Run ServiceStatus

The sample output below shows the result of executing the above program against a locally hosted server.

```sh
[{"error":"","addr":"localhost:9000","data":{"storage":{"Total":460373336064,"Free":77001187328,"Backend":{"Type":2,"OnlineDisks":4,"OfflineDisks":0,"StandardSCParity":2,"RRSCParity":2}},"network":{"transferred":30599,"received":36370},"http":{"totalHEADs":{"count":0,"avgDuration":"0s"},"successHEADs":{"count":0,"avgDuration":"0s"},"totalGETs":{"count":11,"avgDuration":"0s"},"successGETs":{"count":11,"avgDuration":"0s"},"totalPUTs":{"count":0,"avgDuration":"0s"},"successPUTs":{"count":0,"avgDuration":"0s"},"totalPOSTs":{"count":0,"avgDuration":"0s"},"successPOSTs":{"count":0,"avgDuration":"0s"},"totalDELETEs":{"count":0,"avgDuration":"0s"},"successDELETEs":{"count":0,"avgDuration":"0s"}},"server":{"uptime":596915001694,"version":"2018-01-18T20:33:21Z","commitID":"e2d5a87b2676e3e01f0f4fa7ebd01205364cfb16","region":"us-east-1","sqsARN":null}}},{"error":"","addr":"minio2:9000","data":{"storage":{"Total":460373336064,"Free":77001187328,"Backend":{"Type":2,"OnlineDisks":4,"OfflineDisks":0,"StandardSCParity":2,"RRSCParity":2}},"network":{"transferred":28538,"received":11845},"http":{"totalHEADs":{"count":0,"avgDuration":"0s"},"successHEADs":{"count":0,"avgDuration":"0s"},"totalGETs":{"count":0,"avgDuration":"0s"},"successGETs":{"count":0,"avgDuration":"0s"},"totalPUTs":{"count":0,"avgDuration":"0s"},"successPUTs":{"count":0,"avgDuration":"0s"},"totalPOSTs":{"count":0,"avgDuration":"0s"},"successPOSTs":{"count":0,"avgDuration":"0s"},"totalDELETEs":{"count":0,"avgDuration":"0s"},"successDELETEs":{"count":0,"avgDuration":"0s"}},"server":{"uptime":595852367296,"version":"2018-01-18T20:33:21Z","commitID":"e2d5a87b2676e3e01f0f4fa7ebd01205364cfb16","region":"us-east-1","sqsARN":null}}},{"error":"","addr":"minio3:9000","data":{"storage":{"Total":460373336064,"Free":77001187328,"Backend":{"Type":2,"OnlineDisks":4,"OfflineDisks":0,"StandardSCParity":2,"RRSCParity":2}},"network":{"transferred":27624,"received":11708},"http":{"totalHEADs":{"count":0,"avgDuration":"0s"},"successHEADs":{"count":0,"avgDuration":"0s"},"totalGETs":{"count":0,"avgDuration":"0s"},"successGETs":{"count":0,"avgDuration":"0s"},"totalPUTs":{"count":0,"avgDuration":"0s"},"successPUTs":{"count":0,"avgDuration":"0s"},"totalPOSTs":{"count":0,"avgDuration":"0s"},"successPOSTs":{"count":0,"avgDuration":"0s"},"totalDELETEs":{"count":0,"avgDuration":"0s"},"successDELETEs":{"count":0,"avgDuration":"0s"}},"server":{"uptime":595831126778,"version":"2018-01-18T20:33:21Z","commitID":"e2d5a87b2676e3e01f0f4fa7ebd01205364cfb16","region":"us-east-1","sqsARN":null}}},{"error":"","addr":"minio4:9000","data":{"storage":{"Total":460373336064,"Free":77001187328,"Backend":{"Type":2,"OnlineDisks":4,"OfflineDisks":0,"StandardSCParity":2,"RRSCParity":2}},"network":{"transferred":27740,"received":12116},"http":{"totalHEADs":{"count":0,"avgDuration":"0s"},"successHEADs":{"count":0,"avgDuration":"0s"},"totalGETs":{"count":0,"avgDuration":"0s"},"successGETs":{"count":0,"avgDuration":"0s"},"totalPUTs":{"count":0,"avgDuration":"0s"},"successPUTs":{"count":0,"avgDuration":"0s"},"totalPOSTs":{"count":0,"avgDuration":"0s"},"successPOSTs":{"count":0,"avgDuration":"0s"},"totalDELETEs":{"count":0,"avgDuration":"0s"},"successDELETEs":{"count":0,"avgDuration":"0s"}},"server":{"uptime":595349958375,"version":"2018-01-18T20:33:21Z","commitID":"e2d5a87b2676e3e01f0f4fa7ebd01205364cfb16","region":"us-east-1","sqsARN":null}}}]
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
* [set-credentials.go](https://github.com/minio/minio/blob/master/pkg/madmin/examples/set-credentials.go)

## Contribute

[Contributors Guide](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)
