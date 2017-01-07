# Publish Minio events via NATS [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Minio](https://www.minio.io) server supports Amazon S3 compatible bucket event notifications. In this recipe we will learn how to set up bucket notifications using [NATS](http://nats.io/). 

## 1. Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).


## 2. Installation

Install NATS  from [here](http://nats.io/).

## 3. Recipe steps

### Step 1: Add NATS endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the NATS configuration block in ``config.json`` as follows:

```
"nats": {
            "1": {
                "enable": true,
                "address": "0.0.0.0:4222",
                "subject": "bucketevents",
                "username": "yourusername",
                "password": "yoursecret",
                "token": "",
                "secure": false,
                "pingInterval": 0
            }
        },
```
Restart the Minio server to reflect config changes made above. ``bucketevents`` is the subject used by NATS in this example.

If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

We will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:nats``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:nats --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing on NATS

Using this program below we can log the bucket notification added to NATS.

```go
package main

// Import Go and NATS packages
import (
  "runtime"
  "log"
  "github.com/nats-io/nats"
)

func main() {

    // Create server connection
    natsConnection, _ := nats.Connect("nats://yourusername:yoursecret@localhost:4222")
    log.Println("Connected")

    // Subscribe to subject
    log.Printf("Subscribing to subject 'bucketevents'\n")
    natsConnection.Subscribe("bucketevents", func(msg *nats.Msg) {

      // Handle the message
      log.Printf("Received message '%s\n", string(msg.Data) + "'")
  })

  // Keep the connection alive
  runtime.Goexit()
}
```

```
go run nats.go
2016/10/12 06:39:18 Connected
2016/10/12 06:39:18 Subscribing to subject 'bucketevents'
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Below terminal running nats.go program prints event notification on console.

```
go run nats.go 
2016/10/12 06:51:26 Connected
2016/10/12 06:51:26 Subscribing to subject 'bucketevents'
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2016-10-12T13:51:33Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"","time":"2016-10-12T06:51:33-07:00"}
```
