[![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio server supports Amazon S3 compatible bucket event notification for following targets [AMQP](https://www.amqp.org/about/what), [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html) , [Redis](http://redis.io/documentation), [nats.io](http://nats.io/) and [PostgreSQL](https://www.postgresql.org).

##  Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).

| Notification Targets| 
|:--
| [`RabbitMQ`](#RabbitMQ) |   
| [`Elasticsearch`](#Elasticsearch) |    
| [`Redis`](#Redis) |    
| [`NATS`](#NATS) |    
| [`PostgreSQL`](#PostgreSQL) |    

<a name="RabbitMQ"></a>
## Publish Minio events via RabbitMQ 

Install RabbitMQ from [here](https://www.rabbitmq.com/).

### Recipe steps

### Step 1: Add RabbitMQ endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the RabbitMQ configuration block in ``config.json`` as follows:

```
"amqp": {
			"1": {
				"enable": true,
				"url": "amqp://myuser:mypassword@localhost:5672",
				"exchange": "bucketevents",
				"routingKey": "bucketlogs",
				"exchangeType": "fanout",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
        }
```
Restart Minio server to reflect config changes. Minio supports all the exchange available in [RabbitMQ](https://www.rabbitmq.com/). For this setup we are using ``fanout`` exchange.

If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

We will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:amqp``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:amqp    --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing on RabbitMQ

Below python script waits on queue exchange ``bucketevents`` and prints event notification on console. It uses [Pika Python Client](https://www.rabbitmq.com/tutorials/tutorial-three-python.html), a library for RabbitMQ will be used in this python program.

```py
#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='bucketevents',
                         type='fanout')

result = channel.queue_declare(exclusive=False)
queue_name = result.method.queue

channel.queue_bind(exchange='bucketevents',
                   queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %r" % body)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=False)

channel.start_consuming()
```


Execute this example python script to watch for RabbitMQ events on the console.

```py
python rabbit.py
```

Open another terminal and upload a JPEG image into “images” bucket.

```
mc cp myphoto.jpg myminio/images
```

You should receive the following event notification via RabbitMQ once the upload completes.

```py
python rabbit.py
‘{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”us-east-1",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}\n
```
<a name="Elasticsearch"></a>
## Publish Minio events via Elasticsearch 

Install Elasticsearch 2.4 from [here](https://www.elastic.co/downloads/past-releases/elasticsearch-2-4-0).

## Recipe steps

### Step 1: Add Elasticsearch endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the Elasticsearch configuration block in ``config.json`` as follows:

```
"elasticsearch": {
                        "1": {
                                "enable": true,
                                "url": "http://127.0.0.1:9200",
                                "index": "bucketevents"
                        }
                },
```
Restart Minio server to reflect config changes. ``bucketevents`` is the index used by Elasticsearch.

If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

We will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:elasticsearch``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:elasticsearch --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing on Elasticsearch

Upload a JPEG image into ``images`` bucket, this is the bucket which has been configured for event notification.

```
mc cp myphoto.jpg myminio/images
```

Running ``curl`` we can see Elasticsearch has created a new index name ``bucketevents``.

```
curl -XGET '127.0.0.1:9200/_cat/indices?v'
health status index pri rep docs.count docs.deleted store.size pri.store.size
yellow open   bucketevents  5   1          1            0      7.8kb          7.8kb
```

Use curl to view contents of ``bucketevents`` index.

```
curl -XGET '127.0.0.1:9200/bucketevents/_search?pretty=1'
{
  "took" : 3,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "failed" : 0
  },
  "hits" : {
    "total" : 1,
    "max_score" : 1.0,
    "hits" : [ {
      "_index" : "bucketevents",
      "_type" : "event",
      "_id" : "AVcRVOlwe-uNB1tfj6bx",
      "_score" : 1.0,
      "_source" : {
        "Records" : [ {
          "eventVersion" : "2.0",
          "eventSource" : "aws:s3",
          "awsRegion" : "us-east-1",
          "eventTime" : "2016-09-09T23:42:39.977Z",
          "eventName" : "s3:ObjectCreated:Put",
          "userIdentity" : {
            "principalId" : "minio"
          },
          "requestParameters" : {
            "sourceIPAddress" : "10.1.10.150:52140"
          },
          "responseElements" : { },
          "s3" : {
            "s3SchemaVersion" : "1.0",
            "configurationId" : "Config",
            "bucket" : {
              "name" : "images",
              "ownerIdentity" : {
                "principalId" : "minio"
              },
              "arn" : "arn:aws:s3:::images"
            },
            "object" : {
              "key" : "myphoto.jpg",
              "size" : 200436,
              "sequencer" : "1472CC35E6971AF3"
            }
          }
        } ]
      }
    } ]
  }
}
```
``curl`` output above states that an Elasticsearch index has been successfully created with notification contents.

<a name="Redis"></a>
## Publish Minio events via Redis

Install Redis from [here](http://redis.io/download).

## Recipe steps

### Step 1: Add Redis endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the Redis configuration block in ``config.json`` as follows:

```
"redis": {
			"1": {
				"enable": true,
				"address": "127.0.0.1:6379",
				"password": "yoursecret",
				"key": "bucketevents"
			}
		}
```
Restart Minio server to reflect config changes. ``bucketevents`` is the key used by Redis in this example.

If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

We will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:redis``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing on Redis

Redis comes with handy command line interface ``redis-cli`` to print all notifications on the console.

```
redis-cli -a yoursecret
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

``redis-cli`` prints event notification to the console.

```
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
1474321638.556108 [0 127.0.0.1:40190] "AUTH" "yoursecret"
1474321638.556477 [0 127.0.0.1:40190] "RPUSH" "bucketevents" "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2016-09-19T21:47:18.555Z\",\"eventName\":\"s3:ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"[::1]:39250\"},\"responseElements\":{},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":23745,\"sequencer\":\"1475D7B80ECBD853\"}}}],\"level\":\"info\",\"msg\":\"\",\"time\":\"2016-09-19T14:47:18-07:00\"}\n"
```
<a name="NATS"></a>
## Publish Minio events via NATS 

Install NATS  from [here](http://nats.io/).

## Recipe steps

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
Restart Minio server to reflect config changes. ``bucketevents`` is the subject used by NATS in this example.

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

The example ``nats.go`` program prints event notification to console.

```
go run nats.go 
2016/10/12 06:51:26 Connected
2016/10/12 06:51:26 Subscribing to subject 'bucketevents'
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2016-10-12T13:51:33Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"","time":"2016-10-12T06:51:33-07:00"}
```

<a name="PostgreSQL"></a>

## Publish Minio events via PostgreSQL

Install PostgreSQL from [here](https://www.postgresql.org/).

## Recipe steps

### Step 1: Add PostgreSQL endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the PostgreSQL configuration block in ``config.json`` as follows:

```
"postgresql": {
                        "1": {
                                "enable": true,
                                "connectionString": "",
                                "table": "bucketevents",
                                "host": "127.0.0.1",
                                "port": "5432",
                                "user": "postgres",
                                "password": "mypassword",
                                "database": "bucketevents_db"
                        }
                }
```
Restart Minio server to reflect config changes. ``bucketevents`` is the database table used by PostgreSQL in this example.

If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

We will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:postgresql``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:postgresql --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing on PostgreSQL

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Open PostgreSQL terminal to list saved event notification logs.

```
bucketevents_db=# select * from bucketevents;

key         |                      value                                                                                                                                                                                                                                                                                                
--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
(1 row)
```	



