# Minio Bucket Notification Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Changes in a bucket, such as object uploads and removal, can be monitored using bucket event notification
mechanism and can be published to the following targets:

| Notification Targets|
|:---|
| [`AMQP`](#AMQP) |
| [`Elasticsearch`](#Elasticsearch) |
| [`Redis`](#Redis) |
| [`NATS`](#NATS) |
| [`PostgreSQL`](#PostgreSQL) |
| [`MySQL`](#MySQL) |
| [`Apache Kafka`](#apache-kafka) |
| [`Webhooks`](#webhooks) |

## Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).

<a name="AMQP"></a>
## Publish Minio events via AMQP

Install RabbitMQ from [here](https://www.rabbitmq.com/).

### Step 1: Add AMQP endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the AMQP configuration block in ``config.json`` as follows:

```json
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

Restart Minio server to reflect config changes. Minio supports all the exchanges available in [RabbitMQ](https://www.rabbitmq.com/). For this setup, we are using ``fanout`` exchange.

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:amqp``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:amqp --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on RabbitMQ

The python program below waits on the queue exchange ``bucketevents`` and prints event notifications on the console. We use [Pika Python Client](https://www.rabbitmq.com/tutorials/tutorial-three-python.html) library to do this.

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

Execute this example python program to watch for RabbitMQ events on the console.

```py
python rabbit.py
```

Open another terminal and upload a JPEG image into ``images`` bucket.

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

```json
"elasticsearch": {
    "1": {
        "enable": true,
        "url": "http://127.0.0.1:9200",
        "index": "bucketevents"
    }
},
```

Restart Minio server to reflect config changes. ``bucketevents`` is the index used by Elasticsearch.

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:elasticsearch``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:elasticsearch --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on Elasticsearch

Upload a JPEG image into ``images`` bucket, this is the bucket which has been configured for event notification.

```
mc cp myphoto.jpg myminio/images
```

Run ``curl`` to see new index name ``bucketevents`` in your Elasticsearch setup.

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

### Step 1: Add Redis endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the Redis configuration block in ``config.json`` as follows:

```json
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

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:redis``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on Redis

Redis comes with a handy command line interface ``redis-cli`` to print all notifications on the console.

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

Install NATS from [here](http://nats.io/).

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

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:nats``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:nats --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on NATS

Using this program below we can log the bucket notification added to NATS.

```go
package main

// Import Go and NATS packages
import (
	"log"
	"runtime"

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
		log.Printf("Received message '%s\n", string(msg.Data)+"'")
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

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:postgresql``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:postgresql --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on PostgreSQL

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Open PostgreSQL terminal to list the saved event notification logs.

```
bucketevents_db=# select * from bucketevents;

key         |                      value
--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
(1 row)
```

<a name="MySQL"></a>
## Publish Minio events via MySQL

Install MySQL from [here](https://dev.mysql.com/downloads/mysql/). For illustrative purposes, we have set the root password as `password` and created a database called `miniodb` to store the events.

### Step 1: Add MySQL server endpoint configuration to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The MySQL configuration is located in the `mysql` key under the `notify` top-level key. Create a configuration key-value pair here for your MySQL instance. The key is a name for your MySQL endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Value type | Description |
|:---|:---|:---|
| `enable` | Boolean | (Required) Is this server endpoint configuration active/enabled? |
| `dsnString` | String | (Optional) [Data-Source-Name connection string](https://github.com/go-sql-driver/mysql#dsn-data-source-name) for the MySQL server. If not specified, the connection information specified by the `host`, `port`, `user`, `password` and `database` parameters are used. |
| `table` | String | (Required) Table name in which events will be stored/updated. If the table does not exist, the Minio server creates it at start-up.|
| `host` | String | Host name of the MySQL server (used only if `dsnString` is empty). |
| `port` | String | Port on which to connect to the MySQL server (used only if `dsnString` is empty). |
| `user` | String | Database user-name (used only if `dsnString` is empty). |
| `password` | String | Database password (used only if `dsnString` is empty). |
| `database` | String | Database name (used only if `dsnString` is empty). |

An example of MySQL configuration is as follows:

```
"mysql": {
        "1": {
                "enable": true,
                "dsnString": "",
                "table": "minio_images",
                "host": "172.17.0.1",
                "port": "3306",
                "user": "root",
                "password": "password",
                "database": "miniodb"
        }
}
```

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs:us-east-1:1:mysql` at start-up if there were no errors.

Note that, you can add as many MySQL server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the MySQL instance and an object of per-server configuration parameters.


### Step 2: Enable bucket notification using Minio client

We will now setup bucket notifications on a bucket named `images`. Whenever a JPEG image object is created/overwritten, a new row is added or an existing row is updated in the MySQL table configured above. When an existing object is deleted, the corresponding row is deleted from the MySQL table. Thus, the rows in the MySQL table, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add  myminio/images arn:minio:sqs:us-east-1:1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
arn:minio:sqs:us-east-1:1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on MySQL

Open another terminal and upload a JPEG image into ``images`` bucket:

```
mc cp myphoto.jpg myminio/images
```

Open MySQL terminal and list the rows in the `minio_images` table.

```
$ mysql -h 172.17.0.1 -P 3306 -u root -p miniodb
mysql> select * from minio_images;
+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| key_name           | value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "467886be95c8ecfd71a2900e3f461b4f", "size": 26, "sequencer": "14AC59476F809FD3"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2017-03-16T11:29:00Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {"x-amz-request-id": "14AC59476F809FD3", "x-minio-origin-endpoint": "http://192.168.86.110:9000"}, "requestParameters": {"sourceIPAddress": "127.0.0.1:38260"}}]} |
+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

```

<a name="apache-kafka"></a>
## Publish Minio events via Kafka

Install kafka from [here](http://kafka.apache.org/).

### Step 1: Add kafka endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the kafka configuration block in ``config.json`` as follows:

```
"kafka": {
    "1": {
        "enable": true,
        "brokers": ["localhost:9092"],
        "topic": "bucketevents"
    }
}
```

Restart Minio server to reflect config changes. ``bucketevents`` is the topic used by kafka in this example.

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:kafka``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:kafka --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:kafka s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on kafka

We used [kafkacat](https://github.com/edenhill/kafkacat) to print all notifications on the console.

```
kafkacat -C -b localhost:9092 -t bucketevents
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

``kafkacat`` prints the event notification to the console.

```
kafkacat -b localhost:9092 -t bucketevents
{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2017-01-31T10:01:51Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},"requestParameters":{"sourceIPAddress":"192.173.5.2:57904"},"responseElements":{"x-amz-request-id":"149ED2FD25589220","x-minio-origin-endpoint":"http://192.173.5.2:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":541596,"eTag":"04451d05b4faf4d62f3d538156115e2a","sequencer":"149ED2FD25589220"}}}],"level":"info","msg":"","time":"2017-01-31T15:31:51+05:30"}
```

<a name="webhooks"></a>
## Publish Minio events via Webhooks

[Webhooks](https://en.wikipedia.org/wiki/Webhook) are a way to receive information when it happens, rather than continually polling for that data.

### Step 1: Add Webhook endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the Webhook configuration block in ``config.json`` as follows

```
"webhook": {
  "1": {
    "enable": true,
    "endpoint": "http://localhost:3000/"
}
```
Here the endpoint is the server listening for webhook notifications. Save the file and restart the Minio server for changes to take effect. Note that the endpoint needs to be live and reachable when you restart your Minio server.

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded to ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:webhook``. To learn more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc mb myminio/images-thumbnail
mc events add myminio/images arn:minio:sqs:us-east-1:1:webhook — events put — suffix .jpg
```

Check if event notification is successfully configured by

```
mc events list myminio/images
```

You should get a response like this

```
arn:minio:sqs:us-east-1:1:webhook   s3:ObjectCreated:*   Filter: suffix=".jpg"
```

### Step 3: Test with Thumbnailer

We used [Thumbnailer](https://github.com/minio/thumbnailer) to listen for Minio notifications when a new JPEG file is uploaded (HTTP PUT). Triggered by a notification, Thumbnailer uploads a thumbnail of new image to Minio server. To start with, download and install Thumbnailer.

```
git clone https://github.com/minio/thumbnailer/
npm install
```

Then open the Thumbnailer config file at ``config/webhook.json`` and add the configuration for your Minio server and then start Thumbnailer by

```
NODE_ENV=webhook node thumbnail-webhook.js
```

Thumbnailer starts running at ``http://localhost:3000/``. Next, configure the Minio server to send notifications to this URL (as mentioned in step 1) and use ``mc`` to set up bucket notifications (as mentioned in step 2). Then upload a JPEG image to Minio server by

```
mc cp ~/images.jpg myminio/images
.../images.jpg:  8.31 KB / 8.31 KB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 100.00% 59.42 KB/s 0s
```
Wait a few moments, then check the bucket’s contents with mc ls — you will see a thumbnail appear.

```
mc ls myminio/images-thumbnail
[2017-02-08 11:39:40 IST]   992B images-thumbnail.jpg
```


*NOTE* If you are running [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` on all the nodes with your bucket event notification backend configuration.
