# Minio Bucket Notification Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Events occurring on objects in a bucket can be monitored using bucket event notifications. Event types supported by Minio server are

| Supported Event Types | | |
|:---------------------------|--------------------------------------------|-------------------------|
| `s3:ObjectCreated:Put`     | `s3:ObjectCreated:CompleteMultipartUpload` | `s3:ObjectAccessed:Head`|
| `s3:ObjectCreated:Post`    | `s3:ObjectRemoved:Delete`                  |
| `s3:ObjectCreated:Copy`    | `s3:ObjectAccessed:Get`                    |

Use client tools like `mc` to set and listen for event notifications using the [`event` sub-command](https://docs.minio.io/docs/minio-client-complete-guide#events). Minio SDK's [`BucketNotification` APIs](https://docs.minio.io/docs/golang-client-api-reference#SetBucketNotification) can also be used. The notification message Minio sends to publish an event is a JSON message with the following [structure](https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html).

Bucket events can be published to the following targets:

| Supported Notification Targets | | |
|:---------------------------|--------------------------------------------|-------------------------|
| [`AMQP`](#AMQP) | [`Redis`](#Redis) | [`MySQL`](#MySQL) |
| [`MQTT`](#MQTT) | [`NATS`](#NATS) | [`Apache Kafka`](#apache-kafka) |
| [`Elasticsearch`](#Elasticsearch) | [`PostgreSQL`](#PostgreSQL) | [`Webhooks`](#webhooks) |

## Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio-quickstart-guide).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).

<a name="AMQP"></a>
## Publish Minio events via AMQP

Install RabbitMQ from [here](https://www.rabbitmq.com/).

### Step 1: Add AMQP endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The AMQP configuration is located in the `amqp` key under the `notify` top-level key. Create a configuration key-value pair here for your AMQP instance. The key is a name for your AMQP endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `url` | _string_ | (Required) AMQP server endpoint, e.g. `amqp://myuser:mypassword@localhost:5672` |
| `exchange` | _string_ | Name of the exchange. |
| `routingKey` | _string_ | Routing key for publishing. |
| `exchangeType` | _string_ | Kind of exchange. |
| `deliveryMode` | _uint8_ | Delivery mode for publishing. 0 or 1 - transient; 2 - persistent. |
| `mandatory` | _bool_ | Publishing related bool. |
| `immediate` | _bool_ | Publishing related bool. |
| `durable` | _bool_ | Exchange declaration related bool. |
| `internal` | _bool_ | Exchange declaration related bool. |
| `noWait` | _bool_ | Exchange declaration related bool. |
| `autoDeleted` | _bool_ | Exchange declaration related bool. |

An example configuration for RabbitMQ is shown below:

```json
"amqp": {
    "1": {
        "enable": true,
        "url": "amqp://myuser:mypassword@localhost:5672",
        "exchange": "bucketevents",
        "routingKey": "bucketlogs",
        "exchangeType": "fanout",
        "deliveryMode": 0,
        "mandatory": false,
        "immediate": false,
        "durable": false,
        "internal": false,
        "noWait": false,
        "autoDeleted": false
    }
}
```

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs::1:amqp` at start-up if there were no errors.

Minio supports all the exchanges available in [RabbitMQ](https://www.rabbitmq.com/). For this setup, we are using ``fanout`` exchange.

Note that, you can add as many AMQP server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the AMQP instance and an object of per-server configuration parameters.


### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs::1:amqp``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:amqp --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
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
‘{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}\n
```

<a name="MQTT"></a>
## Publish Minio events MQTT

Install an MQTT Broker from [here](https://mosquitto.org/).

### Step 1: Add MQTT endpoint to Minio

the default location of Minio server configuration file is ``~./minio/config.json``. The MQTT configuration is location in the `mqtt` key under the `notify` top-level key. Create a configuration key-value pair here for your MQTT instance. The key is a name for your MQTT endpoint, and the value is a collection of key-value parameters described in the table below.


| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `broker` | _string_ | (Required) MQTT server endpoint, e.g. `tcp://localhost:1883` |
| `topic` | _string_ | (Required) Name of the MQTT topic to publish on, e.g. `minio` |
| `qos` | _int_ | Set the Quality of Service Level |
| `clientId` | _string_ | Unique ID for the MQTT broker to identify Minio |
| `username` | _string_ | Username to connect to the MQTT server (if required) |
| `password` | _string_ | Password to connect to the MQTT server (if required) |

An example configuration for MQTT is shown below:

```json
"mqtt": {
    "1": {
        "enable": true,
        "broker": "tcp://localhost:1883",
        "topic": "minio",
        "qos": 1,
        "clientId": "minio",
        "username": "",
        "password": ""
    }
}
```

After updating the configuration file, restart the Minio sever to put the changes into effect. The server will print a line like `SQS ARNs: arn:minio:sqs::1:mqtt` at start-up if there were no errors.

Minio supports any MQTT server that supports MQTT 3.1 or 3.1.1 and can connect to them over TCP, TLS, or a Websocket connection using ``tcp://``, ``tls://``, or ``ws://`` respectively as the scheme for the broker url. See the [Go Client](http://www.eclipse.org/paho/clients/golang/) documentation for more information.

Note that, you can add as many MQTT server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the MQTT instance and an object of per-server configuration parameters.


### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs::1:mqtt``.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:mqtt --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on MQTT

The python program below waits on mqtt topic ``/minio`` and prints event notifications on the console. We use [paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/) library to do this.

```py
#!/usr/bin/env python3
from __future__ import print_function
import paho.mqtt.client as mqtt

# This is the Subscriber

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("minio")

def on_message(client, userdata, msg):
    print(msg.payload)

client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost",1883,60)
client.loop_forever()
```

Execute this example python program to watch for MQTT events on the console.

```py
python mqtt.py
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

You should receive the following event notification via MQTT once the upload completes.

```py
python mqtt.py
{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}
```

<a name="Elasticsearch"></a>
## Publish Minio events via Elasticsearch

Install [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) server.

This notification target supports two formats: _namespace_ and _access_.

When the _namespace_ format is used, Minio synchronizes objects in the bucket with documents in the index. For each event in the Minio, the server creates a document with the bucket and object name from the event as the document ID. Other details of the event are stored in the body of the document. Thus if an existing object is over-written in Minio, the corresponding document in the Elasticsearch index is updated. If an object is deleted, the corresponding document is deleted from the index.

When the _access_ format is used, Minio appends events as documents in an Elasticsearch index. For each event, a document with the event details, with the timestamp of document set to the event's timestamp is appended to an index. The ID of the documented is randomly generated by Elasticsearch. No documents are deleted or modified in this format.

The steps below show how to use this notification target in `namespace` format. The other format is very similar and is omitted for brevity.

### Step 1: Ensure minimum requirements are met

Minio requires a 5.x series version of Elasticsearch. This is the latest major release series. Elasticsearch provides version upgrade migration guidelines [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html).

### Step 2: Add Elasticsearch endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The Elasticsearch configuration is located in the `elasticsearch` key under the `notify` top-level key. Create a configuration key-value pair here for your Elasticsearch instance. The key is a name for your Elasticsearch endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `format` | _string_ | (Required) Either `namespace` or `access`. |
| `url` | _string_ | (Required) The Elasticsearch server's address, with optional authentication info. For example: `http://localhost:9200` or with authentication info `http://elastic:MagicWord@127.0.0.1:9200`. |
| `index` | _string_ | (Required) The name of an Elasticsearch index in which Minio will store documents. |

An example of Elasticsearch configuration is as follows:

```json
"elasticsearch": {
    "1": {
        "enable": true,
        "format": "namespace",
        "url": "http://127.0.0.1:9200",
        "index": "minio_events"
    }
},
```

If Elasticsearch has authentication enabled, the credentials can be supplied to Minio via the `url` parameter formatted as `PROTO://USERNAME:PASSWORD@ELASTICSEARCH_HOST:PORT`.

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs::1:elasticsearch` at start-up if there were no errors.

Note that, you can add as many Elasticsearch server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the Elasticsearch instance and an object of per-server configuration parameters.

### Step 3: Enable bucket notification using Minio client

We will now enable bucket event notifications on a bucket named `images`. Whenever a JPEG image is created/overwritten, a new document is added or an existing document is updated in the Elasticsearch index configured above. When an existing object is deleted, the corresponding document is deleted from the index. Thus, the rows in the Elasticsearch index, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:elasticsearch --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 4: Test on Elasticsearch

Upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Use curl to view contents of ``minio_events`` index.

```
$ curl  "http://localhost:9200/minio_events/_search?pretty=true"
{
  "took" : 40,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "failed" : 0
  },
  "hits" : {
    "total" : 1,
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "minio_events",
        "_type" : "event",
        "_id" : "images/myphoto.jpg",
        "_score" : 1.0,
        "_source" : {
          "Records" : [
            {
              "eventVersion" : "2.0",
              "eventSource" : "minio:s3",
              "awsRegion" : "",
              "eventTime" : "2017-03-30T08:00:41Z",
              "eventName" : "s3:ObjectCreated:Put",
              "userIdentity" : {
                "principalId" : "minio"
              },
              "requestParameters" : {
                "sourceIPAddress" : "127.0.0.1:38062"
              },
              "responseElements" : {
                "x-amz-request-id" : "14B09A09703FC47B",
                "x-minio-origin-endpoint" : "http://192.168.86.115:9000"
              },
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
                  "size" : 6474,
                  "eTag" : "a3410f4f8788b510d6f19c5067e60a90",
                  "sequencer" : "14B09A09703FC47B"
                }
              },
              "source" : {
                "host" : "127.0.0.1",
                "port" : "38062",
                "userAgent" : "Minio (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z"
              }
            }
          ]
        }
      }
    ]
  }
}
```

This output shows that a document has been created for the event in Elasticsearch.

Here we see that the document ID is the bucket and object name. In case `access` format was used, the document ID would be automatically generated by Elasticsearch.

<a name="Redis"></a>
## Publish Minio events via Redis

Install [Redis](http://redis.io/download) server. For illustrative purposes, we have set the database password as "yoursecret".

This notification target supports two formats: _namespace_ and _access_.

When the _namespace_ format is used, Minio synchronizes objects in the bucket with entries in a hash. For each entry, the key is formatted as "bucketName/objectName" for an object that exists in the bucket, and the value is the JSON-encoded event data about the operation that created/replaced the object in Minio. When objects are updated or deleted, the corresponding entry int he hash is updated or deleted respectively.

When the _access_ format is used, Minio appends events to a list using [RPUSH](https://redis.io/commands/rpush). Each item in the list is a JSON encoded list with two items, where the first item is a timestamp string, and second item is a JSON object containing evnet data about the operation that happened in the bucket. No entries appended to the list are updated or deleted by Minio in this format.

The steps below show how to use this notification target in `namespace` and `access` format.

### Step 1: Add Redis endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The Redis configuration is located in the `redis` key under the `notify` top-level key. Create a configuration key-value pair here for your Redis instance. The key is a name for your Redis endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `format` | _string_ | (Required) Either `namespace` or `access`. |
| `address` | _string_ | (Required) The Redis server's address. For example: `localhost:6379`. |
| `password` | _string_ | (Optional) The Redis server's password. |
| `key` | _string_ | (Required) The name of the redis key under which events are stored. A hash is used in case of `namespace` format and a list in case of `access` format.|

An example of Redis configuration is as follows:

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

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs::1:redis` at start-up if there were no errors.

Note that, you can add as many Redis server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the Redis instance and an object of per-server configuration parameters.

### Step 2: Enable bucket notification using Minio client

We will now enable bucket event notifications on a bucket named `images`. Whenever a JPEG image is created/overwritten, a new key is added or an existing key is updated in the Redis hash configured above. When an existing object is deleted, the corresponding key is deleted from the Redis hash. Thus, the rows in the Redis hash, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on Redis

Start the `redis-cli` Redis client program to inspect the contents in Redis. Run the `monitor` Redis command. This prints each operation performed on Redis as it occurs.

```
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

In the previous terminal, you will now see the operation that Minio performs on Redis:

```
127.0.0.1:6379> monitor
OK
1490686879.650649 [0 172.17.0.1:44710] "PING"
1490686879.651061 [0 172.17.0.1:44710] "HSET" "minio_events" "images/myphoto.jpg" "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"minio:s3\",\"awsRegion\":\"\",\"eventTime\":\"2017-03-28T07:41:19Z\",\"eventName\":\"s3:ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1:52234\"},\"responseElements\":{\"x-amz-request-id\":\"14AFFBD1ACE5F632\",\"x-minio-origin-endpoint\":\"http://192.168.86.115:9000\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":2586,\"eTag\":\"5d284463f9da279f060f0ea4d11af098\",\"sequencer\":\"14AFFBD1ACE5F632\"}},\"source\":{\"host\":\"127.0.0.1\",\"port\":\"52234\",\"userAgent\":\"Minio (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z\"}}]}"
```

Here we see that Minio performed `HSET` on `minio_events` key.

In case, `access` format was used, then `minio_events` would be a list, and the Minio server would have performed an `RPUSH` to append to the list. A consumer of this list would ideally use `BLPOP` to remove list items from the left-end of the list.

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
        "streaming": {
            "enable": false,
            "clusterID": "",
            "clientID": "",
            "async": false,
            "maxPubAcksInflight": 0
        }
    }
},
```

Restart Minio server to reflect config changes. ``bucketevents`` is the subject used by NATS in this example.

Minio server also supports [NATS Streaming mode](http://nats.io/documentation/streaming/nats-streaming-intro/) that offers additional functionality like `Message/event persistence`, `At-least-once-delivery`, and `Publisher rate limiting`. To configure Minio server to send notifications to NATS Streaming server, update the Minio server configuration file as follows:

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
        "streaming": {
            "enable": true,
            "clusterID": "test-cluster",
            "clientID": "minio-client",
            "async": true,
            "maxPubAcksInflight": 10
        }
    }
},
```
Read more about sections `clusterID`, `clientID` on [NATS documentation](https://github.com/nats-io/nats-streaming-server/blob/master/README.md). Section `maxPubAcksInflight` is explained [here](https://github.com/nats-io/go-nats-streaming#publisher-rate-limiting).

### Step 2: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs::1:nats``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:nats --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Test on NATS

If you use NATS server, check out this sample program below to log the bucket notification added to NATS.

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
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"","eventTime":"2016-10-12T13:51:33Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"","time":"2016-10-12T06:51:33-07:00"}
```

If you use NATS Streaming server, check out this sample program below to log the bucket notification added to NATS.

```go
package main

// Import Go and NATS packages
import (
	"fmt"
	"runtime"

	"github.com/nats-io/go-nats-streaming"
)

func main() {
	natsConnection, _ := stan.Connect("test-cluster", "test-client")
	log.Println("Connected")

	// Subscribe to subject
	log.Printf("Subscribing to subject 'bucketevents'\n")
	natsConnection.Subscribe("bucketevents", func(m *stan.Msg) {

		// Handle the message
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})

	// Keep the connection alive
	runtime.Goexit()
}
```

```
go run nats.go
2017/07/07 11:47:40 Connected
2017/07/07 11:47:40 Subscribing to subject 'bucketevents'
```
Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

The example ``nats.go`` program prints event notification to console.

```
Received a message: {"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"","eventTime":"2017-07-07T18:46:37Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"192.168.1.80:55328"},"responseElements":{"x-amz-request-id":"14CF20BD1EFD5B93","x-minio-origin-endpoint":"http://127.0.0.1:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":248682,"eTag":"f1671feacb8bbf7b0397c6e9364e8c92","contentType":"image/jpeg","userDefined":{"content-type":"image/jpeg"},"versionId":"1","sequencer":"14CF20BD1EFD5B93"}},"source":{"host":"192.168.1.80","port":"55328","userAgent":"Minio (linux; amd64) minio-go/2.0.4 mc/DEVELOPMENT.GOGET"}}],"level":"info","msg":"","time":"2017-07-07T11:46:37-07:00"}
```

<a name="PostgreSQL"></a>
## Publish Minio events via PostgreSQL

Install [PostgreSQL](https://www.postgresql.org/) database server. For illustrative purposes, we have set the "postgres" user password as `password` and created a database called `minio_events` to store the events.

This notification target supports two formats: _namespace_ and _access_.

When the _namespace_ format is used, Minio synchronizes objects in the bucket with rows in the table. It creates rows with two columns: key and value. The key is the bucket and object name of an object that exists in Minio. The value is JSON encoded event data about the operation that created/replaced the object in Minio. When objects are updated or deleted, the corresponding row from this table is updated or deleted respectively.

When the _access_ format is used, Minio appends events to a table. It creates rows with two columns: event_time and event_data. The event_time is the time at which the event occurred in the Minio server. The event_data is the JSON encoded event data about the operation on an object. No rows are deleted or modified in this format.

The steps below show how to use this notification target in `namespace` format. The other format is very similar and is omitted for brevity.

### Step 1: Ensure minimum requirements are met

Minio requires PostgreSQL version 9.5 or above. Minio uses the [`INSERT ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) (aka UPSERT) feature, introduced in version 9.5 and the [JSONB](https://www.postgresql.org/docs/9.4/static/datatype-json.html) data-type introduced in version 9.4.

### Step 2: Add PostgreSQL endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The PostgreSQL configuration is located in the `postgresql` key under the `notify` top-level key. Create a configuration key-value pair here for your PostgreSQL instance. The key is a name for your PostgreSQL endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `format` | _string_ | (Required) Either `namespace` or `access`. |
| `connectionString` | _string_ | (Optional) [Connection string parameters](https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters) for the PostgreSQL server. Can be used to set `sslmode` for example. |
| `table` | _string_ | (Required) Table name in which events will be stored/updated. If the table does not exist, the Minio server creates it at start-up.|
| `host` | _string_ | (Optional) Host name of the PostgreSQL server. Defaults to `localhost`|
| `port` | _string_ | (Optional) Port on which to connect to PostgreSQL server. Defaults to `5432`. |
| `user` | _string_ | (Optional) Database user name. Defaults to user running the server process. |
| `password` | _string_ | (Optional) Database password. |
| `database` | _string_ | (Optional) Database name. |

An example of PostgreSQL configuration is as follows:

```
"postgresql": {
    "1": {
        "enable": true,
        "format": "namespace",
        "connectionString": "sslmode=disable",
        "table": "bucketevents",
        "host": "127.0.0.1",
        "port": "5432",
        "user": "postgres",
        "password": "password",
        "database": "minio_events"
    }
}
```

Note that for illustration here, we have disabled SSL. In the interest of security, for production this is not recommended.

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs::1:postgresql` at start-up if there were no errors.

Note that, you can add as many PostgreSQL server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the PostgreSQL instance and an object of per-server configuration parameters.


### Step 3: Enable bucket notification using Minio client

We will now enable bucket event notifications on a bucket named `images`. Whenever a JPEG image is created/overwritten, a new row is added or an existing row is updated in the PostgreSQL configured above. When an existing object is deleted, the corresponding row is deleted from the PostgreSQL table. Thus, the rows in the PostgreSQL table, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add myminio/images arn:minio:sqs::1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
mc events list myminio/images
arn:minio:sqs::1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 4: Test on PostgreSQL

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Open PostgreSQL terminal to list the rows in the `bucketevents` table.

```
$ psql -h 127.0.0.1 -U postgres -d minio_events
minio_events=# select * from bucketevents;

key                 |                      value
--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
(1 row)
```

<a name="MySQL"></a>
## Publish Minio events via MySQL

Install MySQL from [here](https://dev.mysql.com/downloads/mysql/). For illustrative purposes, we have set the root password as `password` and created a database called `miniodb` to store the events.

This notification target supports two formats: _namespace_ and _access_.

When the _namespace_ format is used, Minio synchronizes objects in the bucket with rows in the table. It creates rows with two columns: key_name and value. The key_name is the bucket and object name of an object that exists in Minio. The value is JSON encoded event data about the operation that created/replaced the object in Minio. When objects are updated or deleted, the corresponding row from this table is updated or deleted respectively.

When the _access_ format is used, Minio appends events to a table. It creates rows with two columns: event_time and event_data. The event_time is the time at which the event occurred in the Minio server. The event_data is the JSON encoded event data about the operation on an object. No rows are deleted or modified in this format.

The steps below show how to use this notification target in `namespace` format. The other format is very similar and is omitted for brevity.

### Step 1: Ensure minimum requirements are met

Minio requires MySQL version 5.7.8 or above. Minio uses the [JSON](https://dev.mysql.com/doc/refman/5.7/en/json.html) data-type introduced in version 5.7.8. We tested this setup on MySQL 5.7.17.

### Step 2: Add MySQL server endpoint configuration to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. The MySQL configuration is located in the `mysql` key under the `notify` top-level key. Create a configuration key-value pair here for your MySQL instance. The key is a name for your MySQL endpoint, and the value is a collection of key-value parameters described in the table below.

| Parameter | Type | Description |
|:---|:---|:---|
| `enable` | _bool_ | (Required) Is this server endpoint configuration active/enabled? |
| `format` | _string_ | (Required) Either `namespace` or `access`. |
| `dsnString` | _string_ | (Optional) [Data-Source-Name connection string](https://github.com/go-sql-driver/mysql#dsn-data-source-name) for the MySQL server. If not specified, the connection information specified by the `host`, `port`, `user`, `password` and `database` parameters are used. |
| `table` | _string_ | (Required) Table name in which events will be stored/updated. If the table does not exist, the Minio server creates it at start-up.|
| `host` | _string_ | Host name of the MySQL server (used only if `dsnString` is empty). |
| `port` | _string_ | Port on which to connect to the MySQL server (used only if `dsnString` is empty). |
| `user` | _string_ | Database user-name (used only if `dsnString` is empty). |
| `password` | _string_ | Database password (used only if `dsnString` is empty). |
| `database` | _string_ | Database name (used only if `dsnString` is empty). |

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

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs::1:mysql` at start-up if there were no errors.

Note that, you can add as many MySQL server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the MySQL instance and an object of per-server configuration parameters.


### Step 3: Enable bucket notification using Minio client

We will now setup bucket notifications on a bucket named `images`. Whenever a JPEG image object is created/overwritten, a new row is added or an existing row is updated in the MySQL table configured above. When an existing object is deleted, the corresponding row is deleted from the MySQL table. Thus, the rows in the MySQL table, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add myminio/images arn:minio:sqs::1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
arn:minio:sqs::1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 4: Test on MySQL

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
| images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "467886be95c8ecfd71a2900e3f461b4f", "size": 26, "sequencer": "14AC59476F809FD3"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "", "eventName": "s3:ObjectCreated:Put", "eventTime": "2017-03-16T11:29:00Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {"x-amz-request-id": "14AC59476F809FD3", "x-minio-origin-endpoint": "http://192.168.86.110:9000"}, "requestParameters": {"sourceIPAddress": "127.0.0.1:38260"}}]} |
+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

```

<a name="apache-kafka"></a>
## Publish Minio events via Kafka

Install Apache Kafka from [here](http://kafka.apache.org/).

### Step 1: Ensure minimum requirements are met

Minio requires Kafka version 0.10 or 0.9. Internally Minio uses the [Shopify/sarama](https://github.com/Shopify/sarama/) library and so has the same version compatibility as provided by this library.

### Step 2: Add Kafka endpoint to Minio

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

### Step 3: Enable bucket notification using Minio client

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs::1:kafka``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:kafka --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:kafka s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 4: Test on Kafka

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
{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"","eventTime":"2017-01-31T10:01:51Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},"requestParameters":{"sourceIPAddress":"192.173.5.2:57904"},"responseElements":{"x-amz-request-id":"149ED2FD25589220","x-minio-origin-endpoint":"http://192.173.5.2:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":541596,"eTag":"04451d05b4faf4d62f3d538156115e2a","sequencer":"149ED2FD25589220"}}}],"level":"info","msg":"","time":"2017-01-31T15:31:51+05:30"}
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

We will enable bucket event notification to trigger whenever a JPEG image is uploaded to ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs::1:webhook``. To learn more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc mb myminio/images-thumbnail
mc events add myminio/images arn:minio:sqs::1:webhook --events put --suffix .jpg
```

Check if event notification is successfully configured by

```
mc events list myminio/images
```

You should get a response like this

```
arn:minio:sqs::1:webhook   s3:ObjectCreated:*   Filter: suffix=".jpg"
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
