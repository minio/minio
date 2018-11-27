# Minio Bucket Notification Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This topic describes the bucket notification events and publishing targets supported by Minio Server.

1. [Bucket Notification Event Support](#event-support) 
2. [Publish Minio Events via AMQP](#publish-minio-events-via-amqp) 
3. [Publish Minio Events via MQTT](#publish-minio-events-via-mqtt) 
4. [Publish Minio Events via Elasticsearch](#publish-minio-events-via-elasticsearch) 
5. [Publish Minio Events via Redis](#publish-minio-events-via-redis) 
6. [Publish Minio Events via NATS](#publish-minio-events-via-nats) 
7. [Publish Minio Events via PostgreSQL](#publish-minio-events-via-postgresql) 
8. [Publish Minio Events via MySQL](#publish-minio-events-via-mysql) 
9. [Publish Minio Events via Kafka](#publish-minio-events-via-kafka) 
10. [Publish Minio Events via Webhooks](#publish-minio-events-via-webhooks)

**Note:** Before continuing, ensure the [Minio Server](https://docs.minio.io/docs/minio-quickstart-guide) and [Minio Client](https://docs.minio.io/docs/minio-client-quickstart-guide) are installed.


## 1.<a name="event-support"></a> Bucket Notification Event Support

### 1.1 Event Types Supported by Minio Server
Events occurring on objects in a bucket can be monitored using bucket event notifications. The following event types are supported by Minio Server:

| **Event Type** | **Description** |
|:---------------------------|--------------------------------------------|
| `s3:ObjectCreated:Put`     | An object was created by an HTTP PUT operation. |
| `s3:ObjectCreated:Post`    | An object was created by an HTTP POST operation. |
| `s3:ObjectCreated:Copy`    | An object was created by an S3 copy operation. |
| `s3:ObjectCreated:CompleteMultipartUpload` | An object was created by the completion of an S3 multi-part upload. |
| `s3:ObjectRemoved:Delete`     | An object was removed by an HTTP DELETE operation. |
| `s3:ObjectAccessed:Get`       | An object was obtained by an HTTP GET operation. |

### 1.2 Listening for Events

Use the following methods to set and listen for event notifications:
* Invoke `mc` with the [`events` sub-command](https://docs.minio.io/docs/minio-client-complete-guide#events).
* Invoke Minio's [`BucketNotification` APIs](https://docs.minio.io/docs/golang-client-api-reference#SetBucketNotification).

### 1.3 Notification Message Structure

Minio Server publishes an event by sending a [JSON notification message](https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html).

## 2. <a name="publish-minio-events-via-amqp"></a>Publish Minio Events via AMQP

### 2.1 Install RabbitMQ

Install RabbitMQ using these instructions: [https://www.rabbitmq.com/](https://www.rabbitmq.com/).

### 2.2 Add an AMQP Endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format. The AMQP configuration is located in the `amqp` key under the top-level `notify` key. Create a configuration key-value pair here for the AMQP instance. The key is a name for the AMQP endpoint, and the value is a collection of parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `url` | `string` | (_Required_) The AMQP server endpoint (e.g. `amqp://myuser:mypassword@localhost:5672`). |
| `exchange` | `string` | The name of the exchange. |
| `routingKey` | `string` | The routing key for publishing. |
| `exchangeType` | `string` | The type of exchange. |
| `deliveryMode` | `uint8` | The delivery mode for publishing. Set this field to 0 or 1 for transient, or 2 for persistent. |
| `mandatory` | `bool` | A flag related to publishing. |
| `immediate` | `bool` | A flag related to publishing. |
| `durable` | `bool` | A flag related to exchange declaration. |
| `internal` | `bool` | A flag related to exchange declaration. |
| `noWait` | `bool` | A flag related to exchange declaration. |
| `autoDeleted` | `bool` | A flag related to exchange declaration. |

The following example shows a configuration for RabbitMQ:

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

Minio supports all of the exchanges available in [RabbitMQ](https://www.rabbitmq.com/). The configuration above uses the `fanout` exchange.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the AMQP configuration in `/tmp/myconfig` , use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to take effect. A response similar to this one should be displayed at startup if there were no errors:

```sh
SQS ARNs:  arn:minio:sqs::1:amqp
```

**Note:** An arbitrary number of AMQP server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the AMQP instance, and an object name for the per-server configuration parameters.

### 2.3 Enable Bucket Notification Using Minio Client

The following example enables a bucket event notification to trigger when a JPEG image is uploaded to or deleted from the `images` bucket on the `myminio` server:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:amqp --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```
In this example, the ARN value is ``arn:minio:sqs::1:amqp``. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

### 2.4 Test on RabbitMQ

The following Python example program uses the [Pika Python Client](https://www.rabbitmq.com/tutorials/tutorial-three-python.html) library to wait for the `bucketevents` queue exchange, and prints an event notification to the console:

```py
#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='bucketevents',
                         exchange_type='fanout')

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

Use the following command to execute the program and watch for RabbitMQ events:

```py
python rabbit.py
```

Open another console and use the following command to upload a JPEG image into the `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console. A response similar to this one should be displayed showing an event notification from RabbitMQ:

```json
'{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"","eventTime":"2016–09–08T22:34:38.226Z","eventName":"s3:ObjectCreated:Put",
 "userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"10.1.10.150:44576"},"responseElements":{},"s3":
 {"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},
 "object":{"key":"myphoto.jpg","size":200436,"sequencer":"147279EAF9F40933"}}}],"level":"info","msg":"","time":"2016–09–08T15:34:38–07:00"}'
```

## 3. <a name="publish-minio-events-via-mqtt"></a>Publish Minio Events via MQTT

### 3.1 Install an MQTT Broker

Install an MQTT Broker using these instructions: [https://mosquitto.org/](https://mosquitto.org/).

### 3.2 Add an MQTT Endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format. The MQTT configuration is located in the `mqtt` key under the top-level `notify` key. Create a configuration key-value pair here for the MQTT instance. The key is a name for the MQTT endpoint, and the value is a collection of parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `broker` | `string` | (_Required_) The MQTT server endpoint (e.g. `tcp://localhost:1883`). |
| `topic` | `string` | (_Required_) The name of the MQTT topic on which to publish (e.g. `minio`). |
| `qos` | `int` | Sets the quality-of-service (QOS) level. |
| `clientId` | `string` | The unique ID for the MQTT broker to identify Minio. |
| `username` | `string` | The username to connect to the MQTT server (if required). |
| `password` | `string` | The password to connect to the MQTT server (if required). |

The following is an example of a configuration for MQTT:

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

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the MQTT configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to take effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs: arn:minio:sqs::1:mqtt
```

Minio Server can communicate with an MGTT server using MQTT 3.1 and MGTT 3.1.1. Minio Server can connect to these servers over TCP, TLS, or a Websocket connection using ``tcp://``, ``tls://``, or ``ws://`` respectively, as the scheme for the broker URL. For more information see the [Go Client Documentation](http://www.eclipse.org/paho/clients/golang/).

**Note:** An arbitrary number of MQTT server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the MQTT instance, and an object name for the per-server configuration parameters.

### 3.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable a bucket event notification to trigger when a JPEG image is uploaded to or deleted from the ``images`` bucket on the ``myminio`` server:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:mqtt --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

In this example, the ARN value is ``arn:minio:sqs::1:mqtt``. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

### 3.4 Test on MQTT

The following example Python program uses the [paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/) library to wait on the MQTT `/minio` topic and displays event notifications:

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

Use the following command to execute this program and watch for MQTT events on the console:

```py
python mqtt.py
```

Open another console and use the following command to upload a JPEG image into ``images`` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console. A response similar to this one should be displayed showing an event notification from MQTT:

```json
{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,
 ”userIdentity”:{“principalId”:”minio”}, ”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":
 {“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},
”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}
```

## 4.<a name="publish-minio-events-via-elasticsearch"></a> Publish Minio Events via Elasticsearch

### 4.1 Install Elasticsearch

Minio requires Elasticsearch 5.x which is the latest major release series.

Install Elasticsearch using these instructions: [https://www.elastic.co/downloads/elasticsearch](https://www.elastic.co/downloads/elasticsearch). For version upgrade migration guidelines see these instructions: [https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html).

This notification target supports two formats:

* **namespace**: Minio Server synchronizes objects in the bucket with documents in the index. For each event in Minio, the server creates a document, and a document ID composed of the bucket and object name; details of the event are stored in the body of the document. If an existing object is overwritten in Minio, the corresponding document in the Elasticsearch index is updated. If an object is deleted, the corresponding document is deleted from the index.
* **access**: Minio appends events as documents in an Elasticsearch index. For each event, a document with the event details is appended to an index with the timestamp of the event. The ID of the document is randomly generated by Elasticsearch, and no documents are deleted or modified in this format.

The steps below show how to use this notification target in the `namespace` format. The `access` format is very similar and is omitted for brevity.

### 4.2 Add Elasticsearch endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format. The Elasticsearch configuration is located in the `elasticsearch` key under the top-level `notify` key. Create a configuration key-value pair here for the Elasticsearch instance. The key is a name for the Elasticsearch endpoint, and the value is a collection of parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `format` | `string` | (_Required_) Specifies the format. This field must be set to `namespace` or `access`. |
| `url` | `string` | (_Required_) The Elasticsearch server's address, with optional authentication information. **Example 1**: `http://localhost:9200`. **Example 2**: (with authentication information): `http://elastic:MagicWord@127.0.0.1:9200`. |
| `index` | `string` | (_Required_) The name of an Elasticsearch index in which Minio will store documents. |

The following example shows a configuration for Elasticsearch:

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

If authentication is enabled Elasticsearch, the credentials are supplied to Minio via the `url` parameter formatted as `PROTO://USERNAME:PASSWORD@ELASTICSEARCH_HOST:PORT`.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the Elasticsearch configuration in `/tmp/myconfig` , use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs:  arn:minio:sqs::1:elasticsearch`
```

**Note:** An arbitrary number of Elasticsearch server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the Elasticsearch instance, and an object name for the per-server configuration parameters.

### 4.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:elasticsearch --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

**Note:** The ARN displayed by Minio in the previous step is required to configure this bucket notification. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

When a JPEG image is created/overwritten, a new document is added or an existing document is updated in the Elasticsearch index configured above. When an existing object is deleted, the corresponding document is deleted from the index. The rows in the Elasticsearch index reflect the `.jpg` objects in the `images` bucket.

### 4.4 Test on Elasticsearch

Use the following command to upload a JPEG image into the ``images`` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Use `curl` to view contents of ``minio_events`` index:

```sh
$ curl  "http://localhost:9200/minio_events/_search?pretty=true"
```

A response similar to this one should be displayed:

```json
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

This output shows that a document has been created for the event in Elasticsearch, where the document ID contains the bucket and object name. If the `access` format is used, the document ID will be automatically generated by Elasticsearch.

<a name="Redis"></a>
## 5.<a name="publish-minio-events-via-redis"></a> Publish Minio Events via Redis

### 5.1 Install Redis

Install Redis using these instructions: [http://redis.io/download](http://redis.io/download). 

This notification target supports two formats:

* **namespace**: Minio Server synchronizes objects in the bucket with entries in a hash. For each entry, the key is formatted as `bucketName/objectName` for an existing object in the bucket, and the value is the JSON-encoded event data about the operation that created or replaced the object in Minio Server. When objects are updated or deleted, the corresponding entry in the hash is also updated or deleted.
* **access**: Minio Server appends events to a list using [RPUSH](https://redis.io/commands/rpush). Each item in the list is a JSON-encoded list with a timestamp string and a JSON object containing event data about the operation that occurred in the bucket. None of the entries appended to the list are updated or deleted by Minio in this format.

The steps below show how to use this notification target in with `namespace` and `access` formats.

### 5.2 Add the Redis Endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format. The Redis configuration is located in the `redis` key under the top-level `notify` key. Create a configuration key-value pair here for the Redis instance. The key is a name for the Redis endpoint, and the value is a collection of parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `format` | `string` | (_Required_) Specifies the format. This field must be set to `namespace` or `access`. |
| `address` | `string` | (_Required_) The Redis server's address (e.g. `localhost:6379`). |
| `password` | `string` | (Optional) The Redis server's password. |
| `key` | `string` | (_Required_) The name of the Redis key under which events are stored. A hash is used for the `namespace` format and a list is used for the `access` format.|

The following is an example of a configuration for Redis:

```json
"redis": {
    "1": {
        "enable": true,
        "format": "namespace",
        "address": "127.0.0.1:6379",
        "password": "yoursecret",
        "key": "bucketevents"
    }
}
```

**Note:** The database password has been set to `"yoursecret"` for illustrative purposes.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the Redis configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs:  arn:minio:sqs::1:redis
```

**Note:** an arbitrary number of Redis server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the Redis instance, and an object name for the per-server configuration parameters.

### 5.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

**Note:** The ARN displayed by Minio in the previous step is required to configure this bucket notification. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

When a JPEG image is created/overwritten, a new key is added or an existing key is updated in the Redis hash. When an existing object is deleted, the corresponding key is deleted from the Redis hash. The rows in the Redis hash reflect the `.jpg` objects in the `images` bucket.

### 5.4 Test on Redis

Use the `redis-cli` Redis client program to inspect the contents in Redis: 

```sh
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
```

The `monitor` argument prints each operation performed on Redis as it occurs.

Open another console and upload a JPEG image into the `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console. A response similar to this one should be displayed showing the operation that Minio performs on Redis:

```
127.0.0.1:6379> monitor
OK
1490686879.650649 [0 172.17.0.1:44710] "PING"
1490686879.651061 [0 172.17.0.1:44710] "HSET" "minio_events" "images/myphoto.jpg" "{\"Records\":[{\"eventVersion\":\"2.0\",\
"eventSource\":\"minio:s3\",\"awsRegion\":\"\",\"eventTime\":\"2017-03-28T07:41:19Z\",\"eventName\":\"s3:ObjectCreated:Put\",
\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1:52234\"},\"responseElements\":
{\"x-amz-request-id\":\"14AFFBD1ACE5F632\",\"x-minio-origin-endpoint\":\"http://192.168.86.115:9000\"},\"s3\":
{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":
{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":2586,\"eTag\":
\"5d284463f9da279f060f0ea4d11af098\",\"sequencer\":\"14AFFBD1ACE5F632\"}},\"source\":{\"host\":\"127.0.0.1\",\"port\":\"52234\",
\"userAgent\":\"Minio (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z\"}}]}"
```

In this example, Minio Server performed `HSET` on the `minio_events` key. If the `access` format is used, `minio_events` will be a list, and Minio Server will perform an `RPUSH` to append to the list. A consumer of this list should use `BLPOP` to remove items from the left-end of the list.

## 6.<a name="publish-minio-events-via-nats"></a> Publish Minio Events via NATS

### 6.1 Install NATS

Install NATS using these instructions: [http://nats.io/](http://nats.io/).

### 6.2 Add NATS endpoint to Minio

The following is an example of a configuration block in `config.json` for NATS:

```json
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

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

Restart Minio server for the changes to take effect. 

**Note:** ``bucketevents`` is the subject used by NATS in this example.

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Minio server also supports [NATS Streaming mode](http://nats.io/documentation/streaming/nats-streaming-intro/) that offers additional functionality like `Message/event persistence`, `At-least-once-delivery`, and `Publisher rate limiting`. To configure Minio Server to send notifications to a NATS streaming server, update the Minio Server configuration file as shown below:

```json
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

For more information about `clusterID` see the [NATS documentation](https://github.com/nats-io/nats-streaming-server/blob/master/README.md). For more information about `maxPubAcksInflight`, see [Publisher rate limiting](https://github.com/nats-io/go-nats-streaming#publisher-rate-limiting).

### 6.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:nats --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

In this example, the ARN value is `arn:minio:sqs::1:nats`. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

This enables bucket event notifications to trigger when a JPEG image is uploaded to or deleted from the `images` bucket on the `myminio` server.

### 6.4 Test on NATS

#### 6.4.1 Test on a NATS Server

If a NATS server is being used, the example program below can be used as a template to log the bucket notification added to NATS:

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

Use the following command to execute the program:

```sh
go run nats.go
```

A response similar to this one should be displayed:

```
2016/10/12 06:39:18 Connected
2016/10/12 06:39:18 Subscribing to subject 'bucketevents'
```

Open another console and use the following command to upload a JPEG image into the `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console. A response similar to this one should be displayed:

```
2016/10/12 06:51:26 Connected
2016/10/12 06:51:26 Subscribing to subject 'bucketevents'
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":
[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"","eventTime":"2016-10-12T13:51:33Z","eventName":
"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},
"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images",
"ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,
"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"",
"time":"2016-10-12T06:51:33-07:00"}
```

#### 6.4.2 Test on a NATS Streaming Server

If a NATS streaming server is in use, the sample program below can be used as a template to log the bucket notification added to NATS:

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

Use the following command to execute the program:

```sh
go run nats.go
```

A response similar to this one should be displayed:

```
2017/07/07 11:47:40 Connected
2017/07/07 11:47:40 Subscribing to subject 'bucketevents'
```

Open another console and use the following command to upload a JPEG image into `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console. A response similar to this one should be displayed:

```
Received a message: {"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0",
"eventSource":"minio:s3","awsRegion":"","eventTime":"2017-07-07T18:46:37Z","eventName":"s3:ObjectCreated:Put",
"userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"192.168.1.80:55328"},
"responseElements":{"x-amz-request-id":"14CF20BD1EFD5B93","x-minio-origin-endpoint":"http://127.0.0.1:9000"},
"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},
"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":248682,"eTag":"f1671feacb8bbf7b0397c6e9364e8c92",
"contentType":"image/jpeg","userDefined":{"content-type":"image/jpeg"},"versionId":"1","sequencer":"14CF20BD1EFD5B93"}},
"source":{"host":"192.168.1.80","port":"55328","userAgent":"Minio (linux; amd64) minio-go/2.0.4 mc/DEVELOPMENT.GOGET"}}],
"level":"info","msg":"","time":"2017-07-07T11:46:37-07:00"}
```

## 7. <a name="publish-minio-events-via-postgresql"></a>Publish Minio Events via PostgreSQL

### 7.1 Install PostgreSQL

Minio Server requires PostgreSQL 9.5 or above. Minio Server uses the [`INSERT ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) (UPSERT) feature, introduced in version 9.5 and the [JSONB](https://www.postgresql.org/docs/9.4/static/datatype-json.html) data-type introduced in version 9.4.

Install a PostgreSQL database server using these instructions: [https://www.postgresql.org/](https://www.postgresql.org/).

This notification target supports two formats:

* **namespace**: Minio synchronizes objects in the bucket with rows in the table. It creates rows with two columns: `key` and `value`. The `key` is the bucket and object name that exist in Minio Server. The `value` contains JSON-encoded event data about the operation that created/replaced the object in Minio Server. When objects are updated or deleted, the corresponding row from this table is updated or deleted as well.
* **access**: Minio appends events to a table. It creates rows with two columns: `event_time` and `event_data`. The `event_time` is the time at which the event occurred in Minio Server. The `event_data` contains JSON-encoded event data about the operation on an object. No rows are deleted or modified in this format.

The steps below show how to use this notification target in `namespace` format. The `access` format is very similar and is omitted for brevity.

### 7.2 Add a PostgreSQL Endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format. The PostgreSQL configuration is located in the `postgresql` key under the top-level `notify` key. Create a configuration key-value pair here for the PostgreSQL instance. The key is a name for the PostgreSQL endpoint, and the value is a collection of  parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `format` | `string` | (_Required_) Specifies the format. This field must be set to `namespace` or `access`. |
| `connectionString` | `string` | (Optional) [Connection string parameters](https://godoc.org/github.com/lib/pq#hdr-Connection`string`Parameters) for the PostgreSQL server (e.g. to set `sslmode`). |
| `table` | `string` | (_Required_) The table name in which events will be stored/updated. If the table does not exist, Minio Server creates it at startup.|
| `host` | `string` | (Optional) The host name of the PostgreSQL server. This default value for this field is `localhost`. |
| `port` | `string` | (Optional) The port on which to connect to the PostgreSQL server. The default value for this field is `5432`. |
| `user` | `string` | (Optional) The database user name. The default value for this field is the user running the server process. |
| `password` | `string` | (Optional) The database password. |
| `database` | `string` | (Optional) The database name. |

The following shows an example of a configuration for PostgreSQL:

```json
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

**Note:** For illustrative purposes, the Postgres user password has been set to `password`, the name of the database to store the events has been set to `minio_events`, and SSL has been disabled. Disabling SSL is not recommended for production servers.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the Postgres configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to take effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs:  arn:minio:sqs::1:postgresql
```

**Note:** An arbitrary number of PostgreSQL server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the PostgreSQL instance, and an object name for the per-server configuration parameters.

### 7.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

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

**Note:** The ARN displayed by Minio in the previous step is required to configure this bucket notification. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

When a JPEG image is created/overwritten, a new row is added or an existing row is updated in the PostgreSQL table configured above. When an existing object is deleted, the corresponding row is deleted from the PostgreSQL table. The rows in the PostgreSQL table reflect the `.jpg` objects in the `images` bucket.

### 7.4 Test on PostgreSQL

Open another console and use the following command to upload a JPEG image into `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Open a PostgreSQL console to list the rows in the `bucketevents` table:

```
$ psql -h 127.0.0.1 -U postgres -d minio_events
minio_events=# select * from bucketevents;
```

A response similar to this one should be displayed:

```
key                 |                      value
--------------------+-----------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------
images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}},
"object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId":
"Config", "s3SchemaVersion": "1.0"}, "awsRegion": "", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource":
"aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": 
"[::1]:39706"}}]}
(1 row)
```

## 8.<a name="publish-minio-events-via-mysql"></a> Publish Minio Events via MySQL

### 8.1 Install MySQL

Minio requires MySQL version 5.7.8 or above. Minio uses the [JSON](https://dev.mysql.com/doc/refman/5.7/en/json.html) data-type introduced in version 5.7.8. This configuration has been tested on MySQL 5.7.17.

Install MySQL using these instructions [https://dev.mysql.com/downloads/mysql/](https://dev.mysql.com/downloads/mysql/). 

This notification target supports two formats:
* **namespace**: Minio synchronizes objects in the bucket with rows in the table. It creates rows with two columns: `key_name` and `value`. The `key_name` is the bucket and object name of an object that exists in Minio Server. The value contains JSON-encoded event data about the operation that created/replaced the object in Minio. When objects are updated or deleted, the corresponding row from this table is updated or deleted as well.
* **access**: Minio appends events to a table. It creates rows with two columns: `event_time` and `event_data`. The `event_time` is the time at which the event occurred in Minio Server. The `event_data` contains JSON-encoded event data about the operation on an object. No rows are deleted or modified in this format.

The steps below show how to use this notification target in `namespace` format. The `access` format is very similar and is omitted for brevity.

### 8.2 Add a MySQL Server Endpoint Configuration to Minio

The Minio Server configuration file is stored on the backend in JSON format. The MySQL configuration is located in the `mysql` key under the top-level `notify` key. Create a configuration key-value pair here for the MySQL instance. The key is a name for the MySQL endpoint, and the value is a collection of parameters described in the table below:

| **Parameter** | **Type** | **Description** |
|:---|:---|:---|
| `enable` | `bool` | (_Required_) Specifies if this server endpoint configuration is active/enabled. |
| `format` | `string` | (_Required_) Specifies the format. This field must be set to `namespace` or `access`. |
| `dsnString` | `string` | (Optional) The [Data-Source-Name connection string](https://github.com/go-sql-driver/mysql#dsn-data-source-name) for the MySQL server. If this field is not specified, then the connection information specified for the `host`, `port`, `user`, `password`, and `database` parameters are used. |
| `table` | `string` | (_Required_) The name of the table in which events will be stored/updated. If the table does not exist, Minio Server creates it at startup.|
| `host` | `string` | The host name of the MySQL server. This value is only used if `dsnString` is empty. |
| `port` | `string` | The port on which to connect to the MySQL server. This value is only used if `dsnString` is empty. |
| `user` | `string` | The database user name. This value is only used if `dsnString` is empty. |
| `password` | `string` | The database password. This value is only used if `dsnString` is empty. |
| `database` | `string` | The database name. This value is only used if `dsnString` is empty. |

The following shows an example of a configuration for MySQL:

```json
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

**Note:** The root password has been set to `password` and the name of the database has been set to `miniodb` for illustrative purposes.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the MySQL configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to take effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs:  arn:minio:sqs::1:mysql
```

**Note:** An arbitrary number of MySQL server endpoint configurations can be added by providing an identifier (e.g. `1` in the example above) for the MySQL instance, and an object name for the per-server configuration parameters.

### 8.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add myminio/images arn:minio:sqs::1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
arn:minio:sqs::1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

**Note:** The ARN displayed by Minio in the previous step is required to configure this bucket notification. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

When a JPEG image object is created/overwritten, a new row is added or an existing row is updated in the MySQL table configured above. When an existing object is deleted, the corresponding row is deleted from the MySQL table. The rows in the MySQL table reflect the `.jpg` objects in the `images` bucket.

### 8.4 Test on MySQL

Use the following command to upload a JPEG image into the `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Open MySQL in another console and list the rows in the `minio_images` table:

```sh
$ mysql -h 172.17.0.1 -P 3306 -u root -p miniodb
mysql> select * from minio_images;
```

A response similar to this one should be displayed:

```
+--------------------+----------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
----------------------------+
| key_name           | value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+--------------------+----------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
----------------------------+
| images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": 
"minio"}}, "object": {"key": "myphoto.jpg", "eTag": "467886be95c8ecfd71a2900e3f461b4f", "size": 26, "sequencer": "14AC59476F809FD3"},
"configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "", "eventName": "s3:ObjectCreated:Put", "eventTime": "2017-03-16T11:29:00Z",
"eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {"x-amz-request-id":
"14AC59476F809FD3", "x-minio-origin-endpoint": "http://192.168.86.110:9000"}, "requestParameters": {"sourceIPAddress": "127.0.0.1:38260"}}]} |
+--------------------+----------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)


```

## 9. <a name="publish-minio-events-via-kafka"></a>Publish Minio Events via Kafka

### 9.1 Install Apache Kafka

Minio requires Kafka version 0.9 or 0.10. Internally, Minio uses the [Shopify/sarama](https://github.com/Shopify/sarama/) library and has the same version compatibility as provided by this library.

Install Apache Kafka using these instructions: [http://kafka.apache.org/](http://kafka.apache.org/).

### 9.2 Add Kafka endpoint to Minio

The Minio Server configuration file is stored on the backend in JSON format.

The following shows an example of a configuration in `config.json` for Kafka:

```json
"kafka": {
    "1": {
        "enable": true,
        "brokers": ["localhost:9092"],
        "topic": "bucketevents"
    }
}
```

In this example, `bucketevents` is the topic used by Kafka.

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the Kafka configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart the Minio server for the changes to take effect. A response similar to this one should be displayed at startup if there were no errors:

```
SQS ARNs:  arn:minio:sqs::1:kafka
``` 

### 9.3 Enable Bucket Notifications Using Minio Client

Use the following commands to enable bucket event notifications on a bucket named `images` for a Minio Server instance aliased as `myminio`:

```sh
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs::1:kafka --suffix .jpg
mc events list myminio/images
arn:minio:sqs::1:kafka s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

In this example, the ARN value is `arn:minio:sqs::1:kafka`. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

This enables a bucket event notification to trigger when a JPEG image is uploaded or deleted from `images` bucket on ``myminio`` server.

### 9.4 Test on Kafka

The following examples use [kafkacat](https://github.com/edenhill/kafkacat) to display all notifications on the console:

```sh
kafkacat -C -b localhost:9092 -t bucketevents
```

Open another console and use the following command to upload a JPEG image into the `images` bucket:

```sh
mc cp myphoto.jpg myminio/images
```

Once the upload completes, return to the first console and invoke `kafkacat` to display the event notification to the console:

```sh
kafkacat -b localhost:9092 -t bucketevents
```

A response similar to this one should be displayed:

```json
{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"",
"eventTime":"2017-01-31T10:01:51Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},
"requestParameters":{"sourceIPAddress":"192.173.5.2:57904"},"responseElements":{"x-amz-request-id":"149ED2FD25589220",
"x-minio-origin-endpoint":"http://192.173.5.2:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":
{"name":"images","ownerIdentity":{"principalId":"88QR09S7IOT4X1IBAQ9B"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg",
"size":541596,"eTag":"04451d05b4faf4d62f3d538156115e2a","sequencer":"149ED2FD25589220"}}}],"level":"info","msg":"","time":"2017-01-31T15:31:51+05:30"}
```

## 10. <a name="publish-minio-events-via-webhooks"></a> Publish Minio Events via Webhooks

[Webhooks](https://en.wikipedia.org/wiki/Webhook) are a way to receive information when it happens, rather than continually polling for that data.

### 10.1 Add a Webhook Endpoint to Minio Server

The Minio Server configuration file is stored on the backend in JSON format. 

The following is an example webhook in ``config.json``:

```json
"webhook": {
  "1": {
    "enable": true,
    "endpoint": "http://localhost:3000/"
}
```

To update the configuration, use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the webhook configuration in `/tmp/myconfig`, use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

The endpoint is the server listening for webhook notifications. Save the file and restart Minio Server for the changes to take effect. 

**Note:** The endpoint needs to be live and reachable when Minio Server is restarted.

### 10.2 Enable Bucket Notifications Using Minio Client

The following example enables a bucket event notification to trigger when a JPEG image is uploaded from the `images` bucket on the `myminio` server:

```sh
mc mb myminio/images
mc mb myminio/images-thumbnail
mc events add myminio/images arn:minio:sqs::1:webhook --events put --suffix .jpg
```

In this example, the ARN value is `arn:minio:sqs::1:amqp`. For more information about ARN see the [AWS ARN documentation](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

Use the following command to check if the event notification was successful:

```sh
mc events list myminio/images
```

A response similar to this one should be displayed:

```
arn:minio:sqs::1:webhook   s3:ObjectCreated:*   Filter: suffix=".jpg"
```

### 10.3 Test with Thumbnailer

[Thumbnailer](https://github.com/minio/thumbnailer) can be used to listen for Minio Server notifications when a new JPEG file is uploaded using HTTP PUT. Thumbnailer uploads the thumbnail of a new image to Minio Server when a notification occurs.

Use the following commands to download and install Thumbnailer:

```sh
git clone https://github.com/minio/thumbnailer/
npm install
```

Open the Thumbnailer configuration file located in ``config/webhook.json`` and add the configuration for Minio Server.

Use the following command to run Thumbnailer at `http://localhost:3000/`:

```sh
NODE_ENV=webhook node thumbnail-webhook.js
```

Configure Minio Server to send notifications to this URL (as mentioned in step 1) and use ``mc`` to set up bucket notifications (as mentioned in step 2).

Use the following command to upload a JPEG image to Minio Server:

```sh
mc cp ~/images.jpg myminio/images
```

A response similar to this one should be displayed:

```sh
.../images.jpg:  8.31 KB / 8.31 KB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 100.00% 59.42 KB/s 0s
```

Wait a few moments and then use the following command to check the bucket’s contents: 

```sh
mc ls myminio/images-thumbnail
```

A response similar to this one should be displayed that includes the uploaded thumbnail:

```
[2017-02-08 11:39:40 IST]   992B images-thumbnail.jpg
```

**Note:** For [distributed Minio](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` on all of the nodes with the backend configuration for bucket event notifications.
