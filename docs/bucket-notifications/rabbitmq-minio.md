# Publish Minio events via RabbitMQ [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

[Minio](https://www.minio.io) server supports Amazon S3 compatible bucket event notifications. In this recipe we will learn how to set up bucket notifications using [RabbitMQ](https://www.rabbitmq.com/) server. 

## 1. Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).


## 2. Installation

Install RabbitMQ from [here](https://www.rabbitmq.com/).

## 3. Recipe steps

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

