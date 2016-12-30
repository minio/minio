# Publish Minio events via Redis [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Minio](https://www.minio.io) server supports Amazon S3 compatible bucket event notification. In this recipe we will learn how to set up bucket notifications using [Redis](http://redis.io/documentation).

## 1. Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).


## 2. Installation

Install Redis from [here](http://redis.io/download).

## 3. Recipe steps

### Step 1: Add Redis endpoint to Minio

The default location of Minio server configuration file is ``~/.minio/config.json``. Update the Redis configuration block in ``config.json`` as follows.

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
Restart the Minio server to reflect config changes made above. ``bucketevents`` is the key used by Redis in this example.

If you are running Minio in [distributed setup](https://docs.minio.io/docs/distributed-minio-quickstart-guide), modify ``~/.minio/config.json`` with these local changes on all the nodes.

### Step 2: Enable bucket notification using Minio client

In this recipe  we will enable bucket events only when JPEG images are uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:redis``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### Step 3: Testing at Redis

Redis comes with handy command line interface, redis-cli. It uses ``monitor`` command to print all notification on console.

```
redis-cli -a yoursecret
```

Open another terminal and upload a JPEG image into ``images`` bucket.

```
mc cp myphoto.jpg myminio/images
```

Below ``redis-cli`` prints event notification on console.

```
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
1474321638.556108 [0 127.0.0.1:40190] "AUTH" "yoursecret"
1474321638.556477 [0 127.0.0.1:40190] "RPUSH" "bucketevents" "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2016-09-19T21:47:18.555Z\",\"eventName\":\"s3:ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"[::1]:39250\"},\"responseElements\":{},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":23745,\"sequencer\":\"1475D7B80ECBD853\"}}}],\"level\":\"info\",\"msg\":\"\",\"time\":\"2016-09-19T14:47:18-07:00\"}\n"
```
