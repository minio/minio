# Publish Minio events via PostgreSQL [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Minio](https://www.minio.io) server supports Amazon S3 compatible bucket event notifications. In this recipe we will learn how to set up bucket notifications using [PostgreSQL](https://www.postgresql.org/).

## 1. Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).


## 2. Installation

Install PostgreSQL from [here](https://www.postgresql.org/).

## 3. Recipe steps

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
Restart the Minio server to reflect config changes made above. ``bucketevents`` is the database table used by PostgreSQL in this example.

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

Below PostgreSQL terminal lists table ``bucketevents`` stored log of event notification.

```
bucketevents_db=# select * from bucketevents;

key         |                      value                                                                                                                                                                                                                                                                                                
--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
(1 row)
```	
