# Publish Minio events via Elasticsearch [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Minio](https://www.minio.io) server supports Amazon S3 compatible bucket event notifications. In this recipe we will learn how to set up bucket notifications using [Elasticsearch](https://www.elastic.co/) server. 

## 1. Prerequisites

* Install and configure Minio Server from [here](http://docs.minio.io/docs/minio).
* Install and configure Minio Client from [here](https://docs.minio.io/docs/minio-client-quickstart-guide).


## 2. Installation

Install Elasticsearch 2.4 from [here](https://www.elastic.co/downloads/past-releases/elasticsearch-2-4-0).

## 3. Recipe steps

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
Restart the Minio server to reflect config changes made above. ``bucketevents`` is the index used by Elasticsearch.

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
