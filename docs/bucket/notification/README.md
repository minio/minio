# Minio Event Notification

Minio object storage server supports [AWS S3 SQS/SNS style](http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) event notification for object create and delete operations. Notifications can be received through AMQP, Elasticsearch and Redis targets.

## Supported Events

Currently, Minio can publish the following events:

- _ObjectCreated_ event is generated whenever a new object is created.

| Event types| Description|
|:-----------|:-----------|
|_s3:ObjectCreated:*_| All object create operations.|
|_s3:ObjectCreated:Put_| Object created via PUT method. (Most commonly used by SDKs)|
|_s3:ObjectCreated:Post_| Object created via POST method. (Most commonly used by browser)|
|_s3:ObjectCreated:Copy_| Object created via server side copy operation.|
|_s3:ObjectCreated:CompleteMultipartUpload_| Object created via Multipart API. (Most commonly used for large objects)|

- _ObjectRemoved_ event is generated whenever an object is deleted.

| Event types| Description|
|:-----------|:-----------|
|_s3:ObjectRemoved:*_| All delete operations.|
|_s3:ObjectRemoved:Delete_||

## Supported Targets

Minio can send bucket notifications to one or more of the following supported targets. The target is identified uniquely through an unique resource identifier called [ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

### AMQP via Simple Queue Service (SQS)

The Advanced Message Queuing Protocol (AMQP) is an open standard application layer protocol for message-oriented middleware. The defining features of AMQP are message orientation, queuing, routing (including point-to-point and publish-and-subscribe), reliability and security.

Configure `~/.minio/config.json` with your AMQP config.

```json
	"notify": {
		"amqp": {
			"1": {
				"enable": true,
				"url": "amqp://minio:minio@localhost:5672/",
				"exchange": "minio1_direct",
				"routingKey": "minio1_direct",
				"exchangeType": "direct",
				"mandatory": false,
				"immediate": false,
				"durable": true,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
        }
```

### Elasticsearch via Simple Queueing Service (SQS)

Elasticsearch is a search engine based on Lucene. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents. To know more details about configuration please read [Minio server configuration guide](https://docs.minio.io/docs/minio-server-configuration-files-guide)

Configure `~/.minio/config.json` with your ElasticSearch config.

```json
	"notify": {
		"elasticsearch": {
			"1": {
				"enable": true,
				"url": "http://localhost:9200",
                                "index": "minio"
			}
		},
        }
```

### Redis via Simple Queueing Service (SQS)

Redis is a data structure server. It is open-source, networked, in-memory, and stores keys with optional durability. To know more details about configuration please read [Minio server configuration guide](https://docs.minio.io/docs/minio-server-configuration-files-guide)

Configure `~/.minio/config.json` with your Redis config.

```json
	"notify": {
		"redis": {
			"1": {
				"enable": true,
				"address": "localhost:6379",
                                "password": "minio",
                                "key": "minio1"
			}
		},
        }
```

### Listen via Simple Notification Service (SNS)

Applications may directly listen for events using an extended [ListenBucketNotification RESTful API](https://docs.minio.io/docs/golang-client-api-reference#ListenBucketNotification).

You may also look at [mc watch](https://docs.minio.io/docs/minio-client-complete-guide#watch) command to write useful shell scripts around bucket events.

## Understanding Resource Names (ARN)

Describe

```
SQS ARNs:   arn:minio:sqs:us-east-1:1:amqp  arn:minio:sqs:us-east-1:1:redis
```

## Enable Bucket Notification

You can use any of the following methods to enable/disable bucket notifications:

- Minio Client command [mc events --help](https://docs.minio.io/docs/minio-client-complete-guide#events)

- Minio Client SDKs [SetBucketNotification() API](https://docs.minio.io/docs/golang-client-api-reference#SetBucketNotification)
