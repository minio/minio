# Minio存储桶通知指南 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

存储桶（Bucket）如果发生改变,比如上传对象和删除对象，可以使用存储桶事件通知机制进行监控，并发布到以下目标:

| Notification Targets|
|:---|
| [`AMQP`](#AMQP) |
| [`MQTT`](#MQTT) |
| [`Elasticsearch`](#Elasticsearch) |
| [`Redis`](#Redis) |
| [`NATS`](#NATS) |
| [`PostgreSQL`](#PostgreSQL) |
| [`MySQL`](#MySQL) |
| [`Apache Kafka`](#apache-kafka) |
| [`Webhooks`](#webhooks) |

## 前提条件

* 从 [这里](http://docs.minio.io/docs/zh_CN/minio-quickstart-guide)下载并安装Minio Server.
* 从[这里](https://docs.minio.io/docs/zh_CN/minio-client-quickstart-guide)下载并安装Minio Client.

<a name="AMQP"></a>
## 使用AMQP发布Minio事件

从[这里](https://www.rabbitmq.com/)下载安装RabbitMQ。

### 第一步: 将AMQP endpoint添加到Minio

Minio Server的配置文件默认路径是 ``~/.minio/config.json``。AMQP配置信息是在`notify`这个节点下的`amqp`节点下，在这里为你的AMQP实例创建配置信息键值对，key是你的AMQP endpoint的名称，value是下面表格中列列的键值对集合。

| 参数 | 类型 | 描述 |
|:---|:---|:---|
| `enable` | _bool_ | (必须) 此AMQP server endpoint是否可用 |
| `url` | _string_ | (必须) AMQP server endpoint, 例如. `amqp://myuser:mypassword@localhost:5672` |
| `exchange` | _string_ | exchange名称 |
| `routingKey` | _string_ | 发布用的Routing key  |
| `exchangeType` | _string_ | exchange类型 |
| `deliveryMode` | _uint8_ | 发布方式。 0或1 - 瞬态; 2 - 持久。|
| `mandatory` | _bool_ | Publishing related bool. |
| `immediate` | _bool_ | Publishing related bool. |
| `durable` | _bool_ | Exchange declaration related bool. |
| `internal` | _bool_ | Exchange declaration related bool. |
| `noWait` | _bool_ | Exchange declaration related bool. |
| `autoDeleted` | _bool_ | Exchange declaration related bool. |

下面展示的是RabbitMQ的配置示例:

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

更新完配置文件后，重启Minio Server让配置生效。如果一切顺利，Minio Server会在启动时输出一行信息，类似 `SQS ARNs:  arn:minio:sqs:us-east-1:1:amqp`。

Minio支持[RabbitMQ](https://www.rabbitmq.com/)中所有的交换方式，这次我们采用  ``fanout`` 交换。

注意一下，你可以听从你内心的想法，想配几个AMQP服务就配几个，只要每个AMQP服务实例有不同的ID (比如前面示例中的"1") 和配置信息。


### 第二步: 使用Minio客户端启用bucket通知

如果一个JPEG图片上传到``myminio`` server里的``images`` 存储桶或者从桶中删除，一个存储桶事件通知就会被触发。 这里ARN值是``arn:minio:sqs:us-east-1:1:amqp``，想了解更多关于ARN的信息，请参考[AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:amqp --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步:在RabbitMQ上进行验证

下面将要出场的python程序会等待队列交换T``bucketevents``并在控制台中输出事件通知。我们使用的是[Pika Python Client](https://www.rabbitmq.com/tutorials/tutorial-three-python.html) 来实现此功能。

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

执行示例中的python程序来观察RabbitMQ事件。

```py
python rabbit.py
```

另开一个terminal终端并上传一张JPEG图片到``images``存储桶。

```
mc cp myphoto.jpg myminio/images
```

一旦上传完毕，你应该会通过RabbitMQ收到下面的事件通知。

```py
python rabbit.py
‘{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”us-east-1",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}\n
```

<a name="MQTT"></a>
## 使用MQTT发布Minio事件

从 [这里](https://mosquitto.org/)安装MQTT Broker。

### 第一步: 添加MQTT endpoint到Minio

Minio Server的配置文件默认路径是 ``~/.minio/config.json``。MQTT配置信息是在`notify`这个节点下的`mqtt`节点下，在这里为你的MQTT实例创建配置信息键值对，key是你的MQTT endpoint的名称，value是下面表格中列列的键值对集合。 


| 参数 | 类型 | 描述 |
|:---|:---|:---|
| `enable` | _bool_ | (必须) 这个 server endpoint是否可用? |
| `broker` | _string_ | (必须) MQTT server endpoint, 例如. `tcp://localhost:1883` |
| `topic` | _string_ | (必须) 要发布的MQTT主题的名称, 例如. `minio` |
| `qos` | _int_ | 设置服务质量级别 |
| `clientId` | _string_ | MQTT代理识别Minio的唯一ID |
| `username` | _string_ | 连接MQTT server的用户名 (如果需要的话) |
| `password` | _string_ | 链接MQTT server的密码 (如果需要的话) |

以下是一个MQTT的配置示例:

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

更新完配置文件后，重启Minio Server让配置生效。如果一切顺利，Minio Server会在启动时输出一行信息，类似 `SQS ARNs:  arn:minio:sqs:us-east-1:1:mqtt`。

Minio支持任何支持MQTT 3.1或3.1.1的MQTT服务器，并且可以通过TCP，TLS或Websocket连接使用``tcp://``, ``tls://``, or ``ws://``分别作为代理URL的方案。 更多信息，请参考 [Go Client](http://www.eclipse.org/paho/clients/golang/)。

注意一下，你还是和之前AMQP一样可以听从你内心的想法，想配几个MQTT服务就配几个，只要每个MQTT服务实例有不同的ID (比如前面示例中的"1") 和配置信息。


### 第二步: 使用Minio客户端启用bucket通知

如果一个JPEG图片上传到``myminio`` server里的``images`` 存储桶或者从桶中删除，一个存储桶事件通知就会被触发。 这里ARN值是``arn:minio:sqs:us-east-1:1:mqtt``。

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:mqtt --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步：验证MQTT

下面的python程序等待mqtt主题``/ minio``，并在控制台上打印事件通知。 我们使用[paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/)库来执行此操作。

```py
#!/usr/bin/env python
from __future__ import print_function
import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("/minio")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost:1883", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
```

执行这个python示例程序来观察MQTT事件。

```py
python mqtt.py
```

打开一个新的terminal终端并上传一张JPEG图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

一旦上传完毕，你应该会通过MQTT收到下面的事件通知。

```py
python mqtt.py
{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”us-east-1",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}
```

<a name="Elasticsearch"></a>
## 使用Elasticsearch发布Minio事件

安装 [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) 。

这个通知目标支持两种格式: _namespace_ and _access_.

如果使用的是 _namespace_ 格式, Minio将桶中的对象与索引中的文档进行同步。对于Minio的每一个事件，ES都会创建一个document,这个document的ID就是存储桶以及存储对象的名称。事件的其他细节存储在document的正文中。因此，如果一个已经存在的对象在Minio中被覆盖，在ES中的相对应的document也会被更新。如果一个对象被删除，相对应的document也会从index中删除。

如果使用的是_access_格式，Minio将事件作为document加到ES的index中。对于每一个事件，ES同样会创建一个document,这个document包含事件的所有细节，document的时间戳设置为事件的时间戳，并将该document加到ES的index中。这个document的ID是由ES随机生成的。在_access_格式下，没有文档会被删除或者修改，对于一个对象的操作，都会生成新的document附加到index中。

下面的步骤展示的是在`namespace`格式下，如何使用通知目标。另一种格式和这个很类似，为了不让你们说我墨迹，就不再赘述了。


### 第一步：确保至少满足第低要求

Minio要求使用的是ES 5.X系统版本。如果使用的是低版本的ES，也没关系，ES官方支持升级迁移，详情请看[这里](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html)。

### 第二步：把ES集成到Minio中

Minio Server的配置文件默认路径是 ``~/.minio/config.json``。ES配置信息是在`notify`这个节点下的`elasticsearch`节点下，在这里为你的ES实例创建配置信息键值对，key是你的ES的名称，value是下面表格中列列的键值对集合。 

| 参数 | 类型 | 描述 |
|:---|:---|:---|
| `enable` | _bool_ | (必须) 是否启用这个配置? |
| `format` | _string_ | (必须)  是`namespace` 还是 `access` |
| `url` | _string_ | (必须) ES地址，比如: `http://localhost:9200` |
| `index` | _string_ | (必须) 给Minio用的index |

以下是ES的一个配置示例:

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

更新完配置文件后，重启Minio Server让配置生效。如果一切顺利，Minio Server会在启动时输出一行信息，类似 `SQS ARNs:  arn:minio:sqs:us-east-1:1:elasticsearch`。

注意一下，你又可以再一次听从你内心的想法，想配几个ES服务就配几个，只要每个ES服务实例有不同的ID (比如前面示例中的"1") 和配置信息。

### 第三步：使用Minio客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知。一旦有文件被创建或者覆盖，一个新的ES的document会被创建或者更新到之前咱配的index里。如果一个已经存在的对象被删除，这个对应的document也会从index中删除。因此，这个ES index里的行，就映射着`images`存储桶里的对象。

要配置这种存储桶通知，我们需要用到前面步骤Minio输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设咱们的Minio服务别名叫`myminio`,可执行下列脚本：

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:elasticsearch --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第四步：验证ES

上传一张图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

使用curl查到``minio_events`` index中的内容。

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
              "awsRegion" : "us-east-1",
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

这个输出显示在ES中为这个事件创建了一个document。

这里我们可以看到这个document ID就是存储桶和对象的名称。如果用的是`access`格式，这个document ID就是由ES随机生成的。

<a name="Redis"></a>
## 使用Redis发布Minio事件

安装 [Redis](http://redis.io/download)。为了演示，我们将数据库密码设为"yoursecret"。

这咱通知目标支持两种格式: _namespace_ 和 _access_。

如果用的是_namespacee_格式，Minio将存储桶里的对象同步成Redis hash中的条目。对于每一个条目，对对应一个存储桶里的对象，其key都被设为"存储桶名称/对象名称"，value都是一个有关这个Minio对象的JSON格式的事件数据。如果对象更新或者删除，hash中对象的条目也会相应的更新或者删除。

如果使用的是_access_,Minio使用[RPUSH](https://redis.io/commands/rpush)将事件添加到list中。这个list中每一个元素都是一个JSON格式的list,这个list中又有两个元素，第一个元素是时间戳的字符串，第二个元素是一个含有在这个存储桶上进行操作的事件数据的JSON对象。在这种格式下，list中的元素不会更更新或者删除。

下面的步骤展示的是如何在`namespace`和`access`格式下使用通知目标。

### 第一步：集成Redis到Minio

Minio Server的配置文件默认路径是 ``~/.minio/config.json``。Redis配置信息是在`notify`这个节点下的`redis`节点下，在这里为你的Redis实例创建配置信息键值对，key是你的Redis的名称，value是下面表格中列列的键值对集合。 

| 参数 | 类型 | 描述 |
|:---|:---|:---|
| `enable` | _bool_ | (必须) 这个配置是否可用? |
| `format` | _string_ | (必须) 是 `namespace` 还是 `access` |
| `address` | _string_ | (必须) Redis服务地址，比如: `localhost:6379` |
| `password` | _string_ | (可选) Redis服务密码 |
| `key` | _string_ | (必须) 事件要存储到redis key的名称。如果用的是`namespace`格式的话，则是一个hash,如果是`access`格式的话，则是一个list|

下面是一个Redis配置示例:

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

更新完配置文件后，重启Minio Server让配置生效。如果一切顺利，Minio Server会在启动时输出一行信息，类似 `SQS ARNs:  arn:minio:sqs:us-east-1:1:redis`。

注意一下，你永远都可以听从你内心的想法，想配几个Redis服务就配几个，只要每个Redis服务实例有不同的ID (比如前面示例中的"1") 和配置信息。

### 第二步: 使用Minio客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知。一旦有文件被创建或者覆盖，一个新的key会被创建,或者一个已经存在的key就会被更新到之前咱配的redis hash里。如果一个已经存在的对象被删除，这个对应的key也会从hash中删除。因此，这个Redis hash里的行，就映射着`images`存储桶里的对象。

要配置这种存储桶通知，我们需要用到前面步骤Minio输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设咱们的Minio服务别名叫`myminio`,可执行下列脚本：

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:redis --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步：验证Redis

启动`redis-cli`这个Redis客户端程序来检查Redis中的内容. 运行`monitor`Redis命令。 这将打印在Redis上执行的每个操作。

```
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
```

打开一个新的terminal终端并上传一张JPEG图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

在上一个终端中，你将看到Minio在Redis上执行的操作：

```
127.0.0.1:6379> monitor
OK
1490686879.650649 [0 172.17.0.1:44710] "PING"
1490686879.651061 [0 172.17.0.1:44710] "HSET" "minio_events" "images/myphoto.jpg" "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"minio:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2017-03-28T07:41:19Z\",\"eventName\":\"s3:ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1:52234\"},\"responseElements\":{\"x-amz-request-id\":\"14AFFBD1ACE5F632\",\"x-minio-origin-endpoint\":\"http://192.168.86.115:9000\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":2586,\"eTag\":\"5d284463f9da279f060f0ea4d11af098\",\"sequencer\":\"14AFFBD1ACE5F632\"}},\"source\":{\"host\":\"127.0.0.1\",\"port\":\"52234\",\"userAgent\":\"Minio (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z\"}}]}"
```

在这我看看到了Minio在`minio_events`这个key上执行了`HSET`命令。

如果用的是`access`格式，那么`minio_events`就是一个list,Minio就会调用`RPUSH`添加到list中。这个list的消费者会使用`BLPOP`从list的最左端删除list元素。

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

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:nats``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:nats --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
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
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2016-10-12T13:51:33Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"","time":"2016-10-12T06:51:33-07:00"}
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

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs:us-east-1:1:postgresql` at start-up if there were no errors.

Note that, you can add as many PostgreSQL server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the PostgreSQL instance and an object of per-server configuration parameters.


### Step 3: Enable bucket notification using Minio client

We will now enable bucket event notifications on a bucket named `images`. Whenever a JPEG image is created/overwritten, a new row is added or an existing row is updated in the PostgreSQL configured above. When an existing object is deleted, the corresponding row is deleted from the PostgreSQL table. Thus, the rows in the PostgreSQL table, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add myminio/images arn:minio:sqs:us-east-1:1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
mc events list myminio/images
arn:minio:sqs:us-east-1:1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
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
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
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

After updating the configuration file, restart the Minio server to put the changes into effect. The server will print a line like `SQS ARNs:  arn:minio:sqs:us-east-1:1:mysql` at start-up if there were no errors.

Note that, you can add as many MySQL server endpoint configurations as needed by providing an identifier (like "1" in the example above) for the MySQL instance and an object of per-server configuration parameters.


### Step 3: Enable bucket notification using Minio client

We will now setup bucket notifications on a bucket named `images`. Whenever a JPEG image object is created/overwritten, a new row is added or an existing row is updated in the MySQL table configured above. When an existing object is deleted, the corresponding row is deleted from the MySQL table. Thus, the rows in the MySQL table, reflect the `.jpg` objects in the `images` bucket.

To configure this bucket notification, we need the ARN printed by Minio in the previous step. Additional information about ARN is available [here](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html).

With the `mc` tool, the configuration is very simple to add. Let us say that the Minio server is aliased as `myminio` in our mc configuration. Execute the following:

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc events add myminio/images arn:minio:sqs:us-east-1:1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc events list myminio/images
arn:minio:sqs:us-east-1:1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
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
| images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "467886be95c8ecfd71a2900e3f461b4f", "size": 26, "sequencer": "14AC59476F809FD3"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "us-east-1", "eventName": "s3:ObjectCreated:Put", "eventTime": "2017-03-16T11:29:00Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {"x-amz-request-id": "14AC59476F809FD3", "x-minio-origin-endpoint": "http://192.168.86.110:9000"}, "requestParameters": {"sourceIPAddress": "127.0.0.1:38260"}}]} |
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

We will enable bucket event notification to trigger whenever a JPEG image is uploaded or deleted from ``images`` bucket on ``myminio`` server. Here ARN value is ``arn:minio:sqs:us-east-1:1:kafka``. To understand more about ARN please follow [AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) documentation.

```
mc mb myminio/images
mc events add  myminio/images arn:minio:sqs:us-east-1:1:kafka --suffix .jpg
mc events list myminio/images
arn:minio:sqs:us-east-1:1:kafka s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
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
mc events add myminio/images arn:minio:sqs:us-east-1:1:webhook --events put --suffix .jpg
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
