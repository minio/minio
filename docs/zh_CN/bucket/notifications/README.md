# MinIO存储桶通知指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

可以使用存储桶事件通知来监视存储桶中对象上发生的事件。 MinIO服务器支持的事件类型是

| Supported Event Types   |                                            |                          |
| :---------------------- | ------------------------------------------ | ------------------------ |
| `s3:ObjectCreated:Put`  | `s3:ObjectCreated:CompleteMultipartUpload` | `s3:ObjectAccessed:Head` |
| `s3:ObjectCreated:Post` | `s3:ObjectRemoved:Delete`                  |                          |
| `s3:ObjectCreated:Copy` | `s3:ObjectAccessed:Get`                    |                          |

使用诸如`mc`之类的客户端工具通过[`event`子命令](https://docs.min.io/cn/minio-client-complete-guide#events)设置和监听事件通知。也可以使用MinIO SDK [`BucketNotification` APIs](https://docs.min.io/cn/golang-client-api-reference#SetBucketNotification) 。MinIO发送的用于发布事件的通知消息是JSON格式的，JSON结构参考[这里](https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html)。

存储桶事件可以发布到以下目标：

| 支持的通知目标    |                             |                                 |
| :-------------------------------- | --------------------------- | ------------------------------- |
| [`AMQP`](#AMQP)                   | [`Redis`](#Redis)           | [`MySQL`](#MySQL)               |
| [`MQTT`](#MQTT)                   | [`NATS`](#NATS)             | [`Apache Kafka`](#apache-kafka) |
| [`Elasticsearch`](#Elasticsearch) | [`PostgreSQL`](#PostgreSQL) | [`Webhooks`](#webhooks)         |
| [`NSQ`](#NSQ)                     |                             |                                 |

## 前提条件

* 从[这里](https://docs.min.io/cn/minio-quickstart-guide)下载并安装MinIO Server。
* 从[这里](https://docs.min.io/cn/minio-client-quickstart-guide)下载并安装MinIO Client。

```
$ mc admin config get myminio | grep notify
notify_webhook        publish bucket notifications to webhook endpoints
notify_amqp           publish bucket notifications to AMQP endpoints
notify_kafka          publish bucket notifications to Kafka endpoints
notify_mqtt           publish bucket notifications to MQTT endpoints
notify_nats           publish bucket notifications to NATS endpoints
notify_nsq            publish bucket notifications to NSQ endpoints
notify_mysql          publish bucket notifications to MySQL databases
notify_postgres       publish bucket notifications to Postgres databases
notify_elasticsearch  publish bucket notifications to Elasticsearch endpoints
notify_redis          publish bucket notifications to Redis datastores
```

> 注意:
> - '\*' 结尾的参数是必填的.
> - '\*' 结尾的值，是参数的的默认值.
> - 当通过环境变量配置的时候, `:name` 可以通过这样 `MINIO_NOTIFY_WEBHOOK_ENABLE_<name>` 的格式指定.

<a name="AMQP"></a>
## 使用AMQP发布MinIO事件

从[这里](https://www.rabbitmq.com/)下载安装RabbitMQ。

### 第一步: 将AMQP endpoint添加到MinIO

AMQP的配置信息位于`notify_amqp`这个顶级的key下。在这里为你的AMQP实例创建配置信息键值对。key是你的AMQP endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_amqp[:name]  发布存储桶通知到AMQP endpoints

ARGS:
url*           (url)       AMQP server endpoint, 例如. `amqp://myuser:mypassword@localhost:5672`
exchange       (string)    AMQP exchange名称
exchange_type  (string)    AMQP exchange类型
routing_key    (string)    发布用的routing key
mandatory      (on|off)    当设置为'off'的时候,忽略未发送的消息(默默的)，默认是 'on'
durable        (on|off)    当设置为'on'的时候，表示持久化队列，broker重启后也会存在, 默认是 'off'
no_wait        (on|off)    当设置为'on'的时候，传递非阻塞的消息， 默认是 'off'
internal       (on|off)    设置为'on'表示exchange是rabbitmq内部使用
auto_deleted   (on|off)    当没有使用者时，设置为'on'时自动删除队列
delivery_mode  (number)    '1'代表非持久队列，'2'代表持久队列
queue_dir      (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit    (number)    未发送消息的最大限制, 默认是'100000'
comment        (sentence)  可选的注释
```

或者通过环境变量(配置说明参考上面)

```
KEY:
notify_amqp[:name]  publish bucket notifications to AMQP endpoints

ARGS:
MINIO_NOTIFY_AMQP_ENABLE*        (on|off)    enable notify_amqp target, default is 'off'
MINIO_NOTIFY_AMQP_URL*           (url)       AMQP server endpoint e.g. `amqp://myuser:mypassword@localhost:5672`
MINIO_NOTIFY_AMQP_EXCHANGE       (string)    name of the AMQP exchange
MINIO_NOTIFY_AMQP_EXCHANGE_TYPE  (string)    AMQP exchange type
MINIO_NOTIFY_AMQP_ROUTING_KEY    (string)    routing key for publishing
MINIO_NOTIFY_AMQP_MANDATORY      (on|off)    quietly ignore undelivered messages when set to 'off', default is 'on'
MINIO_NOTIFY_AMQP_DURABLE        (on|off)    persist queue across broker restarts when set to 'on', default is 'off'
MINIO_NOTIFY_AMQP_NO_WAIT        (on|off)    non-blocking message delivery when set to 'on', default is 'off'
MINIO_NOTIFY_AMQP_INTERNAL       (on|off)    set to 'on' for exchange to be not used directly by publishers, but only when bound to other exchanges
MINIO_NOTIFY_AMQP_AUTO_DELETED   (on|off)    auto delete queue when set to 'on', when there are no consumers
MINIO_NOTIFY_AMQP_DELIVERY_MODE  (number)    set to '1' for non-persistent or '2' for persistent queue
MINIO_NOTIFY_AMQP_QUEUE_DIR      (path)      staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_AMQP_QUEUE_LIMIT    (number)    maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_AMQP_COMMENT        (sentence)  optionally add a comment to this setting
```

MinIO支持持久事件存储。持久存储将在AMQP broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

更新配置前, 可以使用`mc admin config get notify_amqp`命令获取`notify_amqp`的当前配置.

```sh
$ mc admin config get myminio/ notify_amqp
notify_amqp:1 delivery_mode="0" exchange_type="" no_wait="off" queue_dir="" queue_limit="0"  url="" auto_deleted="off" durable="off" exchange="" internal="off" mandatory="off" routing_key=""
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::1:amqp`。

RabbitMQ的示例配置如下所示：

```sh
$ mc admin config set myminio/ notify_amqp:1 exchange="bucketevents" exchange_type="fanout" mandatory="false" no_wait="false"  url="amqp://myuser:mypassword@localhost:5672" auto_deleted="false" delivery_mode="0" durable="false" internal="false" routing_key="bucketlogs"
```

MinIO支持[RabbitMQ](https://www.rabbitmq.com/)中所有的exchange类型，这次我们采用  `fanout` exchange。

请注意, 根据你的需要，你可以添加任意多个AMQP server endpoint，只要提供AMQP实例的标识符（如上例中的“ 1”）和每个实例配置参数的信息即可。

### 第二步: 使用MinIO客户端启用bucket通知

如果一个JPEG图片上传到`myminio` server里的`images` 存储桶或者从桶中删除，一个存储桶事件通知就会被触发。 这里ARN值是`arn:minio:sqs:us-east-1:1:amqp`，想了解更多关于ARN的信息，请参考[AWS ARN](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) 文档.

```
mc mb myminio/images
mc event add myminio/images arn:minio:sqs::1:amqp --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步:在RabbitMQ上进行验证

下面将要出场的python程序会在exchange `bucketevents` 上等待队列,并在控制台中输出事件通知。我们使用的是[Pika Python Client](https://www.rabbitmq.com/tutorials/tutorial-three-python.html) 来实现此功能。

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
## 使用MQTT发布MinIO事件

从 [这里](https://mosquitto.org/)安装MQTT Broker。

### 第一步: 添加MQTT endpoint到MinIO

MQTT的配置信息位于`notify_mqtt`这个顶级的key下。在这里为你的MQTT实例创建配置信息键值对。key是你的MQTT endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_mqtt[:name]  发布存储桶通知到MQTT endpoints

ARGS:
broker*              (uri)       MQTT服务 endpoint,例如 `tcp://localhost:1883`
topic*               (string)    要发布的MQTT topic名称
username             (string)    MQTT 用户名
password             (string)    MQTT 密码
qos                  (number)    设置服务质量的级别, 默认是 '0'
keep_alive_interval  (duration)  MQTT连接的保持活动间隔（s，m，h，d）
reconnect_interval   (duration)  MQTT连接的重新连接间隔（s，m，h，d）
queue_dir            (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit          (number)    未发送消息的最大限制, 默认是'100000'
comment              (sentence)  可选的注释
```

或者通过环境变量(配置说明参考上面)

```
KEY:
notify_mqtt[:name]  publish bucket notifications to MQTT endpoints

ARGS:
MINIO_NOTIFY_MQTT_ENABLE*              (on|off)    enable notify_mqtt target, default is 'off'
MINIO_NOTIFY_MQTT_BROKER*              (uri)       MQTT server endpoint e.g. `tcp://localhost:1883`
MINIO_NOTIFY_MQTT_TOPIC*               (string)    name of the MQTT topic to publish
MINIO_NOTIFY_MQTT_USERNAME             (string)    MQTT username
MINIO_NOTIFY_MQTT_PASSWORD             (string)    MQTT password
MINIO_NOTIFY_MQTT_QOS                  (number)    set the quality of service priority, defaults to '0'
MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL  (duration)  keep-alive interval for MQTT connections in s,m,h,d
MINIO_NOTIFY_MQTT_RECONNECT_INTERVAL   (duration)  reconnect interval for MQTT connections in s,m,h,d
MINIO_NOTIFY_MQTT_QUEUE_DIR            (path)      staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_MQTT_QUEUE_LIMIT          (number)    maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_MQTT_COMMENT              (sentence)  optionally add a comment to this setting
```

MinIO支持持久事件存储。持久存储将在MQTT broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

更新配置前, 可以使用`mc admin config get`命令获取当前配置.

```sh
$ mc admin config get myminio/ notify_mqtt
notify_mqtt:1 broker="" password="" queue_dir="" queue_limit="0" reconnect_interval="0s"  keep_alive_interval="0s" qos="0" topic="" username=""
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::1:mqtt`。

```sh
$ mc admin config set myminio notify_mqtt:1 broker="tcp://localhost:1883" password="" queue_dir="" queue_limit="0" reconnect_interval="0s"  keep_alive_interval="0s" qos="1" topic="minio" username=""
```

更新完配置文件后，重启MinIO Server让配置生效。如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs:  arn:minio:sqs:us-east-1:1:mqtt`。

MinIO支持任何支持MQTT 3.1或3.1.1的MQTT服务器，并且可以使用`tcp://`, `tls://`, or `ws://`通过TCP，TLS或Websocket连接,作为代理URL的方案。 更多信息，请参考 [Go Client](http://www.eclipse.org/paho/clients/golang/)。

请注意, 根据你的需要，你可以添加任意多个MQTT server endpoint，只要提供MQTT实例的标识符（如上例中的“ 1”）和每个实例配置参数的信息即可。

### 第二步: 使用MinIO客户端启用bucket通知

如果一个JPEG图片上传到`myminio` server里的`images` 存储桶或者从桶中删除，一个存储桶事件通知就会被触发。 这里ARN值是`arn:minio:sqs::1:mqtt`。

```
mc mb myminio/images
mc event add  myminio/images arn:minio:sqs::1:mqtt --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:amqp s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步：验证MQTT

下面的python程序等待mqtt topic `/ minio`，并在控制台上打印事件通知。 我们使用[paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/)库来执行此操作。

```py
#!/usr/bin/env python3
from __future__ import print_function
import paho.mqtt.client as mqtt

# This is the Subscriber

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  # qos level is set to 1
  client.subscribe("minio", 1)

def on_message(client, userdata, msg):
    print(msg.payload)

# client_id is a randomly generated unique ID for the mqtt broker to identify the connection.
client = mqtt.Client(client_id="myclientid",clean_session=False)

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost",1883,60)
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
{“Records”:[{“eventVersion”:”2.0",”eventSource”:”aws:s3",”awsRegion”:”",”eventTime”:”2016–09–08T22:34:38.226Z”,”eventName”:”s3:ObjectCreated:Put”,”userIdentity”:{“principalId”:”minio”},”requestParameters”:{“sourceIPAddress”:”10.1.10.150:44576"},”responseElements”:{},”s3":{“s3SchemaVersion”:”1.0",”configurationId”:”Config”,”bucket”:{“name”:”images”,”ownerIdentity”:{“principalId”:”minio”},”arn”:”arn:aws:s3:::images”},”object”:{“key”:”myphoto.jpg”,”size”:200436,”sequencer”:”147279EAF9F40933"}}}],”level”:”info”,”msg”:””,”time”:”2016–09–08T15:34:38–07:00"}
```

<a name="Elasticsearch"></a>
## 使用Elasticsearch发布MinIO事件

安装 [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) 。

这个通知目标支持两种格式: _namespace_ 和 _access_。

如果使用的是 _namespace_ 格式, MinIO将桶中的对象与索引中的文档进行同步。对于MinIO中的每个事件，服务器都会使用事件中的存储桶和对象名称作为文档ID创建一个文档。事件的其他细节存储在document的正文中。因此，如果一个已经存在的对象在MinIO中被覆盖，在ES中的相对应的document也会被更新。如果一个对象被删除，相对应的document也会从index中删除。

如果使用的是 _access_ 格式，MinIO将事件作为document附加到ES的index中。对于每个事件，将带有事件详细信息的文档（文档的时间戳设置为事件的时间戳）附加到索引。这个文档的ID是由ES随机生成的。在 _access_ 格式下，不会有文档被删除或者修改。

下面的步骤展示的是在`namespace`格式下，如何使用通知目标。另一种格式和这个很类似，为了不让你们说我墨迹，就不再赘述了。


### 第一步：确保至少满足第低要求

MinIO要求使用的是ES 5.X系统版本。如果使用的是低版本的ES，也没关系，ES官方支持升级迁移，详情请看[这里](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html)。

### 第二步：把ES集成到MinIO中

Elasticsearch的配置信息位于`notify_elasticsearch`这个顶级的key下。在这里为你的Elasticsearch实例创建配置信息键值对。key是你的Elasticsearch endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_elasticsearch[:name]  发布存储桶通知到Elasticsearch endpoints

ARGS:
url*         (url)                Elasticsearch服务器的地址，以及可选的身份验证信息
index*       (string)             存储/更新事件的Elasticsearch索引，索引是自动创建的
format*      (namespace*|access)  是`namespace` 还是 `access`，默认是 'namespace'
queue_dir    (path)               未发送消息的暂存目录 例如 '/home/events'
queue_limit  (number)             未发送消息的最大限制, 默认是'100000'
comment      (sentence)           可选的注释
```
 
或者通过环境变量(配置说明参考上面)

```
KEY:
notify_elasticsearch[:name]  publish bucket notifications to Elasticsearch endpoints

ARGS:
MINIO_NOTIFY_ELASTICSEARCH_ENABLE*      (on|off)             enable notify_elasticsearch target, default is 'off'
MINIO_NOTIFY_ELASTICSEARCH_URL*         (url)                Elasticsearch server's address, with optional authentication info
MINIO_NOTIFY_ELASTICSEARCH_INDEX*       (string)             Elasticsearch index to store/update events, index is auto-created
MINIO_NOTIFY_ELASTICSEARCH_FORMAT*      (namespace*|access)  'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'
MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR    (path)               staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_ELASTICSEARCH_QUEUE_LIMIT  (number)             maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_ELASTICSEARCH_COMMENT      (sentence)           optionally add a comment to this setting
```

比如: `http://localhost:9200` 或者带有授权信息的 `http://elastic:MagicWord@127.0.0.1:9200`

MinIO支持持久事件存储。持久存储将在Elasticsearch broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

如果Elasticsearch启用了身份验证, 凭据可以通过格式为`PROTO://USERNAME:PASSWORD@ELASTICSEARCH_HOST:PORT`的`url`参数，提供给MinIO。

更新配置前，可以通过`mc admin config get`命令获取当前配置。

```sh
$ mc admin config get myminio/ notify_elasticsearch
notify_elasticsearch:1 queue_limit="0"  url="" format="namespace" index="" queue_dir=""
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似`SQS ARNs: arn:minio:sqs::1:elasticsearch`。

请注意, 根据你的需要，你可以添加任意多个ES server endpoint，只要提供ES实例的标识符（如上例中的“ 1”）和每个实例配置参数的信息即可。

### 第三步：使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知。一旦有文件被创建或者覆盖，一个新的ES的document会被创建或者更新到之前咱配的index里。如果一个已经存在的对象被删除，这个对应的document也会从index中删除。因此，这个ES index里的行，就映射着`images`存储桶里的`.jpg`对象。

要配置这种存储桶通知，我们需要用到前面步骤MinIO输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设咱们的MinIO服务别名叫`myminio`,可执行下列脚本：

```
mc mb myminio/images
mc event add  myminio/images arn:minio:sqs::1:elasticsearch --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:elasticsearch s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第四步：验证Elasticsearch

上传一张JPEG图片到`images` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

使用curl查看`minio_events` index中的内容。

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
                "userAgent" : "MinIO (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z"
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
## 使用Redis发布MinIO事件

安装 [Redis](http://redis.io/download)。为了演示，我们将数据库密码设为"yoursecret"。

这种通知目标支持两种格式: _namespace_ 和 _access_。

如果用的是 _namespacee_ 格式，MinIO将存储桶里的对象同步成Redis hash中的条目。对于每一个条目，对应一个存储桶里的对象，其key都被设为"存储桶名称/对象名称"，value都是一个有关这个MinIO对象的JSON格式的事件数据。如果对象更新或者删除，hash中对象的条目也会相应的更新或者删除。

如果使用的是 _access_ ,MinIO使用[RPUSH](https://redis.io/commands/rpush)将事件添加到list中。这个list中每一个元素都是一个JSON格式的list,这个list中又有两个元素，第一个元素是时间戳的字符串，第二个元素是一个含有在这个存储桶上进行操作的事件数据的JSON对象。在这种格式下，list中的元素不会更新或者删除。

下面的步骤展示如何在`namespace`和`access`格式下使用通知目标。

### 第一步：集成Redis到MinIO

The MinIO server的配置文件以json格式存储在后端。Redis的配置信息位于`notify_redis`这个顶级的key下。在这里为你的Redis实例创建配置信息键值对。key是你的Redis endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_redis[:name]  发布存储桶通知到Redis

ARGS:
address*     (address)            Redis服务器的地址. 例如: `localhost:6379`
key*         (string)             存储/更新事件的Redis key, key会自动创建
format*      (namespace*|access)  是`namespace` 还是 `access`，默认是 'namespace'
password     (string)             Redis服务器的密码
queue_dir    (path)               未发送消息的暂存目录 例如 '/home/events'
queue_limit  (number)             未发送消息的最大限制, 默认是'100000'
comment      (sentence)           可选的注释说明
```
          
或者通过环境变量(配置说明参考上面)

```
KEY:
notify_redis[:name]  publish bucket notifications to Redis datastores

ARGS:
MINIO_NOTIFY_REDIS_ENABLE*      (on|off)             enable notify_redis target, default is 'off'
MINIO_NOTIFY_REDIS_KEY*         (string)             Redis key to store/update events, key is auto-created
MINIO_NOTIFY_REDIS_FORMAT*      (namespace*|access)  'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'
MINIO_NOTIFY_REDIS_PASSWORD     (string)             Redis server password
MINIO_NOTIFY_REDIS_QUEUE_DIR    (path)               staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_REDIS_QUEUE_LIMIT  (number)             maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_REDIS_COMMENT      (sentence)           optionally add a comment to this setting
```

MinIO支持持久事件存储。持久存储将在Redis broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

更新配置前，可以通过`mc admin config get`命令获取当前配置。

```sh
$ mc admin config get myminio/ notify_redis
notify_redis:1 address="" format="namespace" key="" password="" queue_dir="" queue_limit="0"
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似`SQS ARNs: arn:minio:sqs::1:redis`。

```sh
$ mc admin config set myminio/ notify_redis:1 address="127.0.0.1:6379" format="namespace" key="bucketevents" password="yoursecret" queue_dir="" queue_limit="0"
```

请注意, 根据你的需要，你可以添加任意多个Redis server endpoint，只要提供Redis实例的标识符（如上例中的“ 1”）和每个实例配置参数的信息即可。

### 第二步: 使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知。当一个JPEG文件被创建或者覆盖，一个新的key会被创建,或者一个已经存在的key就会被更新到之前配置好的redis hash里。如果一个已经存在的对象被删除，这个对应的key也会从hash中删除。因此，这个Redis hash里的行，就映射着`images`存储桶里的`.jpg`对象。

要配置这种存储桶通知，我们需要用到前面步骤MinIO输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设咱们的MinIO服务别名叫`myminio`,可执行下列脚本：

```
mc mb myminio/images
mc event add myminio/images arn:minio:sqs::1:redis --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:redis s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步：验证Redis

启动`redis-cli`这个Redis客户端程序来检查Redis中的内容. 运行`monitor`Redis命令将会输出在Redis上执行的每个命令的。

```
redis-cli -a yoursecret
127.0.0.1:6379> monitor
OK
```

打开一个新的terminal终端并上传一张JPEG图片到`images` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

在上一个终端中，你将看到MinIO在Redis上执行的操作：

```
127.0.0.1:6379> monitor
OK
1490686879.650649 [0 172.17.0.1:44710] "PING"
1490686879.651061 [0 172.17.0.1:44710] "HSET" "minio_events" "images/myphoto.jpg" "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"minio:s3\",\"awsRegion\":\"\",\"eventTime\":\"2017-03-28T07:41:19Z\",\"eventName\":\"s3:ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"minio\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1:52234\"},\"responseElements\":{\"x-amz-request-id\":\"14AFFBD1ACE5F632\",\"x-minio-origin-endpoint\":\"http://192.168.86.115:9000\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"Config\",\"bucket\":{\"name\":\"images\",\"ownerIdentity\":{\"principalId\":\"minio\"},\"arn\":\"arn:aws:s3:::images\"},\"object\":{\"key\":\"myphoto.jpg\",\"size\":2586,\"eTag\":\"5d284463f9da279f060f0ea4d11af098\",\"sequencer\":\"14AFFBD1ACE5F632\"}},\"source\":{\"host\":\"127.0.0.1\",\"port\":\"52234\",\"userAgent\":\"MinIO (linux; amd64) minio-go/2.0.3 mc/2017-02-15T17:57:25Z\"}}]}"
```

在这我们可以看到MinIO在`minio_events`这个key上执行了`HSET`命令。

如果用的是`access`格式，那么`minio_events`就是一个list,MinIO就会调用`RPUSH`添加到list中。这个list的消费者会使用`BLPOP`从list的最左端删除list元素。

<a name="NATS"></a>
## 使用NATS发布MinIO事件

安装 [NATS](http://nats.io/).

### 第一步：集成NATS到MinIO

MinIO支持持久事件存储。持久存储将在NATS broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

```
KEY:
notify_nats[:name]  发布存储桶通知到NATS endpoints

ARGS:
address*                          (address)   NATS服务器地址，例如 '0.0.0.0:4222'
subject*                          (string)    NATS 订阅的 subject
username                          (string)    NATS 用户名
password                          (string)    NATS 密码
token                             (string)    NATS token
tls                               (on|off)    'on'代表启用TLS
tls_skip_verify                   (on|off)    跳过TLS证书验证, 默认是"on" (可信的)
ping_interval                     (duration)  客户端ping命令的时间间隔(s,m,h,d)。 默认禁止
streaming                         (on|off)    设置为'on', 代表用streaming NATS 服务器
streaming_async                   (on|off)    设置为'on', 代表启用异步发布
streaming_max_pub_acks_in_flight  (number)    无需等待ACK即可发布的消息数
streaming_cluster_id              (string)    NATS streaming集群的唯一ID
cert_authority                    (string)    目标NATS服务器的证书链的路径
client_cert                       (string)    NATS mTLS身份验证的客户端证书
client_key                        (string)    NATS mTLS身份验证的客户端证书密钥
queue_dir                         (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit                       (number)    未发送消息的最大限制, 默认是'100000'
comment                           (sentence)  可选的注释说明
```
         
或者通过环境变量(配置说明参考上面)
```
KEY:
notify_nats[:name]  publish bucket notifications to NATS endpoints

ARGS:
MINIO_NOTIFY_NATS_ENABLE*                           (on|off)    enable notify_nats target, default is 'off'
MINIO_NOTIFY_NATS_ADDRESS*                          (address)   NATS server address e.g. '0.0.0.0:4222'
MINIO_NOTIFY_NATS_SUBJECT*                          (string)    NATS subscription subject
MINIO_NOTIFY_NATS_USERNAME                          (string)    NATS username
MINIO_NOTIFY_NATS_PASSWORD                          (string)    NATS password
MINIO_NOTIFY_NATS_TOKEN                             (string)    NATS token
MINIO_NOTIFY_NATS_TLS                               (on|off)    set to 'on' to enable TLS
MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY                   (on|off)    trust server TLS without verification, defaults to "on" (verify)
MINIO_NOTIFY_NATS_PING_INTERVAL                     (duration)  client ping commands interval in s,m,h,d. Disabled by default
MINIO_NOTIFY_NATS_STREAMING                         (on|off)    set to 'on', to use streaming NATS server
MINIO_NOTIFY_NATS_STREAMING_ASYNC                   (on|off)    set to 'on', to enable asynchronous publish
MINIO_NOTIFY_NATS_STREAMING_MAX_PUB_ACKS_IN_FLIGHT  (number)    number of messages to publish without waiting for ACKs
MINIO_NOTIFY_NATS_STREAMING_CLUSTER_ID              (string)    unique ID for NATS streaming cluster
MINIO_NOTIFY_NATS_CERT_AUTHORITY                    (string)    path to certificate chain of the target NATS server
MINIO_NOTIFY_NATS_CLIENT_CERT                       (string)    client cert for NATS mTLS auth
MINIO_NOTIFY_NATS_CLIENT_KEY                        (string)    client cert key for NATS mTLS auth
MINIO_NOTIFY_NATS_QUEUE_DIR                         (path)      staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_NATS_QUEUE_LIMIT                       (number)    maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_NATS_COMMENT                           (sentence)  optionally add a comment to this setting
```

更新配置前, 使用`mc admin config get` 命令获取当前配置.

```sh
$ mc admin config get myminio/ notify_nats
notify_nats:1 password="yoursecret" streaming_max_pub_acks_in_flight="10" subject="" address="0.0.0.0:4222"  token="" username="yourusername" ping_interval="0" queue_limit="0" tls="off" tls_skip_verify="off" streaming_async="on" queue_dir="" streaming_cluster_id="test-cluster" streaming_enable="on"
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。`bucketevents` 是NATS在这个例子中使用的subject.

```sh
$ mc admin config set myminio notify_nats:1 password="yoursecret" streaming_max_pub_acks_in_flight="10" subject="" address="0.0.0.0:4222"  token="" username="yourusername" ping_interval="0" queue_limit="0" tls="off" streaming_async="on" queue_dir="" streaming_cluster_id="test-cluster" streaming_enable="on"
```

MinIO server还支持[NATS Streaming模式](http://nats.io/documentation/streaming/nats-streaming-intro/) ，该模式提供一些附加功能， 比如`At-least-once-delivery`,  `Publisher rate limiting`。要配置MinIO server发送通知到NATS Streaming 服务器, 请参考上面的更新MinIO配置.

更多关于`cluster_id`, `client_id`的信息，请参考 [NATS documentation](https://github.com/nats-io/nats-streaming-server/blob/master/README.md). 点击[这里](https://github.com/nats-io/stan.go#publisher-rate-limiting)查看关于`maxPubAcksInflight`的说明.

### 第二步: 使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦``myminio`` server上有文件  从``images``存储桶里删除或者上传到存储桶中，事件即被触发。在这里，ARN的值是`arn:minio:sqs::1:nats`。 更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

```
mc mb myminio/images
mc event add myminio/images arn:minio:sqs::1:nats --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:nats s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

###  第三步：验证NATS

如果你用的是NATS server，请查看下面的示例程序来记录添加到NATS的存储桶通知。

```go
package main

// Import Go and NATS packages
import (
	"log"
	"runtime"

	"github.com/nats-io/nats.go"
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

打开一个新的terminal终端并上传一张JPEG图片到`images` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

`nats.go`示例程序将事件通知打印到控制台。

```
go run nats.go
2016/10/12 06:51:26 Connected
2016/10/12 06:51:26 Subscribing to subject 'bucketevents'
2016/10/12 06:51:33 Received message '{"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"","eventTime":"2016-10-12T13:51:33Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"[::1]:57106"},"responseElements":{},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":56060,"eTag":"1d97bf45ecb37f7a7b699418070df08f","sequencer":"147CCD1AE054BFD0"}}}],"level":"info","msg":"","time":"2016-10-12T06:51:33-07:00"}
```

如果你用的是NATS Streaming server,请查看下面的示例程序来记录添加到NATS的存储桶通知。

```go
package main

// Import Go and NATS packages
import (
	"fmt"
	"runtime"

	"github.com/nats-io/stan.go"
)

func main() {

	var stanConnection stan.Conn

	subscribe := func() {
		fmt.Printf("Subscribing to subject 'bucketevents'\n")
		stanConnection.Subscribe("bucketevents", func(m *stan.Msg) {

			// Handle the message
			fmt.Printf("Received a message: %s\n", string(m.Data))
		})
	}


	stanConnection, _ = stan.Connect("test-cluster", "test-client", stan.NatsURL("nats://yourusername:yoursecret@0.0.0.0:4222"), stan.SetConnectionLostHandler(func(c stan.Conn, _ error) {
		go func() {
			for {
				// Reconnect if the connection is lost.
				if stanConnection == nil || stanConnection.NatsConn() == nil ||  !stanConnection.NatsConn().IsConnected() {
					stanConnection, _ = stan.Connect("test-cluster", "test-client", stan.NatsURL("nats://yourusername:yoursecret@0.0.0.0:4222"), stan.SetConnectionLostHandler(func(c stan.Conn, _ error) {
						if c.NatsConn() != nil {
							c.NatsConn().Close()
						}
						_ = c.Close()
					}))
					if stanConnection != nil {
						subscribe()
					}

				}
			}

		}()
	}))

	// Subscribe to subject
	subscribe()

	// Keep the connection alive
	runtime.Goexit()
}

```

```
go run nats.go
2017/07/07 11:47:40 Connected
2017/07/07 11:47:40 Subscribing to subject 'bucketevents'
```

打开一个新的terminal终端并上传一张JPEG图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

`nats.go`示例程序将事件通知打印到控制台。

```
Received a message: {"EventType":"s3:ObjectCreated:Put","Key":"images/myphoto.jpg","Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"","eventTime":"2017-07-07T18:46:37Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"sourceIPAddress":"192.168.1.80:55328"},"responseElements":{"x-amz-request-id":"14CF20BD1EFD5B93","x-minio-origin-endpoint":"http://127.0.0.1:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::images"},"object":{"key":"myphoto.jpg","size":248682,"eTag":"f1671feacb8bbf7b0397c6e9364e8c92","contentType":"image/jpeg","userDefined":{"content-type":"image/jpeg"},"versionId":"1","sequencer":"14CF20BD1EFD5B93"}},"source":{"host":"192.168.1.80","port":"55328","userAgent":"MinIO (linux; amd64) minio-go/2.0.4 mc/DEVELOPMENT.GOGET"}}],"level":"info","msg":"","time":"2017-07-07T11:46:37-07:00"}
```

<a name="PostgreSQL"></a>
## 使用PostgreSQL发布MinIO事件

> 注意：在版本RELEASE.2020-04-10T03-34-42Z之前的PostgreSQL通知用于支持以下选项：
>
> ```
> host                (hostname)           Postgres server hostname (used only if `connection_string` is empty)
> port                (port)               Postgres server port, defaults to `5432` (used only if `connection_string` is empty)
> username            (string)             database username (used only if `connection_string` is empty)
> password            (string)             database password (used only if `connection_string` is empty)
> database            (string)             database name (used only if `connection_string` is empty)
> ```
>
> 这些现在已经弃用, 如果你打算升级到*RELEASE.2020-04-10T03-34-42Z*之后的版本请确保
> 仅使用*connection_string*选项迁移.一旦所有服务器都升级完成，请使用以下命令更新现有的通知目标完成迁移。
>
> ```
> mc admin config set myminio/ notify_postgres[:name] connection_string="host=hostname port=2832 username=psqluser password=psqlpass database=bucketevents"
> ```
>
> 请确保执行此步骤，否则将无法执行PostgreSQL通知目标，
> 服务器升级/重启后，控制台上会显示一条错误消息，请务必遵循上述说明。
> 如有其他问题，请加入我们的 https://slack.min.io

安装 [PostgreSQL](https://www.postgresql.org/) 数据库。为了演示，我们将"postgres"用户的密码设为`password`，并且创建了一个`minio_events`数据库来存储事件信息。

这个通知目标支持两种格式: _namespace_ 和 _access_。

如果使用的是 _namespace_ 格式，MinIO将存储桶里的对象同步成数据库表中的行。每一行有两列：key和value。key是这个对象的存储桶名字加上对象名，value都是一个有关这个MinIO对象的JSON格式的事件数据。如果对象更新或者删除，表中相应的行也会相应的更新或者删除。

如果使用的是 _access_,MinIO将将事件添加到表里，行有两列：event_time 和 event_data。event_time是事件在MinIO server里发生的时间，event_data是有关这个MinIO对象的JSON格式的事件数据。在这种格式下，不会有行会被删除或者修改。

下面的步骤展示的是如何在`namespace`格式下使用通知目标，`_access_`差不多，不再赘述，我相信你可以触类旁通，举一反三，不要让我失望哦。

### 第一步：确保确保至少满足第低要求

MinIO要求PostgresSQL9.5版本及以上。 MinIO用了PostgreSQL9.5引入的[`INSERT ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) (aka UPSERT) 特性,以及9.4引入的 [JSONB](https://www.postgresql.org/docs/9.4/static/datatype-json.html) 数据类型。

### 第二步：集成PostgreSQL到MinIO

PostgreSQL的配置信息位于`notify_postgresql`这个顶级的key下。在这里为你的PostgreSQL实例创建配置信息键值对。key是你的PostgreSQL endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_postgres[:name]  发布存储桶通知到Postgres数据库

ARGS:
connection_string*  (string)             Postgres server的连接字符串，例如 "host=localhost port=5432 dbname=minio_events user=postgres password=password sslmode=disable"
table*              (string)             存储/更新事件的数据库表名, 表会自动被创建
format*             (namespace*|access)  'namespace'或者'access', 默认是'namespace'
queue_dir           (path)               未发送消息的暂存目录 例如 '/home/events'
queue_limit         (number)             未发送消息的最大限制, 默认是'100000'
comment             (sentence)           可选的注释说明
```

或者通过环境变量（说明详见上面）
```
KEY:
notify_postgres[:name]  publish bucket notifications to Postgres databases

ARGS:
MINIO_NOTIFY_POSTGRES_ENABLE*             (on|off)             enable notify_postgres target, default is 'off'
MINIO_NOTIFY_POSTGRES_CONNECTION_STRING*  (string)             Postgres server connection-string e.g. "host=localhost port=5432 dbname=minio_events user=postgres password=password sslmode=disable"
MINIO_NOTIFY_POSTGRES_TABLE*              (string)             DB table name to store/update events, table is auto-created
MINIO_NOTIFY_POSTGRES_FORMAT*             (namespace*|access)  'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'
MINIO_NOTIFY_POSTGRES_QUEUE_DIR           (path)               staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_POSTGRES_QUEUE_LIMIT         (number)             maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_POSTGRES_COMMENT             (sentence)           optionally add a comment to this setting
```

MinIO支持持久事件存储。持久存储将在PostgreSQL连接离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

注意这里为了演示, 我们禁止了SSL. 处于安全起见, 不推荐用于生产.
更新配置前, 使用`mc admin config get`命令获取当前配置。

```sh
$ mc admin config get myminio notify_postgres
notify_postgres:1 queue_dir="" connection_string="" queue_limit="0"  table="" format="namespace"
```

Use `mc admin config set`命令更新完配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::1:postgresql`。

```sh
$ mc admin config set myminio notify_postgres:1 connection_string="host=localhost port=5432 dbname=minio_events user=postgres password=password sslmode=disable" table="bucketevents" format="namespace"
```

请注意, 根据你的需要，你可以添加任意多个PostgreSQL server endpoint，只要提供PostgreSQL实例的标识符（如上例中的“ 1”）和每个实例配置参数的信息即可。

### 第三步：使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦上有文件上传到存储桶中，PostgreSQL中会insert一条新的记录或者一条已经存在的记录会被update，如果一个存在对象被删除，一条对应的记录也会从PostgreSQL表中删除。因此，PostgreSQL表中的行，对应的就是存储桶里的一个对象。

要配置这种存储桶通知，我们需要用到前面步骤中MinIO输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设MinIO服务别名叫`myminio`,可执行下列脚本：

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc event add myminio/images arn:minio:sqs::1:postgresql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc event list myminio/images
mc event list myminio/images
arn:minio:sqs::1:postgresql s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第四步：验证PostgreSQL

打开一个新的terminal终端并上传一张JPEG图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

打开一个PostgreSQL终端列出表 `bucketevents` 中所有的记录。

```
$ psql -h 127.0.0.1 -U postgres -d minio_events
minio_events=# select * from bucketevents;

key                 |                      value
--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 images/myphoto.jpg | {"Records": [{"s3": {"bucket": {"arn": "arn:aws:s3:::images", "name": "images", "ownerIdentity": {"principalId": "minio"}}, "object": {"key": "myphoto.jpg", "eTag": "1d97bf45ecb37f7a7b699418070df08f", "size": 56060, "sequencer": "147CE57C70B31931"}, "configurationId": "Config", "s3SchemaVersion": "1.0"}, "awsRegion": "", "eventName": "s3:ObjectCreated:Put", "eventTime": "2016-10-12T21:18:20Z", "eventSource": "aws:s3", "eventVersion": "2.0", "userIdentity": {"principalId": "minio"}, "responseElements": {}, "requestParameters": {"sourceIPAddress": "[::1]:39706"}}]}
(1 row)
```

<a name="MySQL"></a>

## 使用MySQL发布MinIO事件

> 注意：在版本RELEASE.2020-04-10T03-34-42Z之前的MySQL通知用于支持以下选项：
>
> ```
> host         (hostname)           MySQL server hostname (used only if `dsn_string` is empty)
> port         (port)               MySQL server port (used only if `dsn_string` is empty)
> username     (string)             database username (used only if `dsn_string` is empty)
> password     (string)             database password (used only if `dsn_string` is empty)
> database     (string)             database name (used only if `dsn_string` is empty)
> ```
>
> 这些现在已经弃用, 如果你打算升级到*RELEASE.2020-04-10T03-34-42Z*之后的版本请确保
> 仅使用*dsn_string*选项迁移. 一旦所有服务器都升级完成，请使用以下命令更新现有的通知目标完成迁移
>
> ```
> mc admin config set myminio/ notify_mysql[:name] dsn_string="mysqluser:mysqlpass@tcp(localhost:2832)/bucketevents"
> ```
>
> 请确保执行此步骤, 否则将无法执行MySQL通知目标，
> 服务器升级/重启后，控制台上会显示一条错误消息，请务必遵循上述说明。
> 如有其他问题，请加入我们的 https://slack.min.io

安装 [MySQL](https://dev.mysql.com/downloads/mysql/). 为了演示，我们将"postgres"用户的密码设为`password`，并且创建了一个`miniodb`数据库来存储事件信息。

这个通知目标支持两种格式: _namespace_ 和 _access_。

如果使用的是 _namespace_ 格式，MinIO将存储桶里的对象同步成数据库表中的行。每一行有两列：key_name和value。key_name是这个对象的存储桶名字加上对象名，value都是一个有关这个MinIO对象的JSON格式的事件数据。如果对象更新或者删除，表中相应的行也会相应的更新或者删除。

如果使用的是 _access_,MinIO将将事件添加到表里，行有两列：event_time 和 event_data。event_time是事件在MinIO server里发生的时间，event_data是有关这个MinIO对象的JSON格式的事件数据。在这种格式下，不会有行会被删除或者修改。

下面的步骤展示的是如何在`namespace`格式下使用通知目标，`_access_`差不多，不再赘述。

### 第一步：确保确保至少满足第低要求

MinIO要求MySQL 版本 5.7.8及以上，MinIO使用了MySQL5.7.8版本引入的 [JSON](https://dev.mysql.com/doc/refman/5.7/en/json.html) 数据类型。我们使用的是MySQL5.7.17进行的测试。

### 第二步：集成MySQL到MinIO

MySQL配置位于 `notify_mysql`key下. 在这里为你的PostgreSQL实例创建配置信息键值对。key是你的MySQL endpoint的名称，value是下面表格中列列的键值对集合。

```
KEY:
notify_mysql[:name]  发布存储桶通知到MySQL数据库. 当需要多个MySQL server endpoint时，可以为每个配置添加用户指定的“name”（例如"notify_mysql:myinstance"）.

ARGS:
dsn_string*  (string)             MySQL数据源名称连接字符串，例如 "<user>:<password>@tcp(<host>:<port>)/<database>"
table*       (string)             存储/更新事件的数据库表名, 表会自动被创建
format*      (namespace*|access)  'namespace'或者'access', 默认是'namespace'
queue_dir    (path)               未发送消息的暂存目录 例如 '/home/events'
queue_limit  (number)             未发送消息的最大限制, 默认是'100000'
comment      (sentence)           可选的注释说明
```

或者通过环境变量（说明详见上面）
```
KEY:
notify_mysql[:name]  publish bucket notifications to MySQL databases

ARGS:
MINIO_NOTIFY_MYSQL_ENABLE*      (on|off)             enable notify_mysql target, default is 'off'
MINIO_NOTIFY_MYSQL_DSN_STRING*  (string)             MySQL data-source-name connection string e.g. "<user>:<password>@tcp(<host>:<port>)/<database>"
MINIO_NOTIFY_MYSQL_TABLE*       (string)             DB table name to store/update events, table is auto-created
MINIO_NOTIFY_MYSQL_FORMAT*      (namespace*|access)  'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'
MINIO_NOTIFY_MYSQL_QUEUE_DIR    (path)               staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_MYSQL_QUEUE_LIMIT  (number)             maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_MYSQL_COMMENT      (sentence)           optionally add a comment to this setting
```

`dsn_string`是必须的，并且格式为 `"<user>:<password>@tcp(<host>:<port>)/<database>"`

MinIO支持持久事件存储。持久存储将在MySQL连接离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

更新配置前, 可以使用`mc admin config get`命令获取当前配置.

```sh
$ mc admin config get myminio/ notify_mysql
notify_mysql:myinstance enable=off format=namespace host= port= username= password= database= dsn_string= table= queue_dir= queue_limit=0
```

使用带有`dsn_string`参数的`mc admin config set`的命令更新MySQL的通知配置:

```sh
$ mc admin config set myminio notify_mysql:myinstance table="minio_images" dsn_string="root:xxxx@tcp(172.17.0.1:3306)/miniodb"
```

请注意, 根据你的需要，你可以添加任意多个MySQL server endpoint，只要提供MySQL实例的标识符（如上例中的"myinstance"）和每个实例配置参数的信息即可。

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::myinstance:mysql`。

### 第三步：使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦上有文件上传到存储桶中，MySQL中会insert一条新的记录或者一条已经存在的记录会被update，如果一个存在对象被删除，一条对应的记录也会从MySQL表中删除。因此，MySQL表中的行，对应的就是存储桶里的一个对象。

要配置这种存储桶通知，我们需要用到前面步骤MinIO输出的ARN信息。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

有了`mc`这个工具，这些配置信息很容易就能添加上。假设咱们的MinIO服务别名叫`myminio`,可执行下列脚本：

```
# Create bucket named `images` in myminio
mc mb myminio/images
# Add notification configuration on the `images` bucket using the MySQL ARN. The --suffix argument filters events.
mc event add myminio/images arn:minio:sqs::myinstance:mysql --suffix .jpg
# Print out the notification configuration on the `images` bucket.
mc event list myminio/images
arn:minio:sqs::myinstance:mysql s3:ObjectCreated:*,s3:ObjectRemoved:*,s3:ObjectAccessed:* Filter: suffix=”.jpg”
```

### 第四步：验证MySQL

打开一个新的terminal终端并上传一张JPEG图片到`images` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

打开一个MySQL终端列出表 `minio_images` 中所有的记录。

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

## 使用Kafka发布MinIO事件

安装[ Apache Kafka](http://kafka.apache.org/).

### 第一步：确保确保至少满足第低要求

MinIO要求Kafka版本0.10或者0.9.MinIO内部使用了 [Shopify/sarama](https://github.com/Shopify/sarama/) 库，因此需要和该库有同样的版本兼容性。

###第二步：集成Kafka到MinIO

MinIO支持持久事件存储。持久存储将在kafka broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

```
KEY:
notify_kafka[:name]  发布存储桶通知到Kafka endpoints

ARGS:
brokers*         (csv)       逗号分隔的Kafka broker地址列表
topic            (string)    用于存储桶通知的Kafka topic
sasl_username    (string)    SASL/PLAIN或者SASL/SCRAM身份验证的用户名
sasl_password    (string)    SASL/PLAIN或者SASL/SCRAM身份验证的密码
sasl_mechanism   (string)    sasl认证机制, 默认是'PLAIN'
tls_client_auth  (string)    clientAuth确定TLS客户端身份验证的Kafka服务器策略
sasl             (on|off)    设置为'on'代表启用 SASL身份验证
tls              (on|off)    设置为'on'代表启用TLS
tls_skip_verify  (on|off)    跳过TLS证书验证, 默认是"on" (可信的)
client_tls_cert  (path)      用于mTLS身份验证的客户端证书的路径
client_tls_key   (path)      mTLS身份验证的客户端密钥的路径
queue_dir        (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit      (number)    未发送消息的最大限制, 默认是'100000'
version          (string)    指定 Kafka集群的版本， 例如 '2.2.0'
comment          (sentence)  可选的注释说明
```
          
患者通过环境变量（说明详见上面）
```
KEY:
notify_kafka[:name]  publish bucket notifications to Kafka endpoints

ARGS:
MINIO_NOTIFY_KAFKA_ENABLE*          (on|off)                enable notify_kafka target, default is 'off'
MINIO_NOTIFY_KAFKA_BROKERS*         (csv)                   comma separated list of Kafka broker addresses
MINIO_NOTIFY_KAFKA_TOPIC            (string)                Kafka topic used for bucket notifications
MINIO_NOTIFY_KAFKA_SASL_USERNAME    (string)                username for SASL/PLAIN or SASL/SCRAM authentication
MINIO_NOTIFY_KAFKA_SASL_PASSWORD    (string)                password for SASL/PLAIN or SASL/SCRAM authentication
MINIO_NOTIFY_KAFKA_SASL_MECHANISM   (plain*|sha256|sha512)  sasl authentication mechanism, default 'plain'
MINIO_NOTIFY_KAFKA_TLS_CLIENT_AUTH  (string)                clientAuth determines the Kafka server's policy for TLS client auth
MINIO_NOTIFY_KAFKA_SASL             (on|off)                set to 'on' to enable SASL authentication
MINIO_NOTIFY_KAFKA_TLS              (on|off)                set to 'on' to enable TLS
MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY  (on|off)                trust server TLS without verification, defaults to "on" (verify)
MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT  (path)                  path to client certificate for mTLS auth
MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY   (path)                  path to client key for mTLS auth
MINIO_NOTIFY_KAFKA_QUEUE_DIR        (path)                  staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_KAFKA_QUEUE_LIMIT      (number)                maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_KAFKA_COMMENT          (sentence)              optionally add a comment to this setting
MINIO_NOTIFY_KAFKA_VERSION          (string)                specify the version of the Kafka cluster e.g. '2.2.0'
```

更新配置前, 使用`mc admin config get`命令获取当前配置

```sh
$ mc admin config get myminio/ notify_kafka
notify_kafka:1 tls_skip_verify="off"  queue_dir="" queue_limit="0" sasl="off" sasl_password="" sasl_username="" tls_client_auth="0" tls="off" brokers="" topic="" client_tls_cert="" client_tls_key="" version=""
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::1:kafka`。`bucketevents`是kafka在此示例中使用的主题。

```sh
$ mc admin config set myminio notify_kafka:1 tls_skip_verify="off"  queue_dir="" queue_limit="0" sasl="off" sasl_password="" sasl_username="" tls_client_auth="0" tls="off" client_tls_cert="" client_tls_key="" brokers="localhost:9092,localhost:9093" topic="bucketevents" version=""
```

### 第三步：使用MinIO客户端启用bucket通知


我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦上有文件上传到存储桶中，事件将被触发。在这里，ARN的值是``arn:minio:sqs:us-east-1:1:kafka``。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

```
mc mb myminio/images
mc event add  myminio/images arn:minio:sqs::1:kafka --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:kafka s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第四步：验证Kafka

我们使用 [kafkacat](https://github.com/edenhill/kafkacat) 将所有的通知输出到控制台。

```
kafkacat -C -b localhost:9092 -t bucketevents
```

打开一个新的terminal终端并上传一张JPEG图片到``images`` 存储桶。

```
mc cp myphoto.jpg myminio/images
```

`kafkacat` 输出事件通知到控制台。

```
kafkacat -b localhost:9092 -t bucketevents
{
    "EventName": "s3:ObjectCreated:Put",
    "Key": "images/myphoto.jpg",
    "Records": [
        {
            "eventVersion": "2.0",
            "eventSource": "minio:s3",
            "awsRegion": "",
            "eventTime": "2019-09-10T17:41:54Z",
            "eventName": "s3:ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AKIAIOSFODNN7EXAMPLE"
            },
            "requestParameters": {
                "accessKey": "AKIAIOSFODNN7EXAMPLE",
                "region": "",
                "sourceIPAddress": "192.168.56.192"
            },
            "responseElements": {
                "x-amz-request-id": "15C3249451E12784",
                "x-minio-deployment-id": "751a8ba6-acb2-42f6-a297-4cdf1cf1fa4f",
                "x-minio-origin-endpoint": "http://192.168.97.83:9000"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "Config",
                "bucket": {
                    "name": "images",
                    "ownerIdentity": {
                        "principalId": "AKIAIOSFODNN7EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::images"
                },
                "object": {
                    "key": "myphoto.jpg",
                    "size": 6474,
                    "eTag": "430f89010c77aa34fc8760696da62d08-1",
                    "contentType": "image/jpeg",
                    "userMetadata": {
                        "content-type": "image/jpeg"
                    },
                    "versionId": "1",
                    "sequencer": "15C32494527B46C5"
                }
            },
            "source": {
                "host": "192.168.56.192",
                "port": "",
                "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0"
            }
        }
    ]
}
```

<a name="webhooks"></a>

## 使用Webhook发布MinIO事件

[Webhooks](https://en.wikipedia.org/wiki/Webhook) 采用推的方式获取数据，而不是一直去拉取。

### 第一步：集成MySQL到MinIO

MinIO支持持久事件存储。持久存储将在webhook离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

```
KEY:
notify_webhook[:name]  发布存储桶通知到webhook endpoints

ARGS:
endpoint*    (url)       webhook server endpoint,例如 http://localhost:8080/minio/events
auth_token   (string)    opaque token或者JWT authorization token
queue_dir    (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit  (number)    未发送消息的最大限制, 默认是'100000'
client_cert  (string)    Webhook的mTLS身份验证的客户端证书
client_key   (string)    Webhook的mTLS身份验证的客户端证书密钥
comment      (sentence)  可选的注释说明
```
 
或者通过环境变量（说明参见上面）
```
KEY:
notify_webhook[:name]  publish bucket notifications to webhook endpoints

ARGS:
MINIO_NOTIFY_WEBHOOK_ENABLE*      (on|off)    enable notify_webhook target, default is 'off'
MINIO_NOTIFY_WEBHOOK_ENDPOINT*    (url)       webhook server endpoint e.g. http://localhost:8080/minio/events
MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN   (string)    opaque string or JWT authorization token
MINIO_NOTIFY_WEBHOOK_QUEUE_DIR    (path)      staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_WEBHOOK_QUEUE_LIMIT  (number)    maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_WEBHOOK_COMMENT      (sentence)  optionally add a comment to this setting
MINIO_NOTIFY_WEBHOOK_CLIENT_CERT  (string)    client cert for Webhook mTLS auth
MINIO_NOTIFY_WEBHOOK_CLIENT_KEY   (string)    client cert key for Webhook mTLS auth   
```

```sh
$ mc admin config get myminio/ notify_webhook
notify_webhook:1 endpoint="" auth_token="" queue_limit="0" queue_dir="" client_cert="" client_key=""
```

用`mc admin config set` 命令更新配置. 在这endpoint是监听webhook通知的服务. 保存配置文件并重启MinIO服务让配配置生效. 注意一下，在重启MinIO时，这个endpoint必须是启动并且可访问到。

```sh
$ mc admin config set myminio notify_webhook:1 queue_limit="0"  endpoint="http://localhost:3000" queue_dir=""
```

### 第二步：使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦上有文件上传到存储桶中，事件将被触发。在这里，ARN的值是`arn:minio:sqs::1:webhook`。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

```
mc mb myminio/images
mc mb myminio/images-thumbnail
mc event add myminio/images arn:minio:sqs::1:webhook --event put --suffix .jpg
```

验证事件通知是否配置正确：

```
mc event list myminio/images
```

你应该可以收到如下的响应：

```
arn:minio:sqs::1:webhook   s3:ObjectCreated:*   Filter: suffix=".jpg"
```

### 第三步：采用Thumbnailer进行验证

我们使用 [Thumbnailer](https://github.com/minio/thumbnailer) 来监听MinIO通知。如果有文件上传于是MinIO服务，Thumnailer监听到该通知，生成一个缩略图并上传到MinIO服务。
安装Thumbnailer:

```
git clone https://github.com/minio/thumbnailer/
npm install
```

然后打开Thumbnailer的``config/webhook.json``配置文件，添加有关MinIO server的配置，使用下面的方式启动Thumbnailer:

```
NODE_ENV=webhook node thumbnail-webhook.js
```

Thumbnailer运行在``http://localhost:3000/``。下一步，配置MinIO server,让其发送消息到这个URL（第一步提到的），并使用 ``mc`` 来设置存储桶通知（第二步提到的）。然后上传一张图片到MinIO server:

```
mc cp ~/images.jpg myminio/images
.../images.jpg:  8.31 KB / 8.31 KB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 100.00% 59.42 KB/s 0s
```

稍等片刻，然后使用mc ls检查存储桶的内容 -，你将看到有个缩略图出现了。

```
mc ls myminio/images-thumbnail
[2017-02-08 11:39:40 IST]   992B images-thumbnail.jpg
```


<a name="NSQ"></a>

## 发布MinIO事件到NSQ

从[这儿](https://nsq.io/)安装一个NSQ. 或者使用Docker命令启动一个nsq daemon:

```
docker run --rm -p 4150-4151:4150-4151 nsqio/nsq /nsqd
```

### 第一步: 添加NSQ endpoint到MinIO

MinIO支持持久事件存储。持久存储将在NSQ broker离线时备份事件，并在broker恢复在线时重播事件。事件存储的目录可以通过`queue_dir`字段设置，存储的最大限制可以通过`queue_limit`设置。例如, `queue_dir`可以设置为`/home/events`, 并且`queue_limit`可以设置为`1000`. 默认情况下 `queue_limit` 是100000.

更新配置前, 使用`mc admin config get`命令获取`notify_nsq`的当前配置.

```
KEY:
notify_nsq[:name]  发布存储桶通知到NSQ endpoints

ARGS:
nsqd_address*    (address)   NSQ server地址，例如 '127.0.0.1:4150'
topic*           (string)    NSQ topic
tls              (on|off)    设为'on'代表启用TLS
tls_skip_verify  (on|off)    跳过TLS证书验证, 默认是"on" (可信的)
queue_dir        (path)      未发送消息的暂存目录 例如 '/home/events'
queue_limit      (number)    未发送消息的最大限制, 默认是'100000'
comment          (sentence)  可选的注释说明
```
 
或者通过环境变量（说明参见上面）
```
KEY:
notify_nsq[:name]  publish bucket notifications to NSQ endpoints

ARGS:
MINIO_NOTIFY_NSQ_ENABLE*          (on|off)    enable notify_nsq target, default is 'off'
MINIO_NOTIFY_NSQ_NSQD_ADDRESS*    (address)   NSQ server address e.g. '127.0.0.1:4150'
MINIO_NOTIFY_NSQ_TOPIC*           (string)    NSQ topic
MINIO_NOTIFY_NSQ_TLS              (on|off)    set to 'on' to enable TLS
MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY  (on|off)    trust server TLS without verification, defaults to "on" (verify)
MINIO_NOTIFY_NSQ_QUEUE_DIR        (path)      staging dir for undelivered messages e.g. '/home/events'
MINIO_NOTIFY_NSQ_QUEUE_LIMIT      (number)    maximum limit for undelivered messages, defaults to '100000'
MINIO_NOTIFY_NSQ_COMMENT          (sentence)  optionally add a comment to this setting
```

```sh
$ mc admin config get myminio/ notify_nsq
notify_nsq:1 nsqd_address="" queue_dir="" queue_limit="0"  tls_enable="off" tls_skip_verify="off" topic=""
```

使用`mc admin config set`命令更新配置后，重启MinIO Server让配置生效。 如果一切顺利，MinIO Server会在启动时输出一行信息，类似 `SQS ARNs: arn:minio:sqs::1:nsq`。

```sh
$ mc admin config set myminio notify_nsq:1 nsqd_address="127.0.0.1:4150" queue_dir="" queue_limit="0" tls_enable="off" tls_skip_verify="on" topic="minio"
```

请注意, 根据你的需要，你可以添加任意多个NSQ daemon endpoint，只要提供NSQ实例的标识符（如上例中的"1"）和每个实例配置参数的信息即可。

### 第二步：使用MinIO客户端启用bucket通知

我们现在可以在一个叫`images`的存储桶上开启事件通知，一旦上有文件上传到存储桶中，事件将被触发。在这里，ARN的值是`arn:minio:sqs::1:nsq`。更多有关ARN的资料，请参考[这里](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

```
mc mb myminio/images
mc event add  myminio/images arn:minio:sqs::1:nsq --suffix .jpg
mc event list myminio/images
arn:minio:sqs::1:nsq s3:ObjectCreated:*,s3:ObjectRemoved:* Filter: suffix=”.jpg”
```

### 第三步: 验证NSQ

最简单的测试是从[nsq github](https://github.com/nsqio/nsq/releases)下载`nsq_tail`。

```
./nsq_tail -nsqd-tcp-address 127.0.0.1:4150 -topic minio
```

打开另一个终端，上传一个JPEG图片到`images`存储桶.

```
mc cp gopher.jpg myminio/images
```

上传完成后，您应该通过NSQ收到以下事件通知。

```
{"EventName":"s3:ObjectCreated:Put","Key":"images/gopher.jpg","Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"","eventTime":"2018-10-31T09:31:11Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"21EJ9HYV110O8NVX2VMS"},"requestParameters":{"sourceIPAddress":"10.1.1.1"},"responseElements":{"x-amz-request-id":"1562A792DAA53426","x-minio-origin-endpoint":"http://10.0.3.1:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"images","ownerIdentity":{"principalId":"21EJ9HYV110O8NVX2VMS"},"arn":"arn:aws:s3:::images"},"object":{"key":"gopher.jpg","size":162023,"eTag":"5337769ffa594e742408ad3f30713cd7","contentType":"image/jpeg","userMetadata":{"content-type":"image/jpeg"},"versionId":"1","sequencer":"1562A792DAA53426"}},"source":{"host":"","port":"","userAgent":"MinIO (linux; amd64) minio-go/v6.0.8 mc/DEVELOPMENT.GOGET"}}]}
```
