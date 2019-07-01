# Minio Server 配置指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## 配置目录
直到MinIO发布版本`RELEASE.2018-02T23-11-36Z`，MinIO服务配置文件（`config.json`）都是被存储在被`--config-dir`指定的或默认的`${HOMR}/.minio`的配置目录下。然而从版本`RELEASE.2018-08-18T-49-57Z`后，配置文件（仅仅），已经被迁移到存储后端(存储后端是在启动服务器时传递给MinIO服务器的目录)。

你能够使用`--config-dir`指定你已存在的配置文件的位置，MinIO将会迁移`config.json`到存储后端。你当前的config.json将会在成功迁移后被重新命名为你当前`--config-dir`下的`config.json.deprecated`。全部存在的配置在迁移后都将被认可。

此外，`--config-dir`现在是一个遗留选项，将来会被删除。因此，请相应地更新你当地的启动以及ansible脚本。

```sh
minio server /data
```

### 证书目录
TLS证书默认存在于``${HOME}/.minio/certs``目录下，你需要将证书放在该目录下来启用`HTTPS` 。如果你是一个乐学上进的好青年，这里有一本免费的秘籍传授一你: [如何使用TLS安全的访问minio](https://docs.min.io/cn/how-to-secure-access-to-minio-server-with-tls).

以下是一个带来TLS证书的MinIO server的目录结构。

```sh
$ tree ~/.minio
/home/user1/.minio
├── certs
│   ├── CAs
│   ├── private.key
│   └── public.crt
```

你可以用`--certs-dir`命令行选项来提供一个个性化的certs目录。

### 访问配置文件
所有的配置变动都可以用`mc admin config`的[get/set](https://github.com/minio/mc/blob/master/cn/minio-admin-complete-guide.md)命令。接下来的部分提供字段的简短解释以及如何使它们个性化。[这里](https://raw.githubusercontent.com/minio/minio/master/docs/config/config.sample.json)提供完整的`config.json`示例。

#### 编辑文件字段配置

##### 获取MinIO部署的当前配置

```sh
$mc admin config get myminio/> /tmp/myconfig
```

##### 设置MinIO部署的当前配置

```sh
$mc admin config set myminio < /tmp/myconfig
```

这个`mc admin`配置API将会很快发展，以便能够使用get/set命令配置具体字段


#### 版本
|参数|类型|描述|
|:---|:---|:---|
|``version``|_string_| `version`决定了配置文件的格式，任何老版本都会在启动时自动迁移到新版本中。 [请勿手动修改]|

#### 凭据
|参数|类型|描述|
|:---|:---|:---|
|``credential``| |对象存储和Web访问的验证凭据。|
|``credential.accessKey`` | _string_ | Access key长度最小是5个字符，你可以通过 `MINIO_ACCESS_KEY`环境变量进行修改|
|``credential.secretKey`` | _string_ | Secret key长度最小是8个字符，你可以通过`MINIO_SECRET_KEY`环境变量进行修改|

> 注意：在分布式配置中，必须使用环境变量MINIO_ACCESS_KEY和MINIO_SECRET_KEY作为凭据。

示例:

```sh
export MINIO_ACCESS_KEY=admin
export MINIO_SECRET_KEY=password
minio server /data
```

#### 区域（Region）
|参数|类型|描述|
|:---|:---|:---|
|``region``| _string_ | `region`描述的是服务器的物理位置，默认是`us-east-1`（美国东区1）,这也是亚马逊S3的默认区域。你可以通过`MINIO_REGION` 环境变量进行修改。如果不了解这块，建议不要随意修改|

示例:

```sh
export MINIO_REGION="my_region"
minio server /data
```

 #### 蠕虫（Worm）
 |字段|类型|描述|
 |--|--|--|
 |``worm``|_string_|启用此选项可启用“一次写入多次读取”。 默认情况下，它设置为关闭。 您可以使用``MINIO_WORM``环境变量覆盖此字段。|

实例：
```sh
export MINIO_WORM=on
minio server /data
```

### 存储类

|字段|类型|描述|
|---|---|---|
|``storageclass``||为配置数据和分区设置存储类，作为每个对象的基础|
|``stroageclass.standard``|_string_|标准存储类的值。它应该使用`EC:Parity`格式，例如要想为标准存储类对象设置4个磁盘分区，可将这个字段设置为`EC:4`|
|``strageclass.rrs|_string_``|减少冗余存储类的值。它应该使用`EC:Parity`,例如要想为减少冗余的存储类对象设置3个磁盘分区，可将这个字段设置为`EC:3`|

在默认情况下，标准存储类对象的分区被设置为`N/2`，而减少冗余存储类对象的分区被设置为`2`。可在[这里](https://github.com/minio/minio/blob/master/docs/erasure/storage-class/README.md)了解更多的MinIO服务的存储类支持。

### 缓冲（Cache）

|字段|类型|描述|
|--|--|--|
|``drives``| _[]string_ | 已启用[`atime`](http://kerolasa.github.io/filetimes.html)支持的已装入文件系统磁盘列表|
|``exclude``|_[]string_|要从缓存中排除的前缀通配符模式列表|
|``expiry``|_int_|缓存到期的天数|
|``maxuse``|_int_|可用于缓冲的磁盘的百分比|

#### 通知（Notify）
|参数|类型|描述|
|:---|:---|:---|
|``notify``| |通知通过以下方式开启存储桶事件通知，用于lambda计算|
|``notify.amqp``| |[通过AMQP发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#AMQP)|
|``notify.nats``| |[通过NATS发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#NATS)|
|``notify.elasticsearch``| |[通过Elasticsearch发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#Elasticsearch)|
|``notify.redis``| |[通过Redis发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#Redis)|
|``notify.postgresql``| |[通过PostgreSQL发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#PostgreSQL)|
|``notify.kafka``| |[通过Apache Kafka发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#apache-kafka)|
|``notify.webhook``| |[通过Webhooks发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#webhooks)|
|`notify.mysql`||[通过mysql发布的MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#MySQL)|
|``notify.mqtt``| |[通过MQTT发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#MQTT)|

## 环境配置

### 浏览器

启用或禁用对web界面的访问。在默认情况下，它被设置为`on`。你能够用环境变量 `MINIO_BROWSER` 覆盖这个字段。

示例:

```sh
export MINIO_BROWSER=off
minio server /data
```

### 域名（Domain）

默认情况下，MinIO支持格式为[http://mydomain.com/bucket/object](http://mydomain.com/bucket/object) 的路径样式请求MINIO_DOMAIN环境变量用于启用虚拟主机样式的请求。如果请求主机头与`(.+).mydomain.com`匹配，则匹配的模式`$1`用作存储桶，路径用作对象。 有关路径样式和虚拟主机样式的更多信息，请参见[此处](http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAPI.html)

示例：
```sh
export MINIO_DOMAIN=mydomain.com
minio server /data
```

对于高级用例，`MINIO_DOMAIN`环境变量支持具有逗号分隔值的多域。
```sh
export MINIO_DOMAIN=sub1.mydomain.com,sub2.mydomain.com
minio server /data
```

### 磁盘同步

在默认情况下，MinIO以同步模式写入磁盘以进行所有元数据操作。也可以设置`MINIO_DRIVE_SYNC`环境变量去启动同步模式去执行所有的数据操作。

示例：
```
export MINIO_DRIVE_SYNC=on
minio server /data
```

### HTTP跟踪
在默认情况下，MinIO禁用记录HTTP跟踪的特性。你可以通过设置`MINIO_HTTP_TRACE`的环境变量来启用这个特性。

示例： 
```sh
export MINIO_HTTP_TRACE=/var/log/minio.log
minio server /data
```

## 了解更多
* [MinIO快速指南](https://docs.min.io/cn/minio-quickstart-guide)
* [TLS配置MinIO服务](https://docs.min.io/cn/how-to-secure-access-to-minio-server-with-tls)