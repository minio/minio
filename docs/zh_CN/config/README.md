# MinIO Server 配置指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## 配置目录

默认的配置目录是 `${HOME}/.minio`，你可以使用`--config-dir`命令行选项重写之。MinIO server在首次启动时会生成一个新的`config.json`，里面带有自动生成的访问凭据。 

```sh
minio server --config-dir /etc/minio /data
```

截止到 MinIO `RELEASE.2018-08-02T23-11-36Z` 版本, MinIO server 的配置文件(`config.json`) 被存储在通过 `--config-dir` 指定的目录或者默认的 `${HOME}/.minio` 目录。 但是从 `RELEASE.2018-08-18T03-49-57Z` 版本之后, 配置文件 (仅仅), 已经被迁移到存储后端 (存储后端指的是启动一个服务器的时候，传递给MinIO server的目录)。

您可以使用`--config-dir`指定现有配置的位置, MinIO 会迁移 `config.json` 配置到你的存储后端。 迁移成功后，你当前 `--config-dir` 目录中的 `config.json` 将被重命名为 `config.json.deprecated`。 迁移后，所有现有配置都将得到保留。

此外，`--config-dir`现在是一个旧配置，计划在将来删除，因此请相应地更新本地startup和ansible脚本。

```sh
minio server /data
```

MinIO还使用管理员凭据对所有配置，IAM和策略内容进行加密。

### 证书目录
TLS证书存在``${HOME}/.minio/certs``目录下，你需要将证书放在该目录下来启用`HTTPS` 。如果你是一个乐学上进的好青年，这里有一本免费的秘籍传授一你: [如何使用TLS安全的访问minio](https://docs.min.io/cn/how-to-secure-access-to-minio-server-with-tls).

以下是一个具有TLS证书的MinIO server的目录结构。

```sh
$ mc tree --files ~/.minio
/home/user1/.minio
└─ certs
   ├─ CAs
   ├─ private.key
   └─ public.crt
```

你可以使用`--certs-dir`命令行选项提供自定义certs目录。

#### 凭据
只能通过环境变量`MINIO_ACCESS_KEY` 和 `MINIO_SECRET_KEY` 更改MinIO的admin凭据和root凭据。使用这两个值的组合，MinIO加密存储在后端的配置

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio13
minio server /data
```

##### 使用新的凭据轮换加密

另外，如果您想更改管理员凭据，则MinIO将自动检测到该凭据，并使用新凭据重新加密，如下所示。一次只需要设置如下所示的环境变量即可轮换加密配置。

> 旧的环境变量永远不会在内存中被记住，并且在使用新凭据迁移现有内容后立即销毁。在服务器再次成功重启后，你可以安全的删除它们。

```
export MINIO_ACCESS_KEY=newminio
export MINIO_SECRET_KEY=newminio123
export MINIO_ACCESS_KEY_OLD=minio
export MINIO_SECRET_KEY_OLD=minio123
minio server /data
```

迁移完成后, 服务器会自动的取消进程空间中的`MINIO_ACCESS_KEY_OLD` and `MINIO_SECRET_KEY_OLD`设置。

> **注意: 在下一次服务重新启动前，要确保移除脚本或者服务文件中的 `MINIO_ACCESS_KEY_OLD` and `MINIO_SECRET_KEY_OLD`， 避免现有的内容被双重加密**

#### 区域
```
KEY:
region  服务器的物理位置标记

ARGS:
name     (string)    服务器的物理位置名字，例如 "us-west-rack2"
comment  (sentence)  为这个设置添加一个可选的注释
```

或者通过环境变量
```
KEY:
region  服务器的物理位置标记

ARGS:
MINIO_REGION_NAME     (string)    服务器的物理位置名字，例如  "us-west-rack2"
MINIO_REGION_COMMENT  (sentence)  为这个设置添加一个可选的注释
```

示例:

```sh
export MINIO_REGION_NAME="my_region"
minio server /data
```

### 存储类型
默认情况下，标准存储类型的奇偶校验值设置为N/2，低冗余的存储类型奇偶校验值设置为2。在[此处](https://github.com/minio/minio/blob/master/docs/zh_CN/erasure/storage-class/README.md)了解有关MinIO服务器存储类型的更多信息。

```
KEY:
storage_class  定义对象级冗余

ARGS:
standard  (string)    设置默认标准存储类型的奇偶校验计数，例如"EC:4"
rrs       (string)    设置默认低冗余存储类型的奇偶校验计数，例如"EC:2"
comment   (sentence)  为这个设置添加一个可选的注释
```

或者通过环境变量
```
KEY:
storage_class  定义对象级冗余

ARGS:
MINIO_STORAGE_CLASS_STANDARD  (string)    设置默认标准存储类型的奇偶校验计数，例如"EC:4"
MINIO_STORAGE_CLASS_RRS       (string)    设置默认低冗余存储类型的奇偶校验计数，例如"EC:2"
MINIO_STORAGE_CLASS_COMMENT   (sentence)  为这个设置添加一个可选的注释
```

### 缓存
MinIO为主要的网关部署提供了缓存存储层，使您可以缓存内容以实现更快的读取速度，并节省从云中重复下载的成本。

```
KEY:
cache  添加缓存存储层

ARGS:
drives*  (csv)       逗号分隔的挂载点，例如 "/optane1,/optane2"
expiry   (number)    缓存有效期限（天），例如 "90"
quota    (number)    以百分比限制缓存驱动器的使用，例如 "90"
exclude  (csv)       逗号分隔的通配符排除模式，例如 "bucket/*.tmp,*.exe"
after    (number)    缓存对象之前的最小可访问次数
comment  (sentence)  为这个设置添加一个可选的注释
```

或者通过环境变量
```
KEY:
cache  添加缓存存储层

ARGS:
MINIO_CACHE_DRIVES*  (csv)       逗号分隔的挂载点，例如 "/optane1,/optane2"
MINIO_CACHE_EXPIRY   (number)    缓存有效期限（天），例如 "90"
MINIO_CACHE_QUOTA    (number)    以百分比限制缓存驱动器的使用，例如 "90"
MINIO_CACHE_EXCLUDE  (csv)       逗号分隔的通配符排除模式，例如 "bucket/*.tmp,*.exe"
MINIO_CACHE_AFTER    (number)    缓存对象之前的最小可访问次数
MINIO_CACHE_COMMENT  (sentence)  为这个设置添加一个可选的注释
```

### 浏览器
|参数|类型|描述|
|:---|:---|:---|
|``browser``| _string_ | 开启或关闭浏览器访问，默认是开启的，你可以通过``MINIO_BROWSER``环境变量进行修改|

示例:

```sh
export MINIO_BROWSER=off
minio server /data
```

#### 通知
|参数|类型|描述|
|:---|:---|:---|
|``notify``| |通知通过以下方式开启存储桶事件通知，用于lambda计算|
|``notify.amqp``| |[通过AMQP发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#AMQP)|
|``notify.mqtt``| |[通过MQTT发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#MQTT)|
|``notify.elasticsearch``| |[通过Elasticsearch发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#Elasticsearch)|
|``notify.redis``| |[通过Redis发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#Redis)|
|``notify.nats``| |[通过NATS发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#NATS)|
|``notify.postgresql``| |[通过PostgreSQL发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#PostgreSQL)|
|``notify.kafka``| |[通过Apache Kafka发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#apache-kafka)|
|``notify.webhook``| |[通过Webhooks发布MinIO事件](https://docs.min.io/cn/minio-bucket-notification-guide#webhooks)|

## 了解更多
* [MinIO Quickstart Guide](https://docs.min.io/cn/minio-quickstart-guide)
