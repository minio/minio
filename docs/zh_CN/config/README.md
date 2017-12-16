# Minio Server `config.json` (v18) 指南 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio server在默认情况下会将所有配置信息存到 `${HOME}/.minio/config.json` 文件中。 以下部分提供每个字段的详细说明以及如何自定义它们。一个完整的 `config.json` 在 [这里](https://raw.githubusercontent.com/minio/minio/master/docs/config/config.sample.json)

## 配置目录
默认的配置目录是 `${HOME}/.minio`，你可以使用`--config-dir`命令行选项重写之。  You can override the default configuration directory using `--config-dir` command-line option. Minio server在首次启动时会生成一个新的`config.json`，里面带有自动生成的访问凭据。 

```sh
minio server --config-dir /etc/minio /data
```

### 证书目录
TLS证书存在``${HOME}/.minio/certs``目录下，你需要将证书放在该目录下来启用`HTTPS` 。如果你是一个乐学上进的好青年，这里有一本免费的秘籍传授一你: [如何使用TLS安全的访问minio](http://docs.minio.io/docs/zh_CN/how-to-secure-access-to-minio-server-with-tls).

以下是一个带来TLS证书的Minio server的目录结构。

```sh
$ tree ~/.minio
/home/user1/.minio
├── certs
│   ├── CAs
│   ├── private.key
│   └── public.crt
└── config.json
```

### 配置参数
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
export MINIO_REGION="中国华北一区"
minio server /data
```

#### 浏览器
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
|``notify.amqp``| |[通过AMQP发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#AMQP)|
|``notify.mqtt``| |[通过MQTT发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#MQTT)|
|``notify.elasticsearch``| |[通过Elasticsearch发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#Elasticsearch)|
|``notify.redis``| |[通过Redis发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#Redis)|
|``notify.nats``| |[通过NATS发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#NATS)|
|``notify.postgresql``| |[通过PostgreSQL发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#PostgreSQL)|
|``notify.kafka``| |[通过Apache Kafka发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#apache-kafka)|
|``notify.webhook``| |[通过Webhooks发布Minio事件](http://docs.minio.io/docs/zh_CN/minio-bucket-notification-guide#webhooks)|

## 了解更多
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
