# 对象锁定指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO服务器允许针对特定的对象一次写入，多次读取 (WORM),或者通过使用默认的对象锁定配置设置存储桶，该配置将默认的保留模式和保留期限应用于所有对象。这将使存储桶中的对象不可改变，也就是说，直到存储桶的对象锁定配置或对象保留中指定的时间到期后，才允许删除版本。

对象锁定要求在创建存储桶时启用锁定，而且还会自动在存储桶上启用版本控制。此外，可以在存储桶上配置默认保留期限和保留模式，以应用于该存储桶中创建的对象。

和保留期限无关，一个对象也可以被依法保留。依法保留的对象将一直有效（不可变），直到通过API调用删除依法保留为止。

## 开始使用

### 1. 前置条件

- 安装 MinIO - [MinIO快速入门指南](https://docs.min.io/cn/minio-quickstart-guide)
- 安装 `awscli` - [Installing AWS Command Line Interface](https://docs.aws.amazon.com/zh_cn/cli/latest/userguide/cli-chap-install.html)

### 2. 设置存储桶WORM配置

通过设置对象锁定配置可以启用存储桶的一次写入，多次读取 (WORM)模式。此配置将应用于存储桶中的现有对象和新对象。下面的例子设置了`Governance`模式，并且`mybucket`里的所有对象自创建时起，保留一天的时间。

```sh
$ awscli s3api put-object-lock-configuration --bucket mybucket --object-lock-configuration 'ObjectLockEnabled=\"Enabled\",Rule={DefaultRetention={Mode=\"GOVERNANCE\",Days=1}}'
```

### 设置对象锁定

PutObject API 可以通过`x-amz-object-lock-mode`和`x-amz-object-lock-retain-until-date`请求头设置每一个对象的保留模式和保留期限。这要比存储桶上设置的对象锁定配置优先级高。

```sh
aws s3api put-object --bucket testbucket --key lockme --object-lock-mode GOVERNANCE --object-lock-retain-until-date "2019-11-20"  --body /etc/issue
```

请参阅 https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/object-lock-overview.html 以获取有关对象锁定、governance bypass所需权限的AWS S3规范。

### 设置依法保留对象

PutObject API 可以通过 `x-amz-object-lock-legal-hold` 请求头设置依法保留.

```sh
aws s3api put-object --bucket testbucket --key legalhold --object-lock-legal-hold-status ON --body /etc/issue
```

请参阅 https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/object-lock-overview.html 以获取有关对象锁定、指定依法保留所需的权限。

## 概念
- 如果一个对象被依法保留,除非明确删除各个版本ID的依法保留，否则无法删除该对象。 DeleteObjectVersion() 会失败。
- 在 `Compliance` 模式中, 在各个版本ID的保留期限到期之前，任何人都无法删除对象。如果用户具有必需的governance bypass权限，则可以在`Compliance`模式下延长对象的保留期限。
- 一旦在一个存储桶上设置对象锁定设置后
  - 新对象会自动继承存储桶对象锁定配置的保留设置
  - 上传对象时可以选择是否设置保留头信息
  - 会对对象调用PutObjectRetention API
- 如果不需要系统时间来设置保留日期，则可以将*MINIO_NTP_SERVER*环境变量设置为远程NTP server endpoint。
- **对象锁定功能仅在纠删码和分布式纠删码模式下可用**。

## 进一步探索

- [使用`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用`aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用`s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用`minio-go`](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO文档](https://docs.min.io/cn)
