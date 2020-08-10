# 存储桶配额配置快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

![quota](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/quota/bucketquota.png)

存储桶有两种配额类型可供选择，分别是FIFO和Hard。

- `Hard` 表示达到配置的配额限制后，禁止向存储桶写入数据。
- `FIFO` 会自动删除最旧的内容，直到存储桶的空间使用在限制范围内，同时允许写入。

> 注意：网关或独立单磁盘模式下不支持存储桶配额。

## 前置条件
- 安装MinIO - [MinIO快速入门指南](https://docs.min.io/cn/minio-quickstart-guide).
- [`mc`和MinIO Server一起使用](https://docs.min.io/cn/minio-client-quickstart-guide)

## 设置存储桶配额

### 在MinIO对象存储上，设置存储桶`mybucket`的额度为1GB，配额类型为hard:

```sh
$ mc admin bucket quota myminio/mybucket --hard 1gb
```

### 将MinIO上的存储桶"mybucket"的额度设置为5GB，配额类型为FIFO，这样就会自动删除较旧的内容，以确保存储桶的空间使用保持在5GB以内

```sh
$ mc admin bucket quota myminio/mybucket --fifo 5gb
```

### 验证MinIO上的存储桶`mybucket`的配额设置

```sh
$ mc admin bucket quota myminio/mybucket
```

### 清除MinIO上的存储桶`mybucket`的配额设置

```sh
$ mc admin bucket quota myminio/mybucket --clear
```
