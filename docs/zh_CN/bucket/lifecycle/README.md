# 存储桶生命周期配置快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

在存储桶上启用对象的生命周期配置，可以设置在指定天数或指定日期后自动删除对象。

## 1. 前提条件
- 安装MinIO - [MinIO快速入门指南](https://docs.min.io/cn/minio-quickstart-guide).
- 安装`mc` - [mc快速入门指南](https://docs.minio.io/cn/minio-client-quickstart-guide.html)

## 2. 启用存储桶生命周期配置

- 创建一个存储桶的生命周期配置，该配置让前缀`old/`下的对象在`2020-01-01T00:00:00.000Z`过期，同时前缀`temp/`下的对象在7天后过期。
- 使用`mc`启用存储桶的生命周期配置:

```sh
$ mc ilm import play/testbucket
{
    "Rules": [
        {
            "Expiration": {
                "Date": "2020-01-01T00:00:00.000Z"
            },
            "ID": "OldPictures",
            "Filter": {
                "Prefix": "old/"
            },
            "Status": "Enabled"
        },
        {
            "Expiration": {
                "Days": 7
            },
            "ID": "TempUploads",
            "Filter": {
                "Prefix": "temp/"
            },
            "Status": "Enabled"
        }
    ]
}

Lifecycle configuration imported successfully to `play/testbucket`.
```

- 列出当前的设置
```
$ mc ilm list play/testbucket
     ID     |  Prefix  |  Enabled   | Expiry |  Date/Days   |  Transition  |    Date/Days     |  Storage-Class   |       Tags
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
OldPictures |   old/   |    ✓       |  ✓     |  1 Jan 2020  |     ✗        |                  |                  |
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
TempUploads |  temp/   |    ✓       |  ✓     |   7 day(s)   |     ✗        |                  |                  |
------------|----------|------------|--------|--------------|--------------|------------------|------------------|------------------
```

## 进一步探索
- [MinIO | Golang Client API文档](https://docs.min.io/cn/golang-client-api-reference.html#SetBucketLifecycle)
- [对象生命周期管理](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html)
