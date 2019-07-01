# 压缩指南 [![slack](https://slack.min.io/slack?type=svg)](https://docs.min.io/docs/minio-quickstart-guide)

MinIO服务器允许流式压缩来确保有效的磁盘空间使用。压缩发生在过程中，即在写入磁盘之前压缩对象。由于golang/snappy流式压缩出色的稳定性和性能，MinIO使用[`golang/snappy`](https://github.com/golang/snappy)流式压缩。

## 入门

### 1.先决条件

安装MinIO - [MinIO快速入门指南](https://docs.min.io/cn/minio-quickstart-guide)。

### 2.使用压缩运行MinIO

可以通过更新MinIO服务器配置的`compress`配置项来启用压缩。配置项`compress`的设置需要压缩扩展和mime类型。

``` json
"compress": {
        "enabled": true,
        "extensions": [".txt",".log",".csv", ".json"],
        "mime-types": ["text/csv","text/plain","application/json"]
}
```

由于text，log，csv，json文件具有高度可压缩性，因此默认情况下会包含这些extensions/mime类型以进行压缩。

要更新配置，请使用`mc admin config get`命令以json格式获取minio集群的当前配置文件，并将其保存在本地。

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

更新/tmp/myconfig中的压缩配置后，使用`mc admin config set`命令更新集群的配置。重新启动MinIO服务器以使更改生效。

```sh
$ mc admin config set myminio < /tmp/myconfigr
```

压缩设置也可以通过环境变量来设置。设置后，环境变量将覆盖服务器配置中定义的`compress`配置设置。

``` bash
export MINIO_COMPRESS="true"
export MINIO_COMPRESS_EXTENSIONS=".pdf,.doc"
export MINIO_COMPRESS_MIMETYPES="appli"
```

### 3.注意

已经压缩的对象不适合再次压缩，因为它们没有可压缩的模式。这些对象不能产生有效的`Run-length encoding (RLE)`，而`Run-length-encoding`是无损数据压缩的适应因子。以下是不适合压缩的常见文件和内容类型的列表。

- 扩展

| `gz` | （GZIP）
| `bz2` | （BZIP2）
| `rar` | （WinRAR）
| `zip` | （ZIP）
| `7z` | （7-Zip）


- 内容类型

| `video/*` | 
| `audio/*` | 
| `application/zip` | 
| `application/x-gzip` | 
| `application/zip` | 
| `application/x-compress` | 
| `application/x-spoon` |

- MinIO不支持使用压缩加密，因为同时压缩和加密为像[`CRIME and BREACH`](https://en.wikipedia.org/wiki/CRIME)的侧通道攻击提供了可能性。 

- MinIO不支持网关（Azure/GCS/NAS)实施的压缩。

## 测试设置

要测试此设置，可以用`mc`将调用放入服务器并使用`mc ls`数据目录来查看对象的大小。

## 进一步探索

- [一起使用`mc`与MinIO Server](https://docs.min.io/cn/minio-client-quickstart-guide)
- [一起使用`aws-cli`与MinIO Server](https://docs.min.io/cn/aws-cli-with-minio)
- [一起使用`s3cmd`与MinIO Server](https://docs.min.io/cn/s3cmd-with-minio)
- [一起使用`minio-goSDK`与MinIO Server](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO文档网站](https://docs.min.io/cn)
