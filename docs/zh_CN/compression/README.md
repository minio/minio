# 压缩指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO Server 允许压缩流以确保有效使用磁盘空间。压缩是在传输中做的，也就是说，对象在写入磁盘之前已被压缩。MinIO使用 [`klauspost/compress/s2`](https://github.com/klauspost/compress/tree/master/s2) 进行流压缩，因为它高效稳定。

该算法专门针对机器生成的内容进行了优化。每个CPU核心的写入吞吐量至少为300MB/s。 解压缩速度通常至少为1GB/s。
这意味着在原始IO低于这些数字的情况下，压缩不仅会减少磁盘使用量，而且有助于提高系统吞吐量。
通常，当内容可以压缩时，在机械硬盘系统上启用压缩将提高速度。

## 开始使用

### 1. 前置条件

安装MinIO - [MinIO快速入门指南](https://docs.min.io/cn/minio-quickstart-guide).

### 2. 为MinIO启用压缩

可以通过更新MinIO Server配置的`compress`设置来启用压缩。`compress`设置可以配置哪些扩展名和mime-types可以被压缩。

```
$ mc admin config get myminio compression
compression extensions=".txt,.log,.csv,.json,.tar,.xml,.bin" mime_types="text/*,application/json,application/xml"
```

默认配置包括最常见的高度可压缩的内容扩展名和mime类型。

```
$ mc admin config set myminio compression extensions=".pdf" mime_types="application/pdf"
```

显示有关设置压缩配置的帮助。
```
~ mc admin config set myminio compression
```

使用默认的扩展名和mime-types对所有内容启用压缩。
```
~ mc admin config set myminio compression enable="on"
```

压缩设置也可以通过环境变量来设置。设置后，环境变量将覆盖服务器配置中定义的`compress`配置设置。

```bash
export MINIO_COMPRESS="on"
export MINIO_COMPRESS_EXTENSIONS=".pdf,.doc"
export MINIO_COMPRESS_MIME_TYPES="application/pdf"
```

### 3. 注意

- 已压缩的对象不具有可压缩的模式，因此不适合再压缩。 此类对象无法产生有效的 [`LZ 压缩`](https://en.wikipedia.org/wiki/LZ77_and_LZ78)。以下是不适合压缩的常见文件和内容类型列表。 

    - 扩展名

      | `gz` | (GZIP)
      | `bz2` | (BZIP2)
      | `rar` | (WinRAR)
      | `zip` | (ZIP)
      | `7z` | (7-Zip)
      | `xz` | (LZMA)
      | `mp4` | (MP4)
      | `mkv` | (MKV media)
      | `mov` | (MOV)

    - 内容类型

      | `video/*` |
      | `audio/*` |
      | `application/zip` |
      | `application/x-gzip` |
      | `application/zip` |
      | `application/x-bz2` |
      | `application/x-compress` |
      | `application/x-xz` |

即使所有类型都启用了压缩，具有以上这些扩展名和mime类型的文件也不会被压缩。

- MinIO不支持压缩加密，因为压缩和加密一起可能为[`CRIME 和 BREACH`](https://blog.minio.io/c-e-compression-encryption-cb6b7f04a369)等信道攻击留出空间。

- MinIO不支持网关（Azure/GCS/NAS）压缩。

## 测试设置

要测试设置, 使用`mc`PUT一个文件到服务器，然后在数据目录上使用 `mc ls`查看对象的大小。

## 进一步探索

- [使用`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用`aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用`s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用`minio-go`](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO官方文档](https://docs.min.io/cn)
