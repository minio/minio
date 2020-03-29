# 超大存储桶快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO的纠删码功能限制了最多只能使用16块磁盘。这就限制了一个租户的数据可以用多少存储空间。不过，为了满足需要大量磁盘或者是超大容量存储空间的需求，我们引入了超大存储桶的支持。

如果一个MinIO存储桶可以扩展到多个纠删码部署集合，我们就称之为超大存储桶。不需要做任何特殊设置，它就可以帮助你创建PB级的存储系统。为了超大存储桶支持，你就可以在部署MinIO服务时使用超过16块磁盘。MinIO在内部创建了多个纠删码集合，这些集合又进一步组合成一个命名空间。本文将对超大存储桶的部署做一个简单的介绍。想了解更多，请参考[设计文档](https://github.com/minio/minio/blob/master/docs/large-bucket/DESIGN.md)。

## 开始
安装和部署方式和分布式MinIO一样。只不过是在输入参数的语法上，用`...`来做为磁盘参数的简写。分布式设置中的远程磁盘被编码为HTTP（s）URI，它也可以被同样的缩写。

### 1. 前提条件
安装MinIO - [MinIO快速入门](https://docs.min.io/docs/minio-quickstart-guide)。

### 2. 在多个磁盘上运行MinIO
我们将在下面的章节中看到如何做到这一点的例子。

*注意*

- 运行分布式MinIO的所有节点都需要具有相同的access key和secret key。为此，我们在执行MinIO服务器命令之前将access key和secret key导出为所有节点上的环境变量。
- 下面的驱动器路径仅用于演示目的，你需要将其替换为实际的路径/文件夹。

#### 多磁盘下的MinIO超大存储桶 (独立模式)
你需要有多块磁盘，例如 `/export1, /export2 .... /export24`。 然后在你想要启动MinIO的所有节点上运行以下命令。

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server /export{1...24}
```

#### 多磁盘下的MinIO超大存储桶 (分布式模式)
你需要有多块在不同服务器上的磁盘，例如`http://host1/export1, http://host2/export2 .... http://host4/export16`。 然后在你想要启动MinIO的所有节点上运行以下命令。

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...4}/export{1...16}
```

### 3. 验证设置是否成功
要验证是否部署成功，你可以通过浏览器或者[`mc`](https://docs.min.io/docs/minio-client-quickstart-guide)来访问刚刚部署的MinIO服务。你应该可以看到上传的文件在所有MinIO节点上都可以访问。

## 了解更多
- [mc快速入门](https://docs.min.io/docs/minio-client-quickstart-guide)
- [使用 aws-cli](https://docs.min.io/docs/aws-cli-with-minio)
- [使用 s3cmd](https://docs.min.io/docs/s3cmd-with-minio)
- [使用 minio-go SDK](https://docs.min.io/docs/golang-client-quickstart-guide)
- [MinIO文档](https://docs.min.io)