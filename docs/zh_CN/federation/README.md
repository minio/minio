# 联合快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
本文档说明如何用`Bucket lookup from DNS` 样式联合配置MinIO 。

## 开始

### 1.前提条件
安装MinIO - [MinIO快速入门指南](https://docs.min.io/docs/minio-quickstart-guide)。

### 2.以联合模式运行MinIO
来自DNS联合的存储桶查找需要两个依赖项

- etcd（用于配置，存储桶SRV记录）
- CoreDNS（用于基于填充桶SRV记录的DNS管理，可选）

## 架构

![bucket-lookup](https://github.com/minio/minio/blob/master/docs/federation/lookup/bucket-lookup.png?raw=true)

### 环境变量

#### MINIO_ETCD_ENDPOINTS

这是您要用作MinIO联合后端的以逗号分隔的etcd服务器列表。这个在联合部署中应该相同，即联合部署中所有MinIO实例都应使用相同的etcd后端。

#### MINIO_DOMAIN

这是用于联合设置的顶级域名。理想情况下，此域名应解析为在所有联合MinIO实例之前运行的负载均衡器，并被用于为etcd创建子域条目。例如，如果域名设置为 `domain.com`，存储桶 `bucket1`，`bucket2` 可由 `bucket1.domain.com` 和 `bucket2.domain.com` 访问。

#### MINIO_PUBLIC_IPS

这是在此MinIO实例上创建的存储区将解析为的逗号分隔的IP地址列表。例如，在当前MinIO实例上创建的存储桶 `bucket1` 将可以访问 `bucket1.domain.com`，并且DNS条目 `bucket1.domain.com` 将指向设置的IP地址 `MINIO_PUBLIC_IPS`。

_注意_

- 对于单点和擦除代码MinIO服务器部署而言，此字段是必需的，以启用联合模式。
- 对于分布式部署，此字段是可选的。如果未在联合设置中设置此字段，我们将使用传递给MinIO服务器启动的主机IP地址，并将它们用于DNS条目。

### 运行多个集群
> 集群1

```sh
export MINIO_ETCD_ENDPOINTS="http://remote-etcd1:2379,http://remote-etcd2:4001"
export MINIO_DOMAIN=domain.com
export MINIO_PUBLIC_IPS=44.35.2.1,44.35.2.2,44.35.2.3,44.35.2.4
minio server http://rack{1...4}.host{1...4}.domain.com/mnt/export{1...32}
```

> 集群2

```sh
export MINIO_ETCD_ENDPOINTS="http://remote-etcd1:2379,http://remote-etcd2:4001"
export MINIO_DOMAIN=domain.com
export MINIO_PUBLIC_IPS=44.35.1.1,44.35.1.2,44.35.1.3,44.35.1.4
minio server http://rack{5...8}.host{5...8}.domain.com/mnt/export{1...32}
```

在此配置中，您可以看到 `MINIO_ETCD_ENDPOINTS` 指向管理MinIO的 `config.json` 和存储桶DNS SRV记录的etcd后端。`MINIO_DOMAIN` 表示将用于通过DNS解析存储桶的存储桶的域后缀。例如，如果您有一个诸如 `mybucket` 的存储桶，则客户端现在可以通过使用 `mybucket.domain.com` 将自身直接解析为正确的集群。 `MINIO_PUBLIC_IPS` 指向可以访问每个集群的公共IP地址，这对于每个集群都是唯一的。

注意：`mybucket` 仅存在于一个集群上，`cluster1` 或者 `cluster2`。这是随机的，并且由 `domain.com` 是如何被解析的所决定，如果在 `domain.com` 中存在循环DNS，则哪个集群可以配置存储桶是随机的。

### 3.升级到etcdv3 API

运行从 `RELEASE.2018-06-09T03-43-35Z` 到 `RELEASE.2018-07-10T01-42-11Z` MinIO联合的用户应该将etcd服务器上的现有存储桶数据迁移到 `etcdv3` API，并在将MinIO服务器更新到最新版本之前更新CoreDNS版本到 `1.2.0`。

这里有一些关于这为什么是需要的背景- 从 `RELEASE.2018-06-09T03-43-35Z` 到 `RELEASE.2018-07-10T01-42-11Z` 的服务器版本使用etcdv2 API将桶数据存储到etcd服务器上。这是因为CoreDNS服务器不支持 `etcdv3` 服务。因此，即使MinIO使用 `etcdv3`  API来存储桶数据，CoreDNS也无法读取并将其当作DNS记录。

既然CoreDNS[支持etcdv3](https://coredns.io/2018/07/11/coredns-1.2.0-release/)，MinIO服务器就使用 `etcdv3 API` 将桶数据存储到etcd服务器中。由于 `etcdv2` 和 `etcdv3` API是不兼容，使用 `etcdv2` API存储的数据对于 `etcdv3` API是不可见的。因此，在完成迁移之前，先前MinIO版本存储的存储桶数据对于当前版本的MinIO是不可见的。

CoreOS团队在[这个博客帖子](https://coreos.com/blog/migrating-applications-etcd-v3.html)记录了将数据从 `etcdv2` 迁移到 `etcdv3` 所需的步骤。请参考帖子并将etcd数据迁移到 `etcdv3`  API。

### 4.测试您的设置

要测试此设置，请通过浏览器或[`mc`](https://docs.min.io/docs/minio-client-quickstart-guide)访问MinIO服务器。您将发现上传的文件可以从所有的MinIO端点访问。

## 进一步探索

- [一起使用`mc`和MinIO服务](https://docs.min.io/docs/minio-client-quickstart-guide)
- [一起使用`aws-cli` 和MinIO服务](https://docs.min.io/docs/aws-cli-with-minio)
- [一起使用`s3cmd`和MinIO服务](https://docs.min.io/docs/s3cmd-with-minio)
- [一起使用`minio-go`服务](https://docs.min.io/docs/golang-client-quickstart-guide)
- [MinIO文档网站](https://docs.min.io/)

