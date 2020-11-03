# 分布式MinIO快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

分布式Minio可以让你将多块硬盘（甚至在不同的机器上）组成一个对象存储服务。由于硬盘分布在不同的节点上，分布式Minio避免了单点故障。   

## 分布式Minio有什么好处?

在大数据领域，通常的设计理念都是无中心和分布式。Minio分布式模式可以帮助你搭建一个高可用的对象存储服务，你可以使用这些存储设备，而不用考虑其真实物理位置。 

### 数据保护


分布式Minio采用 [纠删码](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)来防范多个节点宕机和[位衰减`bit rot`](https://github.com/minio/minio/blob/master/docs/zh_CN/erasure/README.md#what-is-bit-rot-protection)。  

分布式Minio至少需要4个硬盘，使用分布式Minio自动引入了纠删码功能。

### 高可用

单机Minio服务存在单点故障，相反，如果是一个有 _m_ 台服务器， _n_ 块硬盘的分布式Minio,只要有 _m/2_ 台服务器或者 _m*n_/2 及更多硬盘在线，你的数据就是安全的。

例如，一个16节点的Minio集群，每个节点200块硬盘，就算8台服務器宕机，即大概有1600块硬盘，这个集群仍然是可读的，不过你需要9台服務器在线才能写数据。

你还可以使用[存储类型](https://github.com/minio/minio/tree/master/docs/zh_CN/erasure/storage-class)自定义每个对象的奇偶分布。

### 一致性

Minio在分布式和单机模式下，所有读写操作都严格遵守**read-after-write**和**list-after-write**一致性模型。

# 开始吧

如果你了解Minio单机模式的搭建的话，分布式搭建的流程基本一样，Minio服务基于命令行传入的参数自动切换成单机模式还是分布式模式。 

## 1. 前提条件

安装Minio - [Minio快速入门](https://docs.min.io/cn/minio-quickstart-guide).

## 2. 运行分布式Minio

启动一个分布式Minio实例，你只需要把硬盘位置做为参数传给minio server命令即可，然后，你需要在所有其它节点运行同样的命令。

*注意* 

- 分布式Minio里所有的节点需要有同样的access秘钥和secret秘钥，这样这些节点才能建立联接。为了实现这个，__建议__ 在执行minio server命令之前，在所有节点上先将access秘钥和secret秘钥export成环境变量`MINIO_ACCESS_KEY` 和 `MINIO_SECRET_KEY`。 
- __MinIO 可创建每组4到16个磁盘组成的纠删码集合。所以你提供的磁盘总数必须是其中一个数字的倍数。__
- MinIO会根据给定的磁盘总数或者节点总数选择最大的纠删码集合大小，确保统一分布，即每个节点参与每个集合的磁盘数量相等。
- __每个对象被写入一个EC集合中，因此该对象分布在不超过16个磁盘上。__
- __建议运行分布式MinIO设置的所有节点都是同构的，即相同的操作系统，相同数量的磁盘和相同的网络互连。__
- 分布式Minio使用干净的目录，里面没有数据。你也可以与其他程序共享磁盘，这时候只需要把一个子目录单独给MinIO使用即可。例如，你可以把磁盘挂在到`/export`下, 然后把`/export/data`作为参数传给MinIO server即可。
- 下面示例里的IP仅供示例参考，你需要改成你真实用到的IP和文件夹路径。 
- 分布式Minio里的节点时间差不能超过15分钟，你可以使用[NTP](http://www.ntp.org/) 来保证时间一致。
- `MINIO_DOMAIN`环境变量应该定义并且导出,以支持bucket DNS style。
- 在Windows下运行分布式Minio处于实验阶段，请悠着点使用。

示例1: 启动分布式Minio实例，8个节点，每节点1块盘，需要在8个节点上都运行下面的命令。 
示例1: 在n个节点上启动分布式MinIO实例，每个节点有m个磁盘，分别挂载在`/export1` 到 `/exportm` (如下图所示), 在所有n个节点上运行此命令:

![Distributed MinIO, n nodes with m drives each](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_nm.png?raw=true)

#### GNU/Linux 和 macOS

```shell
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...n}/export{1...m}
```

> __注意:__ 在以上示例中`n`和`m`代表正整数, *不要直接复制粘贴它们，你应该在部署的时候改成你期望的值*.

> __注意:__ `{1...n}` 是有3个点的! 用2个点`{1..n}`的话会被shell解析导致不能传给MinIO server, 影响纠删码的顺序, 进而影响性能和高可用性. __所以要始终使用省略号 `{1...n}` (3个点!) 以获得最佳的纠删码分布__

#### 扩展现有的分布式集群
MinIO支持通过命令，指定新的集群来扩展现有集群（纠删码模式），命令行如下：

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...n}/export{1...m} http://host{o...z}/export{1...m}
```

例如:
```
minio server http://host{1...4}/export{1...16} http://host{5...12}/export{1...16}
```

现在整个集群就扩展了 _(newly_added_servers\*m)_ 个磁盘，总磁盘变为 _(existing_servers\*m)+(newly_added_servers\*m)_ 个，新的对象上传请求会自动分配到最少使用的集群上。通过以上扩展策略，您就可以按需扩展您的集群。重新配置后重启集群，会立即在集群中生效，并对现有集群无影响。如上命令中，我们可以把原来的集群看做一个区，新增集群看做另一个区，新对象按每个区域中的可用空间比例放置在区域中。在每个区域内，基于确定性哈希算法确定位置。

> __说明:__ __您添加的每个区域必须具有与原始区域相同的磁盘数量（纠删码集）大小，以便维持相同的数据冗余SLA。__ 
> 例如，第一个区有8个磁盘，您可以将集群扩展为16个、32个或1024个磁盘的区域，您只需确保部署的SLA是原始区域的倍数即可。


## 3. 验证

验证是否部署成功，使用浏览器访问Minio服务或者使用 [`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)。多个节点的存储容量和就是分布式Minio的存储容量。

## 了解更多

- [Minio纠删码快速入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)
- [使用 `mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用 `s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用 `minio-go` SDK ](https://docs.min.io/cn/golang-client-quickstart-guide)

- [minio官方文档](https://docs.min.io)
