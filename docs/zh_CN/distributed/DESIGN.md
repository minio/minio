# 分布式服务器设计指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
该文档介绍了分布式MinIO服务器的设计、架构和高级用法。

## 命令行
```
NAME:
  minio server - start object storage server

USAGE:
  minio server [FLAGS] DIR1 [DIR2..]
  minio server [FLAGS] DIR{1...64}
  minio server [FLAGS] DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
```

## 常用用法

具有4个纠删码集合，每个集合带有16块硬盘的独立纠删码模式部署配置。
```
minio server dir{1...64}
```

具有64个纠删码集合，每个集合16块硬盘的分布式纠删码模式部署配置。

```
minio server http://host{1...16}/export{1...64}
```

## 架构

MinIO可以基于省略号这种扩展方式自动的选择纠删集合的大小。下面是我们的纠删码行为的一些底层实现细节。

- MinIO采用的是 [Reed-Solomon](https://github.com/klauspost/reedsolomon) 纠删码方案, 该方案最大分片数为256个，也就是128个数据片和128个奇偶校验片。 基于一些实用的架构考虑，MinIO的设计超越了这个限制。

- 在MinIO部署中，纠删集合是一个独立的纠删码单元。 一个对象会被分片在一个纠删集合中。纠删集合的大小是根据磁盘的数量自动计算的。 MinIO支持无限数量的磁盘，但每个纠删集合最多有16个磁盘，最少有4个磁盘。

- 我们将纠删集合的磁盘数量限制为16个，是因为超过16个纠删码分片会变得混乱，并且没有任何性能优势。此外，由于16个磁盘的纠删集合默认情况下为每个对象提供8个磁盘的容量，因此在任何实际情况下都足够使用。

-  纠删集合的大小是根据可用的磁盘数量自动选择的，比如说如果有32台服务器，每台服务器有32块磁盘，总共是1024块磁盘。在这种情况下，纠删集合大小是16。这是由可接受的纠删集合大小的最大公因子（GCD）决定的，公因子取值范围从*4到16*。

- *如果磁盘总数有许多共同的除数，那么算法就会为这任意N个纠删集合大小选择最小的纠删集合数量*。例如1024个磁盘 - 4, 8, 16 是公因子. 16个磁盘一组会得到64个纠删集合,8个磁盘一组会得到128个纠删集合, 4个磁盘一组会得到256个纠删集合. 所以算法会自动选择最小的64个集合，也就是 *16 * 64=1024* 个磁盘的总和。

- *如果纠删集合中的磁盘总数是奇数，那么GCD算法提供了对奇数纠删集合的亲和力，以便节点可以均匀分布*。 这是为了确保参与任何纠删集合的磁盘数量相同。例如，如果你有2个主机节点，共有180个磁盘，那么GCD是15(奇数)，但这将导致磁盘分配不均匀，其中一个节点将会有更多的磁盘。为了避免这种情况，对节点给予亲和力，使用次优的GCD系数为12，提供均匀的分布。

- 在这个算法中，我们还确保了将磁盘均匀的分布。MinIO服务器会对传递的省略号参数进行扩展。下面是一个示例来演示这个过程。

```
minio server http://host{1...2}/export{1...8}
```

预期扩展
```
> http://host1/export1
> http://host2/export1
> http://host1/export2
> http://host2/export2
> http://host1/export3
> http://host2/export3
> http://host1/export4
> http://host2/export4
> http://host1/export5
> http://host2/export5
> http://host1/export6
> http://host2/export6
> http://host1/export7
> http://host2/export7
> http://host1/export8
> http://host2/export8
```

*这种扩展的一个显着特征是它选择了唯一的主机，因此该设置可提供最大的保护和可用性。*

- 为对象选择纠删集合是在`PutObject()`的过程中决定的，通过以下伪代码使用对象名称查找正确的纠删集合。
```go
// hashes the key returning an integer.
func sipHashMod(key string, cardinality int, id [16]byte) int {
        if cardinality <= 0 {
                return -1
        }
        sip := siphash.New(id[:])
        sip.Write([]byte(key))
        return int(sip.Sum64() % uint64(cardinality))
}
```
在`PutObject()`输入的key是对象的名字, 返回一个唯一索引。该索引是对象将存储的纠删集合中的一个。 该函数根据给定的对象名字返回一致Hash，也就是说对于相同的对象名称，返回的索引总是相同的。

- 仅在对象的纠删集合中需要满足读写仲裁。还可以在包含对象的纠删集合中对每个对象进行修复。

- 与其他对象存储不同，MinIO的纠删编码是在对象级别而不是卷级别。这允许应用程序通过为每个上传的对象设置`x-amz-storage-class=STANDARD/REDUCED_REDUNDANCY`来选择不同的存储类型，从而有效地利用集群的容量。

- MinIO还支持通过扩展区域的方式扩展现有集群。每个区域都是一个独立的实体，每个对象具有与原有群集相同的SLA（读/写仲裁）。通过使用现有的命名控件进行查询验证，MinIO确保不会创建冲突的对象。如果不存在这样的对象，那么MinIO只会选择使用量最少的区域存储该对象。

__可以组合很多个区域成一个集群，区域数量没有限制__

```
minio server http://host{1...32}/export{1...32} http://host{5...6}/export{1...8}
```

以上示例有两个区域

- 32 * 32 = 1024 drives zone1
- 2 * 8 = 16 drives zone2

> 注意这里对通用SLA的要求，原来的集群有1024个磁盘，每个纠删集合有16个磁盘，第二个区域至少要有16个磁盘才能符合原来集群的SLA，或者应该是16的倍数。


MinIO根据每个区域的可用空间比例将新对象放置在区域中。以下伪代码演示了此行为。
```go
func getAvailableZoneIdx(ctx context.Context) int {
        zones := z.getZonesAvailableSpace(ctx)
        total := zones.TotalAvailable()
        // choose when we reach this many
        choose := rand.Uint64() % total
        atTotal := uint64(0)
        for _, zone := range zones {
                atTotal += zone.Available
                if atTotal > choose && zone.Available > 0 {
                        return zone.Index
                }
        }
        // Should not happen, but print values just in case.
        panic(fmt.Errorf("reached end of zones (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
}
```

## 其他用法

### 具有多个省略号的高级用法

一个独立的纠删模式配置，通过controller组合生成，它有4个纠删集合，每个集合有16个磁盘。
```
minio server /mnt/controller{1...4}/data{1...16}
```

一个独立的纠删模式配置，通过mounts和controller组合生成，它有16个纠删集合，每个集合有16个磁盘。
```
minio server /mnt{1...4}/controller{1...4}/data{1...16}
```

一个分布式的纠删模式配置，通过host组合生成，它有2个纠删集合，每个集合有16个磁盘。
```
minio server http://host{1...32}/disk1
```

一个机柜级别冗余的分布式纠删模式配置，它有32个纠删集合，每个集合有16个磁盘。
```
minio server http://rack{1...4}-host{1...8}.example.net/export{1...16}
```
