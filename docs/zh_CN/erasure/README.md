# MinIO纠删码快速入门 [![Slack][![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO使用纠删码`erasure code`和校验和`checksum`来保护数据免受硬件故障和无声数据损坏。 在最高冗余的情况下，即便您丢失一半数量（N/2）的硬盘，您仍然可以恢复数据。

## 什么是纠删码`erasure code`?

纠删码是一种恢复丢失和损坏数据的数学算法， MinIO采用Reed-Solomon code将对象拆分成可变数据和奇偶校验块。 例如，在有12个硬盘的建立过程中，一个对象在所有的硬盘中会被分成一个可变数量的数据和奇偶校验块，从6个数据和6个奇偶校验块到10个数据和2个奇偶校验块不等。  
在默认情况下，MinIO将对象拆分成N/2数据和N/2个奇偶校验块。但是，你可以使用[存储类](https://github.com/minio/minio/tree/master/docs/erasure/storage-class)来进行个性化的配置。我们推荐使用N/2数据和奇偶校验块的划分方式，因为它可以确保硬盘故障发生时的最佳保护。

## 为什么纠删码有用?

纠删码在多个硬盘故障发生时保护数据的工作原理和RAID或者复制不同。例如，像RAID6可以在损失两块盘的情况下不丢数据，而MinIO纠删码可以在丢失一半的盘的情况下，仍可以保证数据安全。 而且MinIO纠删码是作用在对象级别的，可以一次恢复一个对象，而RAID是作用在卷级别的，数据恢复时间很长。 MinIO对每个对象单独编码，存储服务一经部署，通常情况下是不需要更换硬盘或者修复。MinIO纠删码的设计目标是操作效率和尽可能的使用硬件加速。

![Erasure](https://github.com/minio/minio/blob/master/docs/screenshots/erasure-code.jpg?raw=true)

## 什么是位衰减`bit rot`保护?

位衰减也被称为数据腐化`Data Rot`、无声数据损坏`Silent Data Corruption`,是目前硬盘硬盘所面临的一种数据丢失问题。硬盘上的数据可能会神不知鬼不觉就损坏了，也没有什么错误日志。正所谓明枪易躲，暗箭难防，这种背地里犯的错比硬盘直接宕机了还危险。 不过不用怕，MinIO纠删码采用了高速[BLAKE2](https://blog.min.io/accelerating-blake2b-by-4x-using-simd-in-go-assembly-33ef16c8a56b#.jrp1fdwer) 基于哈希的校验和来防范位衰减。

## MinIO纠删码快速入门

### 1. 前提条件:

安装MinIO- [MinIO快速入门](https://docs.min.io/cn/minio-quickstart-guide)

### 2. 以纠删码模式运行MinIO

示例: 使用MinIO二进制文件，在12个盘中启动MinIO服务。

```sh
minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8 /data9 /data10 /data11 /data12
```

示例: 使用MinIO Docker镜像，在8块盘中启动MinIO服务。

```sh
docker run -p 9000:9000 --name minio \
  -v /mnt/data1:/data1 \
  -v /mnt/data2:/data2 \
  -v /mnt/data3:/data3 \
  -v /mnt/data4:/data4 \
  -v /mnt/data5:/data5 \
  -v /mnt/data6:/data6 \
  -v /mnt/data7:/data7 \
  -v /mnt/data8:/data8 \
  minio/minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8
```

### 3. 验证是否设置成功

你可以随意拔掉硬盘，看MinIO是否可以正常读写。
