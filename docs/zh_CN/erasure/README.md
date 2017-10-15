# Minio纠删码快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio使用纠删码`erasure code`和校验和`checksum`来保护数据免受硬件故障和无声数据损坏。 即便您丢失一半数量（N/2）的硬盘，您仍然可以恢复数据。

## 什么是纠删码`erasure code`?

纠删码是一种恢复丢失和损坏数据的数学算法， Minio采用Reed-Solomon code将对象拆分成N/2数据和N/2 奇偶校验块。 这就意味着如果是12块盘，一个对象会被分成6个数据块、6个奇偶校验块，你可以丢失任意6块盘（不管其是存放的数据块还是奇偶校验块），你仍可以从剩下的盘中的数据进行恢复，是不是很NB，感兴趣的同学请翻墙google。

## 为什么纠删码有用?

纠删码的工作原理和RAID或者复制不同，像RAID6可以在损失两块盘的情况下不丢数据，而Minio纠删码可以在丢失一半的盘的情况下，仍可以保证数据安全。 而且Minio纠删码是作用在对象级别，可以一次恢复一个对象，而RAID是作用在卷级别，数据恢复时间很长。 Minio对每个对象单独编码，存储服务一经部署，通常情况下是不需要更换硬盘或者修复。Minio纠删码的设计目标是为了性能和尽可能的使用硬件加速。

![Erasure](https://github.com/minio/minio/blob/master/docs/screenshots/erasure-code.jpg?raw=true)

## 什么是位衰减`bit rot`保护?

位衰减又被称为数据腐化`Data Rot`、无声数据损坏`Silent Data Corruption`,是目前硬盘数据的一种严重数据丢失问题。硬盘上的数据可能会神不知鬼不觉就损坏了，也没有什么错误日志。正所谓明枪易躲，暗箭难防，这种背地里犯的错比硬盘直接咔咔宕了还危险。 不过不用怕，Minio纠删码采用了高速[BLAKE2](https://blog.minio.io/accelerating-blake2b-by-4x-using-simd-in-go-assembly-33ef16c8a56b#.jrp1fdwer) 基于哈希的校验和来防范位衰减。

## Minio纠删码快速入门

### 1. 前提条件:

安装Minio- [Minio快速入门](https://docs.minio.io/docs/zh_CN/minio-quickstart-guide)

### 2. 以纠删码模式运行Minio

示例: 使用Minio，在12个盘中启动Minio服务。

```sh
minio server /data1 /data2 /data3 /data4 /data5 /data6 /data7 /data8 /data9 /data10 /data11 /data12
```

示例: 使用Minio Docker镜像，在8块盘中启动Minio服务。

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

你可以随意拔掉硬盘，看Minio是否可以正常读写。
