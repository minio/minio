# MinIO 存储类型快速入门 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

在纠删码模式下,MinIO server支持存储类型. 这可以指定每个对象的数据和奇偶校验盘，其实就是可以为对象选择不同的存储类型.

## 概述

MinIO 支持两种存储类型, 低冗余存储和标准存储。 这些存储类型可以在MinIO服务器启动之前通过环境变量定义。 在通过环境变量定义了每个存储类型的数据和奇偶校验盘的数量后，
你可以通过请求中的元数据字段`x-amz-storage-class`来设置一个对象的存储类型。然后，MinIO服务器通过将对象保存在特定数量的数据和奇偶校验盘中来兑现存储类型。

## 可用存储空间

选择不同的数据和奇偶校验盘的数量会直接影响到存储空间的使用。通过存储类型，你能优化以实现高冗余或者是更好的空间利用率。

让我们以在16个盘的MinIO部署中存储100M文件为例，来了解数据和奇偶校验盘数量的不同组合是如何影响可用存储空间的。如果你使用8个数据盘和8个奇偶校验盘，文件空间使用量约为两倍，
即100M文件将占用200M空间。但是，如果你是用10个数据盘和6个奇偶校验盘，则同样的100M文件大约需要160M的空间。如果你是用14个数据盘和2个奇偶校验盘，100M文件仅仅需要约114M空间。

以下是一张16盘的MinIO部署，数据/奇偶校验盘数量和相应的 _近似_ 存储储空间使用情况列表。_空间使用率_ 约等于纠删编码下的使用空间除以文件的实际大小。

|    盘总个数 (N)   |   数据盘个数 (D)  |  奇偶校验码个数 (P) |      空间使用率      |
|------------------|-----------------|-------------------|---------------------|
|               16 |               8 |                 8 |                2.00 |
|               16 |               9 |                 7 |                1.79 |
|               16 |              10 |                 6 |                1.60 |
|               16 |              11 |                 5 |                1.45 |
|               16 |              12 |                 4 |                1.34 |
|               16 |              13 |                 3 |                1.23 |
|               16 |              14 |                 2 |                1.14 |

你可以使用公式: `盘总个数 (N)/数据盘个数 (D)`来计算 _大概的_ 空间使用率。

### 标准(STANDARD)存储类型的允许值

`STANDARD`存储类型意味着奇偶校验盘比`REDUCED_REDUNDANCY`多。 所以, `STANDARD`的奇偶校验盘数量应该

- 如果`REDUCED_REDUNDANCY`的奇偶校验盘未设置的话，应该大于等于2。
- 如果已设置的话，应该大于`REDUCED_REDUNDANCY`的奇偶校验盘数量。

奇偶校验块不能大于数据块，所以`STANDARD`存储类型的奇偶校验块不能大于N/2。（N是盘总个数）

`STANDARD`存储类型的默认值是`N/2`（N是盘总个数）。

### 低冗余(REDUCED_REDUNDANCY)存储类型的允许值

`REDUCED_REDUNDANCY`存储类型意味着奇偶校验盘比`REDUCED_REDUNDANCY`少。 所以, `REDUCED_REDUNDANCY`的奇偶校验盘数量应该

- 如果`STANDARD`的奇偶校验盘未设置的话，应该小于2。
- 如果设置的话，应该小于`STANDARD`的奇偶校验盘数量。

因为不建议奇偶校验盘数量低于2， 所以4个盘组成的纠删码模式部署是不支持`REDUCED_REDUNDANCY`存储类型的。

`REDUCED_REDUNDANCY`存储类型的默认值是`2`。

## 存储类型入门

### 设置存储类型

设置存储类型环境变量的格式如下

`MINIO_STORAGE_CLASS_STANDARD=EC:parity`
`MINIO_STORAGE_CLASS_RRS=EC:parity`

例如, 设置 `MINIO_STORAGE_CLASS_RRS` 奇偶校验盘为2 以及设置 `MINIO_STORAGE_CLASS_STANDARD` 奇偶校验盘为3

```sh
export MINIO_STORAGE_CLASS_STANDARD=EC:3
export MINIO_STORAGE_CLASS_RRS=EC:2
```

也可以通过`mc admin config` get/set 命令来设置存储类型。参考 [存储类型](https://github.com/minio/minio/tree/master/docs/zh_CN/config#存储类型) 获取更多详细信息。


*注意*

- 如果通过环境变量或`mc admin config` get/set命令设置了`STANDARD`存储类型，并且请求元数据中不存在`x-amz-storage-class`，则MinIO服务器会将`STANDARD`存储类型应用于该对象。这意味着将按照`STANDARD`存储类型中的设置使用数据和奇偶校验盘数量。

- 如果在启动MinIO服务器之前未定义存储类型，并且随后的PutObject元数据字段中存在`x-amz-storage-class`，其值为`REDUCED_REDUNDANCY`或`STANDARD`，则MinIO服务器将使用默认的奇偶校验值。

### 设置元数据

如下`minio-go`的示例中，存储类型被设置为`REDUCED_REDUNDANCY`。这意味着对象被拆分为6个数据块和2个奇偶校验块(按照上一步骤中的存储类型设置)。

```go
s3Client, err := minio.New("localhost:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
if err != nil {
	log.Fatalln(err)
}

object, err := os.Open("my-testfile")
if err != nil {
	log.Fatalln(err)
}
defer object.Close()
objectStat, err := object.Stat()
if err != nil {
	log.Fatalln(err)
}

n, err := s3Client.PutObject("my-bucketname", "my-objectname", object, objectStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream", StorageClass: "REDUCED_REDUNDANCY"})
if err != nil {
	log.Fatalln(err)
}
log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
```
