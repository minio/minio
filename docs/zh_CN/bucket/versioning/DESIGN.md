# 存储桶版本控制设计指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## `xl.meta` 文件描述

`xl.meta`是MinIO用于支持AWS S3兼容版本控制的一种新的自描述后端格式文件. 该文件记录了每个静态`version`的真实描述信息。`xl.meta`是一个定义好的数据结构序列化的`msgpack`文件。可以通过以下几部分内容更好的了解`xl.meta`文件。

`xl.meta`文件的前8个字节是XL文件头，该头信息描述了当前文件的格式和相应的格式版本，从而使解析器能够自动使用正确的数据结构来解析流中的后续内容。

当前文件头信息
```go
var (
        // XL header specifies the format
        // one extra byte left for future use
        xlHeader = [4]byte{'X', 'L', '2', ' '}

        // XLv2 version 1 specifies the current
        // version of the XLv2 format, 3 extra bytes
        // left for future use.
        xlVersionV1 = [4]byte{'1', ' ', ' ', ' '}
)
```

了解文件头后，我们接着看`xl.meta`文件的实际数据结构。`xl.meta`带有三种类型的对象条目，用于指定存储的版本对象的类型。

- ObjectType (默认)
- LegacyObjectType (遗留的现有部署和较旧的xl.json格式)
- DeleteMarker (一个versionId，主要是为了实现AWS规范兼容的DELETE序列)

以下是个msgpack格式的`xl.meta`转为JSON后的样例,你可以通过 [xl-meta-to-json.go](https://github.com/minio/minio/blob/master/docs/zh_CN/bucket/versioning/xl-meta-to-json.go) 这个小程序把`xl.meta`转成JSON，查看里面的内容。
```json
{
  "Versions": [
    {
      "Type": 1,
      "V2Obj": {
        "ID": "KWUs8S+8RZq4Vp5TWy6KFg==",
        "DDir": "X3pDAFu8Rjyft7QD6t7W5g==",
        "EcAlgo": 1,
        "EcM": 2,
        "EcN": 2,
        "EcBSize": 10485760,
        "EcIndex": 3,
        "EcDist": [
          3,
          4,
          1,
          2
        ],
        "CSumAlgo": 1,
        "PartNums": [
          1
        ],
        "PartETags": [
          ""
        ],
        "PartSizes": [
          314
        ],
        "PartASizes": [
          282
        ],
        "Size": 314,
        "MTime": 1591820730,
        "MetaSys": {
          "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "bXktbWluaW8ta2V5",
          "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "ZXlKaFpXRmtJam9pUVVWVExUSTFOaTFIUTAwdFNFMUJReTFUU0VFdE1qVTJJaXdpYVhZaU9pSkJMMVZzZFVnelZYVjZSR2N6UkhGWUwycEViRmRCUFQwaUxDSnViMjVqWlNJNklpdE9lbkJXVWtseFlWSlNVa2t2UVhNaUxDSmllWFJsY3lJNklrNDBabVZsZG5WU1NWVnRLMFoyUWpBMVlYTk9aMU41YVhoU1RrNUpkMDlhTkdKa2RuaGpLMjFuVDNnMFFYbFJhbE15V0hkU1pEZzNRMk54ZUN0SFFuSWlmUT09",
          "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "REFSRXYyLUhNQUMtU0hBMjU2",
          "X-Minio-Internal-Server-Side-Encryption-Iv": "bW5YRDhRUGczMVhkc2pJT1V1UVlnbWJBcndIQVhpTUN1dnVBS0QwNUVpaz0=",
          "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "SUFBZkFPeUo5ZHVVSEkxYXFLU0NSRkJTTnM0QkVJNk9JWU1QcFVTSXFhK2dHVThXeE9oSHJCZWwwdnRvTldUNE8zS1BtcWluR0cydmlNNFRWa0N0Mmc9PQ=="
        },
        "MetaUsr": {
          "content-type": "application/octet-stream",
          "etag": "20000f00f58c508b40720270929bd90e9f07b9bd78fb605e5432a67635fc34722e4fc53b1d5fab9ff8400eb9ded4fba2"
        }
      }
    }
  ]
}
```
