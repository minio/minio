# MinIO Server 调试教程 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

### HTTP Trace
可以通过 [`mc admin trace`](https://github.com/minio/mc/blob/master/docs/minio-admin-complete-guide.md#command-trace---display-minio-server-http-trace) 命令启用HTTP跟踪。

示例:
```sh
minio server /data
```

默认只跟踪API调用操作和HTTP响应状态。
```sh
mc admin trace myminio
```

跟踪整个HTTP请求 
```sh
mc admin trace --verbose myminio
```

跟踪整个HTTP请求和节点间通信
```sh
mc admin trace --all --verbose myminio
```


### 诊断工具
诊断工具有助于确保运行MinIO的底层基础设施配置正确，并且运行正常。 此测试是一次长时间运行的测试，建议在首次配置集群时立即运行，并且每次遇到故障时都要运行该测试。 请注意，测试会占用系统上的大部分可用资源. 在使用这个来调试故障场景时必须小心，以防止更大的中断。 可以使用[`mc admin obd`](https://github.com/minio/mc/blob/master/docs/minio-admin-complete-guide.md#command-obd---display-minio-server-obd) 命令触发OBD测试.

示例:
```sh
minio server /data
```

该命令不带标志
```sh
mc admin obd myminio
```

使用如下格式打印结果输出
```sh
● Admin Info ... ✔ 
● CPU ... ✔ 
● Disk Hardware ... ✔ 
● Os Info ... ✔ 
● Mem Info ... ✔ 
● Process Info ... ✔ 
● Config ... ✔ 
● Drive ... ✔ 
● Net ... ✔ 
*********************************************************************************
                                   WARNING!!
     ** THIS FILE MAY CONTAIN SENSITIVE INFORMATION ABOUT YOUR ENVIRONMENT ** 
     ** PLEASE INSPECT CONTENTS BEFORE SHARING IT ON ANY PUBLIC FORUM **
*********************************************************************************
OBD data saved to dc-11-obd_20200321053323.json.gz
```

gzip输出包含系统的调试信息
