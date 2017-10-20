# Minio 快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio) [![Snap Status](https://build.snapcraft.io/badge/minio/minio.svg)](https://build.snapcraft.io/user/minio/minio)

Minio是一个对象存储服务，基于Apache License v2.0协议. 它完全兼容亚马逊的S3云储存服务，非常适合于存储很多非结构化的数据，例如图片、视频、日志文件、备份数据和容器/虚拟机镜像等，而一个对象文件可以是任意大小，从几kb到最大5T不等。 

##  1. 下载

Minio是一个非常轻量的服务,可以很简单的和其他应用的结合，类似 NodeJS, Redis 或者 MySQL.

| Platform| Architecture | URL|
| ----------| -------- | ------|
|GNU/Linux|64-bit Intel|https://dl.minio.io/server/minio/release/linux-amd64/minio|
|Apple OS X|64-bit Intel|https://dl.minio.io/server/minio/release/darwin-amd64/minio|
|Microsoft Windows|64-bit|https://dl.minio.io/server/minio/release/windows-amd64/minio.exe|
|FreeBSD|64-bit|https://dl.minio.io/server/minio/release/freebsd-amd64/minio|

### Homebrew 安装

使用[Homebrew](http://brew.sh/) 来安装minio
 
```sh
$ brew install minio
$ minio --help
```

### 源码安装

源码安装只针对开发者和一些高级用户，如果你还没有golang的环境，请安装golang官网安装[How to install Golang](https://docs.minio.io/docs/zh-CN/how-to-install-golang).

```sh

$ go get -u github.com/minio/minio


```

## 2. 运行Minio服务


### GNU/Linux

 ```sh

$ chmod +x minio
$ ./minio --help
$ ./minio server /data

端点:  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000
AccessKey: USWUXHGYZQYFYFFIT3RE
SecretKey: MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03
区域:    us-east-1

浏览器访问入口:
   http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000

命令行访问: https://docs.minio.io/docs/zh-CN/minio-client-quickstart-guide
   $ mc config host add myminio http://10.0.0.10:9000 USWUXHGYZQYFYFFIT3RE MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03

对象操作API (兼容Amazon S3):
   Go:         https://docs.minio.io/docs/zh-CN/golang-client-quickstart-guide
   Java:       https://docs.minio.io/docs/zh-CN/java-client-quickstart-guide
   Python:     https://docs.minio.io/docs/zh-CN/python-client-quickstart-guide
   JavaScript: https://docs.minio.io/docs/zh-CN/javascript-client-quickstart-guide

```

### OS X


 ```sh

$ chmod 755 minio
$ ./minio --help
$ ./minio server /data

端点:  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000
AccessKey: USWUXHGYZQYFYFFIT3RE
SecretKey: MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03
区域:    us-east-1

浏览器访问入口:
   http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000

命令行访问: https://docs.minio.io/docs/zh-CN/minio-client-quickstart-guide
   $ mc config host add myminio http://10.0.0.10:9000 USWUXHGYZQYFYFFIT3RE MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03

对象操作API (兼容Amazon S3):
   Go:         https://docs.minio.io/docs/zh-CN/golang-client-quickstart-guide
   Java:       https://docs.minio.io/docs/zh-CN/java-client-quickstart-guide
   Python:     https://docs.minio.io/docs/zh-CN/python-client-quickstart-guide
   JavaScript: https://docs.minio.io/docs/zh-CN/javascript-client-quickstart-guide

```

### Microsoft Windows

```sh

C:\Users\Username\Downloads> minio.exe --help
C:\Users\Username\Downloads> minio.exe server D:\Photos

端点:  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000
AccessKey: USWUXHGYZQYFYFFIT3RE
SecretKey: MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03
区域:    us-east-1

浏览器访问入口:
   http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000

命令行访问: https://docs.minio.io/docs/zh-CN/minio-client-quickstart-guide
   $ mc.exe config host add myminio http://10.0.0.10:9000 USWUXHGYZQYFYFFIT3RE MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03

对象操作API (兼容Amazon S3):
   Go:         https://docs.minio.io/docs/zh-CN/golang-client-quickstart-guide
   Java:       https://docs.minio.io/docs/zh-CN/java-client-quickstart-guide
   Python:     https://docs.minio.io/docs/zh-CN/python-client-quickstart-guide
   JavaScript: https://docs.minio.io/docs/zh-CN/javascript-client-quickstart-guide


```

### Docker

```sh

$ docker pull minio/minio
$ docker run -p 9000:9000 minio/minio

```
访问minio的docker入门指南获得更多内容 [here](https://docs.minio.io/docs/zh-CN/minio-docker-quickstart-guide)

### FreeBSD

```sh

$ chmod 755 minio
$ ./minio --help
$ ./minio server /data

端点:  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000
AccessKey: USWUXHGYZQYFYFFIT3RE
SecretKey: MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03
区域:    us-east-1

浏览器访问入口:
   http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000

命令行访问: https://docs.minio.io/docs/zh-CN/minio-client-quickstart-guide
   $ mc config host add myminio http://10.0.0.10:9000 USWUXHGYZQYFYFFIT3RE MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03

对象操作API (兼容Amazon S3):
   Go:         https://docs.minio.io/docs/zh-CN/golang-client-quickstart-guide
   Java:       https://docs.minio.io/docs/zh-CN/java-client-quickstart-guide
   Python:     https://docs.minio.io/docs/zh-CN/python-client-quickstart-guide
   JavaScript: https://docs.minio.io/docs/zh-CN/javascript-client-quickstart-guide


```
请访问FreeBSD的官网指南获取更多详细信息[here](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

## 3. 使用浏览器测试minio服务

打开浏览器并输入 http://127.0.0.1:9000 查看在minio服务器上面的所有bucket

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.jpg?raw=true)


## 4. 使用`mc`测试minio服务


按照 [这个](https://docs.minio.io/docs/minio-client-quickstart-guide) 安装mc. 使用 `mc ls` 命令显示所有在minio服务上面的bucket.

```sh

$ mc ls myminio/
[2015-08-05 08:13:22 IST]     0B andoria/
[2015-08-05 06:14:26 IST]     0B deflector/
[2015-08-05 08:13:11 IST]     0B ferenginar/
[2016-03-08 14:56:35 IST]     0B jarjarbing/
[2016-01-20 16:07:41 IST]     0B my.minio.io/

```

查看更多的例子请访问 [Minio Client Complete Guide](https://docs.minio.io/docs/zh-CN/minio-client-complete-guide).


## 5. 更多内容

- [Minio Erasure Code 快速入门](https://docs.minio.io/docs/zh-CN/minio-erasure-code-quickstart-guide)
- [Minio Docker 快速入门](https://docs.minio.io/docs/zh-CN/minio-docker-quickstart-guide)
- [使用`mc`测试 Minio Server](https://docs.minio.io/docs/zh-CN/minio-client-quickstart-guide)
- [使用 `aws-cli` 测试 Minio Server](https://docs.minio.io/docs/zh-CN/aws-cli-with-minio)
- [使用 `s3cmd` 测试 Minio Server](https://docs.minio.io/docs/zh-CN/s3cmd-with-minio)
- [使用 `minio-go` SDK ce's测试 Minio Server](https://docs.minio.io/docs/zh-CN/golang-client-quickstart-guide)


## 6. 给Minio项目贡献
请按照Minio [贡献者指导手册](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)
