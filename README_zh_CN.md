# Minio Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio 是一个基于Apache License v2.0开源协议的对象存储服务。它兼容亚马逊S3云存储服务接口，非常适合于存储大容量非结构化的数据，例如图片、视频、日志文件、备份数据和容器/虚拟机镜像等，而一个对象文件可以是任意大小，从几kb到最大5T不等。 

Minio是一个非常轻量的服务,可以很简单的和其他应用的结合，类似 NodeJS, Redis 或者 MySQL。

## Docker 容器
### 稳定版
```
docker pull minio/minio
docker run -p 9000:9000 minio/minio server /data
```

### 尝鲜版
```
docker pull minio/minio:edge
docker run -p 9000:9000 minio/minio:edge server /data
```
更多Docker部署信息请访问 [这里](https://docs.minio.io/docs/minio-docker-quickstart-guide)

## macOS
### Homebrew
使用 [Homebrew](http://brew.sh/)安装minio

```sh
brew install minio/stable/minio
minio server /data
```
#### Note
如果你之前使用 `brew install minio`安装过minio, 可以用 `minio/stable/minio` 官方镜像进行重装. 由于golang 1.8的bug,homebrew版本不太稳定。

```
brew uninstall minio
brew install minio/stable/minio
```

### 下载二进制文件
| 操作系统| CPU架构 | 地址|
| ----------| -------- | ------|
|Apple macOS|64-bit Intel|https://dl.minio.io/server/minio/release/darwin-amd64/minio |
```sh
chmod 755 minio
./minio server /data
```

## GNU/Linux
### 下载二进制文件
| 操作系统| CPU架构 | 地址|
| ----------| -------- | ------|
|GNU/Linux|64-bit Intel|https://dl.minio.io/server/minio/release/linux-amd64/minio |
```sh
chmod +x minio
./minio server /data
```

## 微软Windows系统
### 下载二进制文件
| 操作系统| CPU架构 | 地址|
| ----------| -------- | ------|
|微软Windows系统|64位|https://dl.minio.io/server/minio/release/windows-amd64/minio.exe |
```sh
minio.exe server D:\Photos
```

## FreeBSD
### Port
使用 [pkg](https://github.com/freebsd/pkg)进行安装。

```sh
pkg install minio
sysrc minio_enable=yes
sysrc minio_disks=/home/user/Photos
service minio start
```

## 使用源码安装

采用源码安装仅供开发人员和高级用户使用,如果你还没有Golang环境， 请参考 [How to install Golang](https://docs.minio.io/docs/how-to-install-golang).

```sh
go get -u github.com/minio/minio
```

## 使用Minio浏览器进行验证
安装后使用浏览器访问[http://127.0.0.1:9000](http://127.0.0.1:9000)，如果可以访问，则表示minio已经安装成功。

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.png?raw=true)

## 使用Minio客户端 `mc`进行验证
`mc` 提供了一些UNIX常用命令的替代品，像ls, cat, cp, mirror, diff这些。 它支持文件系统和亚马逊S3云存储服务。 更多信息请参考 [mc快速入门](https://docs.minio.io/docs/minio-client-quickstart-guide) 。

## 已经存在的数据
当在单块磁盘上部署Minio server,Minio server允许客户端访问数据目录下已经存在的数据。比如，如果Minio使用`minio server /mnt/data`启动，那么所有已经在`/mnt/data`目录下的数据都可以被客户端访问到。

上述描述对所有网关后端同样有效。

## 了解更多
- [Minio纠删码入门](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [`mc`快速入门](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.minio.io/docs/aws-cli-with-minio)
- [使用 `s3cmd`](https://docs.minio.io/docs/s3cmd-with-minio)
- [使用 `minio-go` SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [Minio文档](https://docs.minio.io)

## 如何参与到Minio项目
请参考 [贡献者指南](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)。欢迎各位中国程序员加到Minio项目中。
