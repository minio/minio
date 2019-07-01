# MinIO快速启动指南
[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

MinIO 是一个基于Apache License v2.0开源协议的对象存储服务。它兼容亚马逊S3云存储服务接口，非常适合于存储大容量非结构化的数据，例如图片、视频、日志文件、备份数据和容器/虚拟机镜像等。而一个对象文件的大小从几kb到最大5TB不等。

MinIO是一个非常轻量的服务,可以很简单地和其他应用结合，与NodeJS, Redis 和 MySQL相类似。

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
注意: Docker不会展示自动生成的key除非你用-it（交互TTY）参数启动容器。一般而言，不建议将自动生成的key用于容器。
更多Docker部署信息请访问 [这里](https://docs.min.io/cn/minio-docker-quickstart-guide)

## macOS
### Homebrew
使用 [Homebrew](http://brew.sh/)安装minio

```sh
brew install minio/stable/minio
minio server /data
```
#### 注意
> 注意：如果你之前使用 `brew install minio`安装过minio, 推荐使用 `minio/stable/minio` 官方镜像进行重装。

```sh
brew uninstall minio
brew install minio/stable/minio
```

### 下载二进制文件
| 操作系统    | CPU架构      | 地址                                                        |
| ----------  | --------     | ------                                                      |
| Apple macOS | 64-bit Intel | https://dl.min.io/server/minio/release/darwin-amd64/minio |
```sh
chmod 755 minio
./minio server /data
```

## GNU/Linux
### 下载二进制文件
| 操作系统   | CPU架构      | 地址                                                       |
| ---------- | --------     | ------                                                     |
| GNU/Linux  | 64-bit Intel | https://dl.min.io/server/minio/release/linux-amd64/minio |
```sh
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server /data
```

## 微软Windows系统
### 下载二进制文件
| 操作系统        | CPU架构  | 地址                                                             |
| ----------      | -------- | ------                                                           |
| 微软Windows系统 | 64位     | https://dl.min.io/server/minio/release/windows-amd64/minio |
```sh
minio.exe server D:\Photos
```

## FreeBSD
### 端口
使用 [pkg](https://github.com/freebsd/pkg)安装minio。

```sh
pkg install minio
sysrc minio_enable=yes
sysrc minio_disks=/home/user/Photos
service minio start
```

## 使用源码安装
源码安装仅供开发人员和高级用户使用,如果你还没有可以工作的Golang环境， 请参考 [How to install Golang](https://docs.min.io/cn/how-to-install-golang).

```sh
GO111MODULE=on go get github.com/minio/minio
```

## 允许防火墙端口访问  
在默认情况下，MinIO使用9000端口监听进来的连接，如果你的平台默认阻断端口，你可能需要通过设置使其能访问端口。

### iptables
对于拥有激活iptable（像RHEL,CentOs等等）的主机而言，你能够通过`iptables`命令使全部的通讯都连接到特定的端口。使用下面的命令来准许访问9000端口

```sh
iptables -A INPUT -p top -dport 9000 -j ACCEPT
service iptalbes restart
```

下面的命令可以使全部接入的通讯连接到从9000到9010的端口。

```sh
iptables -A INPUT -p tcp -dport 9000:9010 -j ACCEPT
service iptables restart
```

### ufw

对于拥有激活ufw（基于distros的Debian）的主机而言，你可以使用`ufw`命令来准许通讯访问特定的端口，使用下面的命令来准许访问9000端口

```sh
ufw allow 9000
```

下面的命令可以使全部接入的通讯连接到从9000到9010的端口。

```sh 
ufw allow 9000:9010/tcp
```

### 防火墙命令(firewall-cmd)  
对于拥有激活firewall-cmd（CentOS）的主机而言，你能够使用`firewall-cmd`命令来准许通讯连接到特定的端口。使用下面的命令来准许访问9000端口

```sh
firewall-cmd -get-active-zones
```

这个命令可以激活zone（s）。现在，应用端口规则到返回以上的相关zones中。例如，如果zone是`public`,使用

```sh
firewall-cmd -zone=public -add-port=9000/tcp -permanent
```

注意参数`permanent`可以确保规则持续存在于防火墙的启动、重启或重载之中。最后重载防火墙来使更改生效。

```sh
firewall-cmd -reload
```

## 使用MinIO浏览器进行验证

MinIO服务具有一个嵌入的基于对象浏览器的网站。点击你的浏览器访问http://127.0.0.1:9000，确保你的服务器已经成功启动。

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.png?raw=true)

## 使用MinIO客户端 `mc`进行验证
`mc` 提供了UNIX常用命令的替代品，像ls, cat, cp, mirror, diff等。 它支持文件系统和亚马逊S3云存储服务。 更多信息请参考 [mc快速入门](https://docs.min.io/docs/minio-client-quickstart-guide) 。

## 已经存在的数据
当在单块磁盘上部署MinIO server,MinIO server允许客户端访问数据目录下已经存在的数据。比如，如果使用`minio server /mnt/data`启动MinIO，那么所有已经在`/mnt/data`目录下存在的数据都可以被客户端访问到。

上述描述对所有网关后端同样有效。

## 了解更多
- [MinIO纠删码入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)
- [`mc`快速入门](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用 `aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用 `s3cmd`](https://docs.min.io/docs/s3cmd-with-minio)
- [使用 `minio-go` SDK](https://docs.min.io/cn/golang-client-quickstart-guide)
- [MinIO文档](https://docs.min.io)

## 如何参与到MinIO项目
请参考MinIO[贡献者指南](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)。

## 认证  
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fminio%2Fminio.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fminio%2Fminio?ref=badge_large)

