# 在 DC/OS上部署minio [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

要在DC/OS上部署MinIO，可以使用我们的 [official universe package](https://github.com/mesosphere/universe/tree/version-3.x/repo/packages/M/minio/6).

## 1. 前提条件

  - DC/OS 1.9或更新版本
  - [Marathon-LB](https://dcos.io/docs/1.9/usage/service-discovery/marathon-lb/usage/) 必须安装并启动。
  - 识别Marathon-LB或者一个指向Marathon-LB的public agent(s)的可用主机名的 [IP of the public agent](https://dcos.io/docs/1.9/administration/locate-public-agent/) 。


## 2. 设置MinIO

你可以使用DC/OS GUI或者CLI安装MinIO Universe package。

### 使用DC/OS GUI安装MinIO
- 访问DC/OS admin页面，然后点击左边栏的Universe,然后点击Packages页签，搜索MinIO,点击左边栏的```Install```按钮。 

- 点击按钮`Install Package`进行一键安装。你可以通过`host:9000`访问你的MinIO server,其中`host`是Marathon-LB所在服务器的IP或者主机名。 `minio` 和 `minio123` 分别是默认的access key和secret key。

- 更多关于自定义安装的内容，请看[这里](https://github.com/dcos/zh_CN/examples/blob/master/minio/1.9/README.md#minio-installation-using-gui).

### 使用DC/OS CLI安装MinIO

使用命令行安装, 输入

```bash
$ dcos package install minio
```

## 3. 卸载MinIO

你确定要这么做吗，如果你真要这么做，我们也不会像国内的软件那么无赖。如需卸载，请输入

```bash
$ dcos package uninstall minio
```

### 了解更多

- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)

- [DC/OS Project](https://docs.mesosphere.com/)

