# 在Chrooted环境中运行MinIO [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Chroot允许在标准的Linux上基于用户的namespace隔离。

## 1. 前置条件
* 熟悉 [chroot](http://man7.org/linux/man-pages/man2/chroot.2.html)
* 系统上已经安装Chroot

## 2. 在Chroot中安装MinIO
```sh
mkdir -p /mnt/export/${USER}/bin
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O /mnt/export/${USER}/bin/minio
chmod +x /mnt/export/${USER}/bin/minio
```

将你的`proc`挂载绑定到目标chroot目录
```
sudo mount --bind /proc /mnt/export/${USER}/proc
```

## 3.在Chroot中运行单节点MinIO
### GNU/Linux
```sh
sudo chroot --userspec username:group /mnt/export/${USER} /bin/minio --config-dir=/.minio server /data

Endpoint:  http://192.168.1.92:9000  http://65.19.167.92:9000
AccessKey: MVPSPBW4NP2CMV1W3TXD
SecretKey: X3RKxEeFOI8InuNWoPsbG+XEVoaJVCqbvxe+PTOa
...
...
```

现在可以在主机的9000端口访问实例，在浏览器中输入http://127.0.0.1:9000/即可访问

## 进一步探索
- [Minio纠删码快速入门](https://docs.min.io/cn/minio-erasure-code-quickstart-guide)
- [使用`mc`](https://docs.min.io/cn/minio-client-quickstart-guide)
- [使用`aws-cli`](https://docs.min.io/cn/aws-cli-with-minio)
- [使用`s3cmd`](https://docs.min.io/cn/s3cmd-with-minio)
- [使用`minio-go`SDK](https://docs.min.io/cn/golang-client-quickstart-guide)
