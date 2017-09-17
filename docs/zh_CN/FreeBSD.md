# Minio FreeBSD 快速入门 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

### Minio with ZFS backend - FreeBSD
此示例假设你已经有正在运行的FreeBSD 11.x。

#### 启动 ZFS service
```sh
sysrc zfs_enable="YES"
```

启动 ZFS service
```sh
service zfs start
```

在 `/zfs` 文件上配置一个回环设备`loopback device `。
```sh
dd if=/dev/zero of=/zfs bs=1M count=4000
mdconfig -a -t vnode -f /zfs
```

创建zfs池
```sh
zpool create minio-example /dev/md0
```

```sh
df /minio-example
Filesystem    512-blocks Used   Avail Capacity  Mounted on
minio-example    7872440   38 7872402     0%    /minio-example
```

验证其是否可写
```sh
touch /minio-example/testfile
ls -l /minio-example/testfile
-rw-r--r--  1 root  wheel  0 Apr 26 00:51 /minio-example/testfile
```

现在您已经成功创建了一个ZFS池，了解更多，请参考 [ZFS 快速入门](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

不过，这个池并没有利用ZFS的任何特性。我们可以在这个池上创建一个带有压缩功能的ZFS文件系统，ZFS支持多种压缩算法: [`lzjb`, `gzip`, `zle`, `lz4`]。`lz4` 在数据压缩和系统开销方面通常是最最优的算法。
```sh
zfs create minio-example/compressed-objects
zfs set compression=lz4 minio-example/compressed-objects
```

监控池是否健康
```sh
zpool status -x
all pools are healthy
```

#### 启动Minio服务
从FreeBSD port安装 [Minio](https://minio.io)。
```sh
pkg install minio
```

配置Minio,让其使用挂载在`/minio-example/compressed-objects`的ZFS卷。
```
sysrc minio_enable=yes
sysrc minio_disks=/minio-example/compressed-objects
```

启动Mino。
```
service minio start
```

现在你已经成功的让Minio运行在ZFS上，你上传的对象都获得了磁盘级别的压缩功能，访问 http://localhost:9000。

#### 关闭Minio服务
```sh
service minio stop
```