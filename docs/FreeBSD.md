# Minio FreeBSD Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

### Minio with ZFS backend - FreeBSD
This example assumes that you have a FreeBSD 11.x running

#### Start ZFS service
```sh
sysrc zfs_enable="YES"
```

Start ZFS service
```sh
service zfs start
```

Configure a loopback device on the `/zfs` file.
```sh
dd if=/dev/zero of=/zfs bs=1M count=4000
mdconfig -a -t vnode -f /zfs
```

Create zfs pool
```sh
zpool create minio-example /dev/md0
```

```sh
df /minio-example
Filesystem    512-blocks Used   Avail Capacity  Mounted on
minio-example    7872440   38 7872402     0%    /minio-example
```

Verify if it is writable
```sh
touch /minio-example/testfile
ls -l /minio-example/testfile
-rw-r--r--  1 root  wheel  0 Apr 26 00:51 /minio-example/testfile
```

Now you have successfully created a ZFS pool for further reading please refer [ZFS Quickstart Guide](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

However, this pool is not taking advantage of any ZFS features. So let's create a ZFS filesytem on this pool with compression enabled. ZFS supports many compression algorithms: [`lzjb`, `gzip`, `zle`, `lz4`]. `lz4` is often the most performant algorithm in terms of compression of data versus system overhead.
```sh
zfs create minio-example/compressed-objects
zfs set compression=lz4 minio-example/compressed-objects
```

To monitor if your pools are healthy.
```sh
zpool status -x
all pools are healthy
```

#### Start Minio service
Install [Minio](https://minio.io) from FreeBSD port.
```sh
pkg install minio
```

Enable minio and configure minio to use ZFS volume mounted at `/minio-example/compressed-objects`.
```
sysrc minio_enable=yes
sysrc minio_disks=/minio-example/compressed-objects
```

Start minio.
```
service minio start
```

Now you have an Minio running on top of your ZFS backend which transparently provides disk level compression for your uploaded objects, please visit http://localhost:9000 to open Minio Browser.

#### Stop Minio service
In-case you wish to stop Minio service.
```sh
service minio stop
```