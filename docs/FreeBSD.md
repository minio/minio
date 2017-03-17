# Minio FreeBSD Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

### Minio with ZFS backend - FreeBSD

This example assumes that you have a FreeBSD 10.x running

#### Step 1.

As root on the FreeBSD edit `/etc/rc.conf`

```sh
zfs_enable="YES"
```

Start ZFS service

```sh
service zfs start
```

```sh
dd if=/dev/zero of=/zfs bs=1M count=4000
```

Configure a loopback device on the `/zfs` file.

```sh
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

Now you have successfully created a ZFS pool for further reading please refer to [ZFS Quickstart Guide](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

However, this pool is not taking advantage of any ZFS features, so let's create a ZFS filesytem on this pool with compression enabled.  ZFS supports many compression algorithms: lzjb, gzip, zle, lz4.  LZ4 is often the most performant algorithm in terms of compression of data versus system overhead.


```sh
zfs create minio-example/compressed-objects
zfs set compression=lz4 minio-example/compressed-objects
```

To keep monitoring your pool use

```sh
zpool status
 pool: minio-example
state: ONLINE
 scan: none requested
config:

	NAME        STATE     READ WRITE CKSUM
	minio-example  ONLINE       0     0     0
	  md0       ONLINE       0     0     0

errors: No known data errors
```

#### Step 2.

Now start minio server on the ``/minio-example/compressed-objects``, change the permissions such that this directory is accessibly by a normal user

```sh
chown -R minio-user:minio-user /minio-example/compressed-objects
```

Now login as ``minio-user`` and start minio server.

```sh
curl https://dl.minio.io/server/minio/release/freebsd-amd64/minio > minio
chmod 755 minio
./minio server /minio-example/compressed-objects
```

Point your browser to http://localhost:9000 and login with the credentials displayed on the command line.

Now you have a S3 compatible server running on top of your ZFS backend which transparently provides disk level compression for your uploaded objects.

Thanks for using Minio, awaiting feedback :-)


#### Building Minio Server From Source

It is possible to build the minio server from source on FreeBSD.  To do this we will used the golang distribution provided by the FreeBSD pkg infrastructure.

We will need to install golang and GNU make:

```sh
sudo pkg install go gmake
```
Now we can proceed with the normal build process of minio server as found [here](https://github.com/minio/minio/blob/master/CONTRIBUTING.md).  The only caveat is we need to specify gmake (GNU make) when building minio server as the current Makefile is not BSD make compatible:


From here you can start the server as you would with a precompiled minio server build.
