### Minio with ZFS backend - FreeBSD

This example assumes that you have a FreeBSD 10.x running

#### Step *1* 

As root on the FreeBSD edit `/etc/rc.conf`
```
zfs_enable="YES"
```

Start ZFS service
```
# service zfs start
```

```
# dd if=/dev/zero of=/zfs bs=1M count=4000
```

Configure a loopback device on the `/zfs` file. 
```
# mdconfig -a -t vnode -f /zfs
```

Create zfs pool
```
# zpool create minio-example /dev/md0
```

```
# df /minio-example
Filesystem    512-blocks Used   Avail Capacity  Mounted on
minio-example    7872440   38 7872402     0%    /minio-example
```

Verify if it is writable
```
# touch /minio-example/testfile
# ls -l /minio-example/testfile
-rw-r--r--  1 root  wheel  0 Apr 26 00:51 /minio-example/testfile
```

Now you have successfully created a ZFS pool for futher reading please refer to [ZFS Quickstart Guide](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

However, this pool is not taking advantage of any ZFS features. To create a dataset on this pool with compression enabled:

```
# zfs create minio-example/compressed-objects
# zfs set compression=gzip minio-example/compressed-objects
```

To keep monitoring your pool use

```
# zpool status
  pool: minio-example
 state: ONLINE
  scan: none requested
config:

	NAME        STATE     READ WRITE CKSUM
	minio-example  ONLINE       0     0     0
	  md0       ONLINE       0     0     0

errors: No known data errors
```

#### Step *2*

Now start minio server on the ``/minio-example/compressed-objects``, change the permissions such that this directory is accessibly by a normal user

```
# chown -R minio-user:minio-user /minio-example/compressed-objects
```

Now login as ``minio-user`` and start minio server. 

```
$ curl https://dl.minio.io/server/minio/release/freebsd-amd64/minio > minio
$ chmod 755 minio
$ ./minio server /minio-example/compressed-objects

AccessKey: X3RU3Q6B4T20TYB281W5  SecretKey: bvp0DF8c+6MzL60UxFqOZGlA5Z8UPfIAATUQuzLS  Region: us-east-1

Minio Object Storage:
    http://159.203.234.49:9000
    http://10.12.0.5:9000
    http://127.0.0.1:9000

Minio Browser:
    http://159.203.234.49:9000
    http://10.12.0.5:9000
    http://127.0.0.1:9000

To configure Minio Client:
    $ wget https://dl.minio.io/client/mc/release/freebsd-amd64/mc
    $ chmod 755 mc
    $ ./mc config host add myminio http://localhost:9000 X3RU3Q6B4T20TYB281W5 bvp0DF8c+6MzL60UxFqOZGlA5Z8UPfIAATUQuzLS
```

Point your browser to http://localhost:9000 and login with the credentials displayed on the command line. 

Now you have a S3 compatible server running on top of your ZFS backend which transparently provides disk level compression for your uploaded objects.

Thanks for using Minio, awaiting feedback :-) 

