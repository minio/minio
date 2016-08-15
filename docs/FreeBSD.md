# Minio FreeBSD Quickstart Guide [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### Minio with ZFS backend - FreeBSD

This example assumes that you have a FreeBSD 10.x running

#### Step 1.

As root on the FreeBSD edit `/etc/rc.conf`

```sh

zfs_enable="YES"

```

Start ZFS service

```sh

# service zfs start

```

```sh

# dd if=/dev/zero of=/zfs bs=1M count=4000

```

Configure a loopback device on the `/zfs` file. 

```sh

# mdconfig -a -t vnode -f /zfs

```

Create zfs pool

```sh

# zpool create minio-example /dev/md0

```

```sh

# df /minio-example
Filesystem    512-blocks Used   Avail Capacity  Mounted on
minio-example    7872440   38 7872402     0%    /minio-example

```

Verify if it is writable

```sh

# touch /minio-example/testfile
# ls -l /minio-example/testfile
-rw-r--r--  1 root  wheel  0 Apr 26 00:51 /minio-example/testfile

```

Now you have successfully created a ZFS pool for further reading please refer to [ZFS Quickstart Guide](https://www.freebsd.org/doc/handbook/zfs-quickstart.html)

However, this pool is not taking advantage of any ZFS features, so let's create a ZFS filesytem on this pool with compression enabled.  ZFS supports many compression algorithms: lzjb, gzip, zle, lz4.  LZ4 is often the most performant algorithm in terms of compression of data versus system overhead.


```sh

# zfs create minio-example/compressed-objects
# zfs set compression=lz4 minio-example/compressed-objects

```

To keep monitoring your pool use

```sh

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

#### Step 2.

Now start minio server on the ``/minio-example/compressed-objects``, change the permissions such that this directory is accessibly by a normal user

```sh

# chown -R minio-user:minio-user /minio-example/compressed-objects

```

Now login as ``minio-user`` and start minio server. 

```sh

$ curl https://dl.minio.io/server/minio/release/freebsd-amd64/minio > minio
$ chmod 755 minio
$ ./minio server /minio-example/compressed-objects


Endpoint:  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000
AccessKey: USWUXHGYZQYFYFFIT3RE
SecretKey: MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03
Region:    us-east-1

Browser Access:
  http://10.0.0.10:9000  http://127.0.0.1:9000  http://172.17.0.1:9000

Command-line Access: https://docs.minio.io/docs/minio-client-quickstart-guide
  $ mc config host add myminio http://localhost:9000  USWUXHGYZQYFYFFIT3RE MOJRH0mkL1IPauahWITSVvyDrQbEEIwljvmxdq03

Object API (Amazon S3 compatible):
  Go:         https://docs.minio.io/docs/golang-client-quickstart-guide
  Java:       https://docs.minio.io/docs/java-client-quickstart-guide
  Python:     https://docs.minio.io/docs/python-client-quickstart-guide
  JavaScript: https://docs.minio.io/docs/javascript-client-quickstart-guide

```

Point your browser to http://localhost:9000 and login with the credentials displayed on the command line. 

Now you have a S3 compatible server running on top of your ZFS backend which transparently provides disk level compression for your uploaded objects.

Thanks for using Minio, awaiting feedback :-) 


#### Building Minio Server From Source

It is possible to build the minio server from source on FreeBSD.  To do this we will used the golang distribution provided by the FreeBSD pkg infrastructure.

First we will need to install golang as well as GNU make:

```sh

$ sudo pkg install go gmake
Updating FreeBSD repository catalogue..
FreeBSD repository is up-to-date.
All repositories are up-to-date.
The following 1 package(s) will be affected (of 0 checked):

New packages to be INSTALLED:
        go: 1.6.2,1
        gmake: 4.1_2

        The process will require 261 MiB more space.
        40 MiB to be downloaded.

        Proceed with this action? [y/N]: y
        Fetching go-1.6.2,1.txz: 100%   40 MiB  10.4MB/s    00:04
        Fetching gmake-4.1_2.txz: 100%  363 KiB 372.2kB/s    00:01
        Checking integrity... done (0 conflicting)
        [1/2] Installing go-1.6.2,1...
        [1/2] Extracting go-1.6.2,1: 100%
        [2/2] Installing gmake-4.1_2...
        [2/2] Extracting gmake-4.1_2: 100%
```

Next we need to configure our environment for golang.  Insert the following lines into your ~/.profile file, and be sure to source the file before proceeding to the next step:


```sh

GOPATH=$HOME/golang; export GOPATH
GOROOT=/usr/local/go/; export GOROOT

```

Now we can proceed with the normal build process of minio server as found [here](https://github.com/nomadlogic/minio/blob/master/CONTRIBUTING.md).  The only caveat is we need to specify gmake (GNU make) when building minio server as the current Makefile is not BSD make compatible:

```sh

$ mkdir -p $GOPATH/src/github.com/minio
$ cd $GOPATH/src/github.com/minio
$ git clone <paste saved URL for personal forked minio repo>
$ cd minio
$ gmake

```

From here you can start the server as you would with a precompiled minio server build.
