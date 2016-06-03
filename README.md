## Minio [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Minio is an object storage server compatible with Amazon S3 and licensed under [Apache license 2.0](./LICENSE).

## Description

Minio is an open source object storage server released under Apache License V2. It is compatible with Amazon S3 cloud storage service. Minio follows a minimalist design philosophy.
Minio is light enough to be bundled with the application stack. It sits on the side of NodeJS, Redis, MySQL and the likes. Unlike databases, Minio stores objects such as photos, videos, log files, backups, container / VM images and so on. Minio is best suited for storing blobs of information ranging from KBs to 5 TBs each. In a simplistic sense, it is like a FTP server with a simple get / put API over HTTP.

Minio currently implements two backends

  - Filesystem (FS) - is available and ready for general purpose use. This version of the Minio binary is built using Filesystem storage backend for magnetic and solid state disks.
  - ErasureCoded (XL) - is work in progress and not ready for general purpose use.

## Minio Client

[Minio Client (mc)](https://github.com/minio/mc#minio-client-mc-) provides a modern alternative to Unix commands like ``ls``, ``cat``, ``cp``, ``sync``, and ``diff``. It supports POSIX compatible filesystems and Amazon S3 compatible cloud storage systems. It is entirely written in Golang.

## Amazon S3 Compatible Client Libraries
- [Golang Library](https://github.com/minio/minio-go)
- [Java Library](https://github.com/minio/minio-java)
- [Nodejs Library](https://github.com/minio/minio-js)
- [Python Library](https://github.com/minio/minio-py)
- [.Net Library](https://github.com/minio/minio-dotnet)

### Install [![Build Status](https://travis-ci.org/minio/minio.svg?branch=master)](https://travis-ci.org/minio/minio)[![Build status](https://ci.appveyor.com/api/projects/status/royh137dni8yevep/branch/master?svg=true)](https://ci.appveyor.com/project/harshavardhana/minio-qxbjq/branch/master)

#### GNU/Linux

Download ``minio`` for:

- ``64-bit Intel`` from https://dl.minio.io/server/minio/release/linux-amd64/minio
- ``32-bit Intel`` from https://dl.minio.io/server/minio/release/linux-386/minio
- ``32-bit ARM`` from https://dl.minio.io/server/minio/release/linux-arm/minio

~~~
$ chmod +x minio
$ ./minio --help
~~~

#### OS X

Download ``minio`` from https://dl.minio.io/server/minio/release/darwin-amd64/minio

~~~
$ chmod 755 minio
$ ./minio --help
~~~

#### Microsoft Windows

Download ``minio`` for:

- ``64-bit`` from https://dl.minio.io/server/minio/release/windows-amd64/minio.exe
- ``32-bit`` from https://dl.minio.io/server/minio/release/windows-386/minio.exe

~~~
C:\Users\Username\Downloads> minio.exe --help
~~~

#### FreeBSD

Download ``minio`` from https://dl.minio.io/server/minio/release/freebsd-amd64/minio

~~~
$ chmod 755 minio
$ ./minio --help
~~~
Read more here on [How to configure Minio on FreeBSD with ZFS backend.](./FreeBSD.md)

#### Docker container

Download ``minio`` for docker.

~~~
$ docker pull minio/minio
~~~

Read more here on [How to configure data volume containers for Minio?](./Docker.md)

#### Source
<blockquote>
NOTE: Source installation is intended for only developers and advanced users.  For general use, please download official releases from https://minio.io/downloads.
</blockquote>

If you do not have a working Golang environment, please follow [Install Golang](./INSTALLGO.md).

~~~
$ go get -d github.com/minio/minio
$ cd $GOPATH/src/github.com/minio/minio
$ make
~~~

### How to use Minio?

Start minio server.

~~~
$ minio server ~/Photos

AccessKey: WLGDGYAQYIGI833EV05A  SecretKey: BYvgJM101sHngl2uzjXS/OBF/aMxAN06JrJ3qJlF  Region: us-east-1

Minio Object Storage:
    http://127.0.0.1:9000
    http://10.1.10.177:9000

Minio Browser:
    http://127.0.0.1:9000
    http://10.1.10.177:9000

To configure Minio Client:
    $ wget https://dl.minio.io/client/mc/release/darwin-amd64/mc
    $ chmod 755 mc
    $ ./mc config host add myminio http://localhost:9000 WLGDGYAQYIGI833EV05A BYvgJM101sHngl2uzjXS/OBF/aMxAN06JrJ3qJlF
~~~

#### How to use AWS CLI with Minio?

<blockquote>
This section assumes that you have already installed aws-cli, if not please visit https://aws.amazon.com/cli/
</blockquote>

To configure `aws-cli`, type `aws configure` and follow below steps.

```
$ aws configure
AWS Access Key ID [None]: YOUR_ACCESS_KEY_HERE
AWS Secret Access Key [None]: YOUR_SECRET_KEY_HERE
Default region name [None]: us-east-1
Default output format [None]: ENTER
```

Additionally enable `aws-cli` to use AWS Signature Version '4' for Minio server.

```
$ aws configure set default.s3.signature_version s3v4
```

To list your buckets.
```
$ aws --endpoint-url http://localhost:9000 s3 ls
2016-03-27 02:06:30 deebucket
2016-03-28 21:53:49 guestbucket
2016-03-29 13:34:34 mbtest
2016-03-26 22:01:36 mybucket
2016-03-26 15:37:02 testbucket
```

To list contents inside bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket
2016-03-30 00:26:53      69297 argparse-1.2.1.tar.gz
2016-03-30 00:35:37      67250 simplejson-3.3.0.tar.gz
```

To make a bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
make_bucket: s3://mybucket/
```

To add an object to a bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 cp simplejson-3.3.0.tar.gz s3://mybucket
upload: ./simplejson-3.3.0.tar.gz to s3://mybucket/simplejson-3.3.0.tar.gz
```

Delete an object from a bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/argparse-1.2.1.tar.gzdelete: s3://mybucket/argparse-1.2.1.tar.gz
```

Remove a bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 rb s3://mybucket
remove_bucket: s3://mybucket/
```

#### How to use AWS SDK with Minio?

Please follow the documentation here - [Using aws-sdk-go with Minio server](./AWS-SDK-GO.md)

#### How to use s3cmd with Minio?

<blockquote>
This section assumes that you have already installed s3cmd, if not please visit http://s3tools.org/s3cmd
</blockquote>

Edit the following fields in your s3cmd configuration file `~/.s3cfg` .

```
host_base = localhost:9000
host_bucket = localhost:9000
access_key = YOUR_ACCESS_KEY_HERE
secret_key = YOUR_SECRET_KEY_HERE
signature_v2 = False
bucket_location = us-east-1
```

To make a bucket.
```
$ s3cmd mb s3://mybucket
Bucket 's3://mybucket/' created
```

To copy an object to bucket.
```
$ s3cmd put newfile s3://testbucket
upload: 'newfile' -> 's3://testbucket/newfile'  
```

To copy an object to local system.
```
$ s3cmd get s3://testbucket/newfile
download: 's3://testbucket/newfile' -> './newfile'
```

To sync local file/directory to a bucket.
```
$ s3cmd sync newdemo s3://testbucket
upload: 'newdemo/newdemofile.txt' -> 's3://testbucket/newdemo/newdemofile.txt'
```

To sync bucket or object with local filesystem.
```
$ s3cmd sync  s3://otherbucket otherlocalbucket
download: 's3://otherbucket/cat.jpg' -> 'otherlocalbucket/cat.jpg'
```

To list buckets.
```
$ s3cmd ls s3://
2015-12-09 16:12  s3://testbbucket
```

To list contents inside bucket.
```
$ s3cmd ls s3://testbucket/
                       DIR   s3://testbucket/test/
2015-12-09 16:05    138504   s3://testbucket/newfile
```

Delete an object from bucket.
```
$ s3cmd del s3://testbucket/newfile
delete: 's3://testbucket/newfile'
```

Delete a bucket.
```
$ s3cmd rb s3://testbucket
Bucket 's3://testbucket/' removed
```

## Contribute to Minio Project
Please follow Minio [Contributor's Guide](./CONTRIBUTING.md)

### Jobs
If you think in Lisp or Haskell and hack in go, you would blend right in. Send your github link to callhome@minio.io.
