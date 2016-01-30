## Minio [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Minio is a distributed object storage server written in Golang. Source is available under free software / open source [Apache license 2.0](./LICENSE). API compatible with Amazon S3 cloud storage service.

## Description

Micro services environment provisions one Minio server per application instance. Scalability is achieved through large number of smaller personalized instances. This version of the Minio binary is built using Filesystem storage backend for magnetic and solid state disks.

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

#### Docker container

Download ``minio`` for docker.

~~~
$ docker pull minio/minio
~~~

Read more here on [How to configure data volume containers for Minio?](./Docker.md)

#### Source
<blockquote>
NOTE: Source installation is intended for only developers and advanced users. ‘minio update’ continous delivery mechanism is not supported for ‘go get’ based binary builds. Please download official releases from https://minio.io/#minio.
</blockquote>

If you do not have a working Golang environment, please follow [Install Golang](./INSTALLGO.md).

```sh
$ GO15VENDOREXPERIMENT=1 go get -u github.com/minio/minio
```

### How to use Minio?

```
NAME:
  minio server - Start Minio cloud storage server.

USAGE:
  minio server [OPTION VALUE] PATH

  OPTION = expiry        VALUE = NN[h|m|s] [DEFAULT=Unlimited]
  OPTION = min-free-disk VALUE = NN% [DEFAULT: 10%]

EXAMPLES:
  1. Start minio server on Linux.
        $ minio server /home/shared

  2. Start minio server on Windows.
        $ minio server C:\MyShare

  3. Start minio server bound to a specific IP:PORT, when you have multiple network interfaces.
        $ minio --address 192.168.1.101:9000 server /home/shared

  4. Start minio server with minimum free disk threshold to 5%
        $ minio server min-free-disk 5% /home/shared/Pictures

  5. Start minio server with minimum free disk threshold to 15% with auto expiration set to 1h
        $ minio server min-free-disk 15% expiry 1h /home/shared/Documents
```

#### Start Minio server.

~~~
$ minio server ~/Photos
AccessKey: G5GJRH51R2HSUWYPGIX5  SecretKey: uxhBC1Yscut3/u81l5L8Yp636ZUk32N4m/gFASuZ

To configure Minio Client.

	$ wget https://dl.minio.io/client/mc/release/linux-amd64/mc
	$ chmod 755 mc
	$ ./mc config host add myminio http://localhost:9000 G5GJRH51R2HSUWYPGIX5 uxhBC1Yscut3/u81l5L8Yp636ZUk32N4m/gFASuZ
	$ ./mc mb myminio/photobucket
	$ ./mc cp --recursive ~/Photos myminio/photobucket

Starting minio server:
Listening on http://127.0.0.1:9000
Listening on http://172.30.2.17:9000
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
2016-01-07 16:38:23 testbucket
```

To list contents inside bucket.
```
$ aws --endpoint-url http://localhost:9000 s3 ls s3://testbucket
                           PRE test/
2015-12-17 08:46:41   12232928 vim
2016-01-07 16:38:23   32232928 emacs
2015-12-09 08:05:24     138504 s3cmd
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
```

To list your buckets.
```
$ s3cmd ls s3://
2015-12-09 16:12  s3://testbbucket
```

To list contents inside buckets.
```
$ s3cmd ls s3://testbucket/
                       DIR   s3://testbucket/test/
2015-12-09 16:05    138504   s3://testbucket/newfile
```

## Contribute to Minio Project
Please follow Minio [Contributor's Guide](./CONTRIBUTING.md)

### Jobs
If you think in Lisp or Haskell and hack in go, you would blend right in. Send your github link to callhome@minio.io.
