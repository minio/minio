## Minio Server [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Minio is a minimal cloud storage server written in Golang and licensed under [Apache license v2](./LICENSE). Minio is compatible with Amazon S3 APIs. [![Build Status](https://travis-ci.org/minio/minio.svg?branch=master)](https://travis-ci.org/minio/minio)

## Minio Client

[Minio Client (mc)](https://github.com/minio/mc#minio-client-mc-) provides a modern alternative to Unix commands like ``ls``, ``cat``, ``cp``, ``sync``, and ``diff``. It supports POSIX compatible filesystems and Amazon S3 compatible cloud storage systems. It is entirely written in Golang.

## Amazon S3 Compatible Client Libraries
- [Golang Library](https://github.com/minio/minio-go)
- [Java Library](https://github.com/minio/minio-java)
- [Nodejs Library](https://github.com/minio/minio-js)
- [Python Library](https://github.com/minio/minio-py)

## Server Roadmap
~~~
Storage Backend:
- Donut: Erasure coded backend.
 - Status: Standalone mode complete.
Storage Operations:
- Collective:
  - Status: Work in progress.

Storage Management:
- WebCLI:
  - Status: Work in progress.
- Authentication:
  - Status: Work in progress.
- Admin Console:
  - Status: Work in progress.
- User Console:
  - Status: Work in progress.
- Logging:
  - Status: Work in progress.
~~~


### Install 

<blockquote>
NOTE: If you happen to compile from source code, following options are not available anymore. Minio master branch is going through lots of rapid changes, documentation will be updated subsequently. 
</blockquote>

#### GNU/Linux

Download ``minio`` from https://dl.minio.io:9000/updates/2015/Jun/linux-amd64/minio

~~~
$ wget https://dl.minio.io:9000/updates/2015/Jun/linux-amd64/minio
$ chmod +x minio
$ ./minio mode memory limit 12GB expire 2h
~~~

#### OS X

Download ``minio`` from https://dl.minio.io:9000/updates/2015/Jun/darwin-amd64/minio

~~~
$ wget https://dl.minio.io:9000/updates/2015/Jun/darwin-amd64/minio
$ chmod +x minio
$ ./minio mode memory limit 12GB expire 2h
~~~

### How to use Minio?

[![asciicast](https://asciinema.org/a/21575.png)](https://asciinema.org/a/21575)

### Contribute to Minio Project
Please follow Minio [Contributor's Guide](./CONTRIBUTING.md)

### Jobs
If you think in Lisp or Haskell and hack in go, you would blend right in. Send your github link to callhome@minio.io.


