## Minio Server (minio) [![Build Status](https://travis-ci.org/minio/minio.svg)](https://travis-ci.org/minio/minio)

Minio is a minimal object storage server written in Golang and licensed under [Apache license v2](./LICENSE). Minio is compatible with Amazon S3 APIs. 

## Roadmap

~~~
Storage Backend:
- Donut: Erasure coded backend.
  - Status: Standalone mode complete.
- Memory: In-memory backend.
  - Status: Complete.
- Filesystem: Local disk filesystem backend.
  - Status: Work in progress.

Storage Operations:
- Collective:
  - Status: Not started.

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

### Minio Client (mc) 

``mc`` provides minimal tools to work with Amazon S3 compatible object storage and filesystems. Go to [Minio Client](https://github.com/minio/mc#minio-client-mc-).

### Minimal S3 Compatible Client Libraries
- [Golang Library](https://github.com/minio/minio-go)
- [Java Library](https://github.com/minio/minio-java)
- [Nodejs Library](https://github.com/minio/minio-js)

### Join The Community
* Community hangout on Gitter    [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* Ask questions on Quora  [![Quora](http://upload.wikimedia.org/wikipedia/commons/thumb/5/57/Quora_logo.svg/55px-Quora_logo.svg.png)](http://www.quora.com/Minio)

### Contribute
* [Contributors Guide](./CONTRIBUTING.md)

### Download

-- No releases yet --

### Supported platforms

| Name  | Supported |
| ------------- | ------------- |
| Linux  | Yes  |
| Mac OSX | Yes |
| Windows | Work in progress |

