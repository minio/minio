## Introduction

Minio is an open source object storage released under [Apache license v2](./LICENSE) . It uses ``Rubberband Erasure`` coding to dynamically protect the data.
Minio's design is inspired by Amazon's S3 for its API and Facebook's Haystack for its immutable data structure.

### Install BUILD dependencies

[Build Dependencies](./DEVELOPERS.md)

### Setup your Minio Github Repository &nbsp; &nbsp; <iframe src="http://ghbtns.com/github-btn.html?user=Minio-io&repo=minio&type=fork&count=true&size=large" height="30" width="170" frameborder="0" scrolling="0" style="width:170px; height: 30px;" allowTransparency="true"></iframe>
Fork [Minio upstream](https://github.com/Minio-io/minio) source repository to your own personal repository. Copy the URL and pass it to ``go get`` command. Go uses git to clone a copy into your project workspace folder.
```sh
$ go get -u github.com/$USER_ID/minio
$ cd $GOPATH/src/github.com/$USER_ID/minio/
$ git remote add upstream https://github.com/Minio-io/minio.git
```

### Compiling Minio from source
Minio use plain Makefile to wrap around some of the limitations of ``go build``. To compile Minio source, simply change to your workspace folder and type ``make``.
```sh
$ cd $GOPATH/src/github.com/$USER_ID/minio/
$ make
Checking dependencies for Minio.. SUCCESS
...
...
Installed minio into /home/harsha/.gvm/pkgsets/go1.4/global/bin
Installed minio-cli into /home/harsha/.gvm/pkgsets/go1.4/global/bin
```

### Contribute

[Howto Contribute](./CONTRIB.md)

### Join Community

* IRC join channel #minio @ irc.freenode.net
* Google Groups - minio-dev@googlegroups.com

[![Analytics](https://ga-beacon.appspot.com/UA-56860620-3/minio/readme)](https://github.com/igrigorik/ga-beacon)
