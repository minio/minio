### Build Dependencies
This installation document assumes Ubuntu 12.04 or later on x86-64.

##### Install Git and GCC
```sh
$ sudo apt-get install git build-essential
```

##### Install YASM

Minio depends on Intel ISAL library for erasure coding, ISAL uses Intel AVX2 processor instructions, to compile these files one needs to install ``yasm`` which supports AVX2 instructions. AVX2 support only ended in ``yasm`` from version ``1.2.0``, any version below ``1.2.0`` will throw a build error as referenced in issue [here](https://github.com/Minio-io/minio/issues/163)

```sh
$ sudo apt-get install yasm
```

##### Install Go 1.3+
Download Go 1.3+ from [https://golang.org/dl/](https://golang.org/dl/) and extract it into ``${HOME}/local`` and setup ``${HOME}/mygo`` as your project workspace folder.
For example:
```sh
$ wget https://storage.googleapis.com/golang/go1.4.linux-amd64.tar.gz
$ mkdir -p ${HOME}/local
$ tar -C ${HOME}/local -xzf go1.4.linux-amd64.tar.gz
$ export PATH=$PATH:${HOME}/local/go/bin
$ export GOROOT=${HOME}/local/go
$ mkdir -p $HOME/mygo
$ export GOPATH=$HOME/mygo
$ export PATH=$PATH:$GOPATH/bin
```
