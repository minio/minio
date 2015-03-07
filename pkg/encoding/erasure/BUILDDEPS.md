## Ubuntu (Kylin) 14.04
### Build Dependencies
This installation document assumes Ubuntu 14.04 or later on x86-64 platform.

##### Install YASM

Erasure depends on Intel ISAL library, ISAL uses Intel AVX2 processor instructions, to compile these files one needs to install ``yasm`` which supports AVX2 instructions. AVX2 support only ended in ``yasm`` from version ``1.2.0``, any version below ``1.2.0`` will throw a build error.

```sh
$ sudo apt-get install yasm
```

##### Install Go 1.4+
Download Go 1.4+ from [https://golang.org/dl/](https://golang.org/dl/) and extract it into ``${HOME}/local`` and setup ``${HOME}/mygo`` as your project workspace folder.
For example:
```sh
.... Extract and install golang ....

$ wget https://storage.googleapis.com/golang/go1.4.linux-amd64.tar.gz
$ mkdir -p ${HOME}/local
$ mkdir -p $HOME/mygo
$ tar -C ${HOME}/local -xzf go1.4.linux-amd64.tar.gz

.... Export necessary environment variables ....

$ export PATH=$PATH:${HOME}/local/go/bin
$ export GOROOT=${HOME}/local/go
$ export GOPATH=$HOME/mygo
$ export PATH=$PATH:$GOPATH/bin

.... Add paths to your bashrc ....

$ echo "export PATH=$PATH:${HOME}/local/go/bin" >> ${HOME}/.bashrc
$ echo "export GOROOT=${HOME}/local/go" >> ${HOME}/.bashrc
$ echo "export GOPATH=$HOME/mygo" >> ${HOME}/.bashrc
$ echo "export PATH=$PATH:$GOPATH/bin" >> ${HOME}/.bashrc
```

## Mac OSX (Yosemite) 10.10
### Build Dependencies
This installation document assumes Mac OSX Yosemite 10.10 or later on x86-64 platform.

##### Install brew
```sh
$ ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

##### Install Git
```sh
$ brew install git
```

##### Install YASM

Erasure depends on Intel ISAL library, ISAL uses Intel AVX2 processor instructions, to compile these files one needs to install ``yasm`` which supports AVX2 instructions. AVX2 support only ended in ``yasm`` from version ``1.2.0``, any version below ``1.2.0`` will throw a build error.

```sh
$ brew install yasm
```

##### Install Go 1.4+
On MacOSX ``brew.sh`` is the best way to install golang

For example:
```sh
.... Install golang using `brew` ....

$ brew install go
$ mkdir -p $HOME/mygo

.... Export necessary environment variables ....

$ export GOPATH=$HOME/mygo
$ export PATH=$PATH:$GOPATH/bin

.... Add paths to your bashrc ....

$ echo "export GOPATH=$HOME/mygo" >> ${HOME}/.bashrc
$ echo "export PATH=$PATH:$GOPATH/bin" >> ${HOME}/.bashrc
```
