### Build Dependencies
This installation document assumes Ubuntu 12.04 or later on x86-64.

##### Install Git and GCC
```sh
$ sudo apt-get install git build-essential
```

##### Install Mercurial
```sh
$ sudo apt-get install mercurial
```

##### Install YASM
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
$ mkdir -p $HOME/mygo
$ export GOPATH=$HOME/mygo
$ export PATH=$PATH:$GOPATH/bin
```
