### Build Dependencies

Minio build depends on ``yasm`` and Go 1.3+

##### Install yasm

Yasm is a Modular Assembler used for compiling [Intel Storage Acceleration Library](https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version)

```sh
$ sudo apt-get install yasm
```
##### Install Go 1.3+ (Ubuntu)

[Download the archive](https://golang.org/dl/) and extract it into ``${HOME}/local``, creating a Go tree in ``${HOME}/local/go``. For example:
```sh
$ mkdir -p ${HOME}/local
$ tar -C ${HOME}/local -xzf go$VERSION.$OS-$ARCH.tar.gz
```

Choose the archive file appropriate for your installation. For instance, if you are installing Go version 1.2.1 for 64-bit x86 on Linux, the archive you want is called ``go1.2.1.linux-amd64.tar.gz``

(Typically these commands must be run as root or through sudo.)

Add ``${HOME}/local/go/bin`` to the ``PATH`` environment variable. You can do this by adding this line to your ``$HOME/.profile``:
```sh
$ export PATH=$PATH:${HOME}/local/go/bin
```

##### Setting up ``GOPATH`` environment variable

The ``GOPATH`` environment variable specifies the location of your workspace. It is likely the only environment variable you'll need to set when developing Go code.

To get started, create a workspace directory and set GOPATH accordingly. Your workspace can be located wherever you like, but we'll use ``$HOME/mygo`` in this document. Note that this must not be the same path as your Go installation.
```sh
$ mkdir -p $HOME/mygo
$ export GOPATH=$HOME/mygo
```

For convenience, add the workspace's bin subdirectory to your ``PATH``:
```sh
$ export PATH=$PATH:$GOPATH/bin
```

For more detailed documentation refer [GOPATH](http://golang.org/doc/code.html#GOPATH)

### Installing Minio (Source)

#### Get directly from GitHub:

Once we are finished the prerequisites in the previous step we now build minio

```sh
$ go get -u github.com/minio-io/minio
$ cd $GOPATH/src/github.com/minio-io/minio
$ make
...
```

#### Clone locally (for contributors):

```sh
$ git clone https://github.com/minio-io/minio
$ cd minio
$ make
```

Because Go expects all of your libraries to be found in either $GOROOT or $GOPATH, it's necessary to symlink the project to the following path:

```sh
$ ln -s /path/to/your/minio $GOPATH/src/github.com/minio-io/minio
```

###  Contribution Guidelines

We welcome your contributions. To make the process as seamless as possible, we ask for the following:

* Go ahead and fork the project and make your changes. We encourage pull requests to discuss code changes.
  - Fork it
  - Create your feature branch (git checkout -b my-new-feature)
  - Commit your changes (git commit -am 'Add some feature')
  - Push to the branch (git push origin my-new-feature)
  - Create new Pull Request

* When you're ready to create a pull request, be sure to:
  - Have test cases for the new code. If you have questions about how to do it, please ask in your pull request.
  - Run go fmt
  - Squash your commits into a single commit. git rebase -i. It's okay to force update your pull request.
  - Make sure go test ./... passes, and go build completes.

##### NOTE

This document assumes that minio user is using Ubuntu 12.04 or 14.04 LTS release
