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
$ wget https://storage.googleapis.com/golang/go1.3.3.linux-amd64.tar.gz
$ mkdir -p ${HOME}/local
$ tar -C ${HOME}/local -xzf go1.3.3.linux-amd64.tar.gz
$ export PATH=$PATH:${HOME}/local/go/bin
$ mkdir -p $HOME/mygo
$ export GOPATH=$HOME/mygo
$ export PATH=$PATH:$GOPATH/bin
```

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
...
```

###  Contribution Guidelines
We welcome your contributions. To make the process as seamless as possible, we ask for the following:
* Go ahead and fork the project and make your changes. We encourage pull requests to discuss code changes.
  - Fork it
  - Create your feature branch (git checkout -b my-new-feature)
  - Commit your changes (git commit -am 'Add some feature')
  - Push to the branch (git push origin my-new-feature)
  - Create new Pull Request
* If you have additional dependencies for ``minio``, ``minio`` manages its depedencies using [godep](https://github.com/tools/godep)
  - Run `go get foo/bar`
  - Edit your code to import foo/bar
  - Run `make save` from top-level directory (or `godep restore && godep save ./...`).
* When you're ready to create a pull request, be sure to:
  - Have test cases for the new code. If you have questions about how to do it, please ask in your pull request.
  - Run go fmt
  - Squash your commits into a single commit. git rebase -i. It's okay to force update your pull request.
  - Make sure go test -race ./... passes, and go build completes.
