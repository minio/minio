### Install Golang

If you do not have a working Golang environment setup please follow [Golang Installation Guide](./INSTALLGO.md).

### Setup your Minio Github Repository
Fork [Minio upstream](https://github.com/minio/minio/fork) source repository to your own personal repository. Copy the URL and pass it to ``go get`` command. Go uses git to clone a copy into your project workspace folder.
```sh
$ mkdir -p $GOPATH/src/github.com/minio
$ cd $GOPATH/src/github.com/minio
$ git clone https://github.com/$USER_ID/minio
$ cd minio
```

### Compiling Minio from source
Minio uses ``Makefile`` to wrap around some of the limitations of ``go`` build system. To compile Minio source, simply change to your workspace folder and type ``make``.
```sh
$ make
Checking if proper environment variables are set.. Done
...
Checking dependencies for Minio.. Done
Installed govet
Building Libraries
...
...
```

### Setting up git remote as ``upstream``
```sh
$ cd $GOPATH/src/github.com/minio/minio
$ git remote add upstream https://github.com/minio/minio
$ git fetch upstream
$ git merge upstream/master
...
...
$ make
Checking if proper environment variables are set.. Done
...
Checking dependencies for Minio.. Done
Installed govet
Building Libraries
...
```

###  Developer Guidelines
``Minio`` community welcomes your contribution. To make the process as seamless as possible, we ask for the following:
* Go ahead and fork the project and make your changes. We encourage pull requests to discuss code changes.
    - Fork it
    - Create your feature branch (git checkout -b my-new-feature)
    - Commit your changes (git commit -am 'Add some feature')
    - Push to the branch (git push origin my-new-feature)
    - Create new Pull Request

* If you have additional dependencies for ``Minio``, ``Minio`` manages its depedencies using [govendor](https://github.com/kardianos/govendor)
    - Run `go get foo/bar`
    - Edit your code to import foo/bar
    - Run `govendor add foo/bar` from top-level directory

* When you're ready to create a pull request, be sure to:
    - Have test cases for the new code. If you have questions about how to do it, please ask in your pull request.
    - Run `make verifiers`
    - Squash your commits into a single commit. `git rebase -i`. It's okay to force update your pull request.
    - Make sure `go test -race ./...` and `go build` completes.

* Read [Effective Go](https://github.com/golang/go/wiki/CodeReviewComments) article from Golang project
    - `Minio` project is fully conformant with Golang style
    - if you happen to observe offending code, please feel free to send a pull request
