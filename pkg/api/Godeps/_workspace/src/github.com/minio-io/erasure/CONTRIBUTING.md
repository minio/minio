### Setup your Erasure Github Repository
Fork [Erasure upstream](https://github.com/minio-io/erasure/fork) source repository to your own personal repository. Copy the URL and pass it to ``go get`` command. Go uses git to clone a copy into your project workspace folder.
```sh
$ git clone https://github.com/$USER_ID/erasure
$ cd erasure
$ mkdir -p ${GOPATH}/src/github.com/minio-io
$ ln -s ${PWD} $GOPATH/src/github.com/minio-io/
```

### Compiling Erasure from source
```sh
$ go generate
$ go build
```
###  Developer Guidelines
To make the process as seamless as possible, we ask for the following:
* Go ahead and fork the project and make your changes. We encourage pull requests to discuss code changes.
  - Fork it
  - Create your feature branch (git checkout -b my-new-feature)
  - Commit your changes (git commit -am 'Add some feature')
  - Push to the branch (git push origin my-new-feature)
  - Create new Pull Request
* When you're ready to create a pull request, be sure to:
  - Have test cases for the new code. If you have questions about how to do it, please ask in your pull request.
  - Run `go fmt`
  - Squash your commits into a single commit. `git rebase -i`. It's okay to force update your pull request.
  - Make sure `go test -race ./...` and `go build` completes.
* Read [Effective Go](https://github.com/golang/go/wiki/CodeReviewComments) article from Golang project
  - `Erasure` project is strictly conformant with Golang style
  - if you happen to observe offending code, please feel free to send a pull request
