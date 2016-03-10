
###  Developer Guidelines

``minio-go`` welcomes your contribution. To make the process as seamless as possible, we ask for the following:

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
      NOTE: go test runs functional tests and requires you to have a AWS S3 account. Set them as environment variables
      ``ACCESS_KEY`` and ``SECRET_KEY``. To run shorter version of the tests please use ``go test -short -race ./...``

* Read [Effective Go](https://github.com/golang/go/wiki/CodeReviewComments) article from Golang project
    - `minio-go` project is strictly conformant with Golang style
    - if you happen to observe offending code, please feel free to send a pull request
