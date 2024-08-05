# MinIO Contribution Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

``MinIO`` community welcomes your contribution. To make the process as seamless as possible, we recommend you read this contribution guide.

## Development Workflow

Start by forking the MinIO GitHub repository, make changes in a branch and then send a pull request. We encourage pull requests to discuss code changes. Here are the steps in details:

### Setup your MinIO GitHub Repository

Fork [MinIO upstream](https://github.com/minio/minio/fork) source repository to your own personal repository. Copy the URL of your MinIO fork (you will need it for the `git clone` command below).

```sh
git clone https://github.com/minio/minio
cd minio
go install -v
ls $(go env GOPATH)/bin/minio
```

### Set up git remote as ``upstream``

```sh
$ cd minio
$ git remote add upstream https://github.com/minio/minio
$ git fetch upstream
$ git merge upstream/master
...
```

### Create your feature branch

Before making code changes, make sure you create a separate branch for these changes

```
git checkout -b my-new-feature
```

### Test MinIO server changes

After your code changes, make sure

- To add test cases for the new code. If you have questions about how to do it, please ask on our [Slack](https://slack.min.io) channel.
- To run `make verifiers`
- To squash your commits into a single commit. `git rebase -i`. It's okay to force update your pull request.
- To run `make test` and `make build` completes.

### Commit changes

After verification, commit your changes. This is a [great post](https://chris.beams.io/posts/git-commit/) on how to write useful commit messages

```
git commit -am 'Add some feature'
```

### Push to the branch

Push your locally committed changes to the remote origin (your fork)

```
git push origin my-new-feature
```

### Create a Pull Request

Pull requests can be created via GitHub. Refer to [this document](https://help.github.com/articles/creating-a-pull-request/) for detailed steps on how to create a pull request. After a Pull Request gets peer reviewed and approved, it will be merged.

## FAQs

### How does ``MinIO`` manage dependencies?

``MinIO`` uses `go mod` to manage its dependencies.

- Run `go get foo/bar` in the source folder to add the dependency to `go.mod` file.

To remove a dependency

- Edit your code and remove the import reference.
- Run `go mod tidy` in the source folder to remove dependency from `go.mod` file.

### What are the coding guidelines for MinIO?

``MinIO`` is fully conformant with Golang style. Refer: [Effective Go](https://github.com/golang/go/wiki/CodeReviewComments) article from Golang project. If you observe offending code, please feel free to send a pull request or ping us on [Slack](https://slack.min.io).
