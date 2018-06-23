# For maintainers only

### Setup your minio GitHub Repository

Fork [minio upstream](https://github.com/minio/minio/fork) source repository to your own personal repository.
```bash
$ mkdir -p $GOPATH/src/github.com/minio
$ cd $GOPATH/src/github.com/minio
$ git clone https://github.com/$USER_ID/minio
$ 
```

``minio`` uses [govendor](https://github.com/kardianos/govendor) for its dependency management.

### To manage dependencies

#### Add new dependencies

  - Run `go get foo/bar`
  - Edit your code to import foo/bar
  - Run `govendor add foo/bar` from top-level directory

#### Remove dependencies 

  - Run `govendor remove foo/bar`

#### Update dependencies

  - Run `govendor remove +vendor`
  - Run to update the dependent package `go get -u foo/bar`
  - Run `govendor add +external`

### Making new releases 

`minio` doesn't follow semantic versioning style, `minio` instead uses the release date and time as the release versions.

`make release` will generate new binary into `release` directory.
