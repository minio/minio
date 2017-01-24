# Minio File Browser

``Minio Browser`` provides minimal set of UI to manage buckets and objects on ``minio`` server. ``Minio Browser`` is written in javascript and released under [Apache 2.0 License](./LICENSE).

## Installation

### Install yarn:
```sh
$ curl -o- -L https://yarnpkg.com/install.sh | bash
$ yarn
```

### Install `go-bindata` and `go-bindata-assetfs`.

If you do not have a working Golang environment, please follow [Install Golang](https://docs.minio.io/docs/how-to-install-golang)

```sh
$ go get github.com/jteeuwen/go-bindata/...
$ go get github.com/elazarl/go-bindata-assetfs/...
```

## Generating Assets.

### Generate ui-assets.go

```sh
$ yarn release
```
This generates ui-assets.go in the current direcotry. Now do `make` in the parent directory to build the minio binary with the newly generated ui-assets.go

### Run Minio Browser with live reload.

```sh
$ yarn dev
```

Open [http://localhost:8080/minio/](http://localhost:8080/minio/) in your browser to play with the application
