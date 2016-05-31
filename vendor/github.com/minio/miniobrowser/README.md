# Minio File Browser

``MinioBrowser`` provides minimal set of UI to manage buckets and objects on ``minio`` server. ``MinioBrowser`` is written in javascript and released under [Apache 2.0 License](./LICENSE).

## Installation

```sh
$ git clone https://github.com/minio/MinioBrowser
$ cd MinioBrowser
$ npm install
```

### Install `go-bindata` and `go-bindata-assetfs`.

If you do not have a working Golang environment, please follow [Install Golang](./INSTALLGO.md).

```sh
$ go get github.com/jteeuwen/go-bindata/...
$ go get github.com/elazarl/go-bindata-assetfs/...
```

## For development environment with live reload.

```
$ npm run dev
```

## Generating Assets.

### Development version

```sh
$ npm run build
```

### Released version

```sh
$ npm run release
```
