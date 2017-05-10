# Minio File Browser

``Minio Browser`` provides minimal set of UI to manage buckets and objects on ``minio`` server. ``Minio Browser`` is written in javascript and released under [Apache 2.0 License](./LICENSE).

## Installation

### Install yarn
```sh
curl -o- -L https://yarnpkg.com/install.sh | bash
yarn
```

### Install `go-bindata` and `go-bindata-assetfs`

If you do not have a working Golang environment, please follow [Install Golang](https://docs.minio.io/docs/how-to-install-golang)

```sh
go get github.com/jteeuwen/go-bindata/...
go get github.com/elazarl/go-bindata-assetfs/...
```

## Generating Assets

### Generate ui-assets.go

```sh
yarn release
```

This generates ui-assets.go in the current directory. Now do `make` in the parent directory to build the minio binary with the newly generated ``ui-assets.go``

### Run Minio Browser with live reload

```sh
yarn dev
```

Open [http://localhost:8080/minio/](http://localhost:8080/minio/) in your browser to play with the application

### Run Minio Browser with live reload on custom port

Edit `browser/webpack.config.js`

```diff
diff --git a/browser/webpack.config.js b/browser/webpack.config.js
index 3ccdaba..9496c56 100644
--- a/browser/webpack.config.js
+++ b/browser/webpack.config.js
@@ -58,6 +58,7 @@ var exports = {
     historyApiFallback: {
       index: '/minio/'
     },
+    port: 8888,
     proxy: {
       '/minio/webrpc': {
        target: 'http://localhost:9000',
@@ -97,7 +98,7 @@ var exports = {
 if (process.env.NODE_ENV === 'dev') {
   exports.entry = [
     'webpack/hot/dev-server',
-    'webpack-dev-server/client?http://localhost:8080',
+    'webpack-dev-server/client?http://localhost:8888',
     path.resolve(__dirname, 'app/index.js')
   ]
 }
```

```sh
yarn dev
```

Open [http://localhost:8888/minio/](http://localhost:8888/minio/) in your browser to play with the application
