# MinIO File Browser

``MinIO Browser`` provides minimal set of UI to manage buckets and objects on ``minio`` server. ``MinIO Browser`` is written in javascript and released under [Apache 2.0 License](./LICENSE).

## Installation

### Install node
```sh
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
exec -l $SHELL
nvm install stable
```

### Install node dependencies
```sh
npm install
```

### Install `go-bindata` and `go-bindata-assetfs`

If you do not have a working Golang environment, please follow [Install Golang](https://golang.org/doc/install)

```sh
go get github.com/go-bindata/go-bindata/go-bindata
go get github.com/elazarl/go-bindata-assetfs/go-bindata-assetfs
```

## Generating Assets

### Generate ui-assets.go

```sh
npm run release
```

This generates ui-assets.go in the current directory. Now do `make` in the parent directory to build the minio binary with the newly generated ``ui-assets.go``

### Run MinIO Browser with live reload

```sh
npm run dev
```

Open [http://localhost:8080/minio/](http://localhost:8080/minio/) in your browser to play with the application

### Run MinIO Browser with live reload on custom port

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
npm run dev
```

Open [http://localhost:8888/minio/](http://localhost:8888/minio/) in your browser to play with the application

### Run MinIO Browser with live reload on any IP

Edit `browser/webpack.config.js`

```diff
diff --git a/browser/webpack.config.js b/browser/webpack.config.js
index 8bdbba53..139f6049 100644
--- a/browser/webpack.config.js
+++ b/browser/webpack.config.js
@@ -71,6 +71,7 @@ var exports = {
     historyApiFallback: {
       index: '/minio/'
     },
+    host: '0.0.0.0',
     proxy: {
       '/minio/webrpc': {
        target: 'http://localhost:9000',

```

```sh
npm run dev
```

Open [http://IP:8080/minio/](http://IP:8080/minio/) in your browser to play with the application
