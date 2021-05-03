# MinIO File Browser

``MinIO Browser`` provides minimal set of UI to manage buckets and objects on ``minio`` server.


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

## Generating Assets

> NOTE: if you are not part of MinIO organization please do not run this yourself and submit in a PR. Static assets in PRs are allowed only for authorized users.

```sh
npm run release
```

This generates `release` in the current directory.


## Run MinIO Browser with live reload

### Run MinIO Browser with live reload

```sh
npm run dev
```

Open [http://localhost:8080/minio/](http://localhost:8080/minio/) in your browser to play with the application.

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

Open [http://localhost:8888/minio/](http://localhost:8888/minio/) in your browser to play with the application.

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

Open [http://IP:8080/minio/](http://IP:8080/minio/) in your browser to play with the application.


## Run tests

    npm run test
