### Generate RSA keys for JWT

```
mkdir -p ~/.minio/web
```

```
openssl genrsa -out ~/.minio/web/private.key 2048
```

```
openssl rsa -in ~/.minio/web/private.key -outform PEM -pubout -out ~/.minio/web/public.key
```
### Start minio server

```
minio server <testdir>
```

### Implemented JSON RPC APIs.

Namespace `Web`

* Login - waits for 'username, password' and on success replies a new JWT token.
* ResetToken - resets token, requires password and token.
* Logout - currently a dummy operation.
* ListBuckets - lists buckets, requires valid token.
* ListObjects - lists objects, requires valid token.
* GetObjectURL - generates a url for download access, requires valid token.

### Now you can use `webrpc.js` to make requests.

- Login example
```js
var webRPC = require('webrpc');
var web = new webRPC("http://localhost:9001/rpc")

// Generate JWT Token.
web.Login({"username": "YOUR-ACCESS-KEY-ID", "password": "YOUR-SECRET-ACCESS-KEY"})
  .then(function(data) {
    console.log("success : ", data);
  })
  .catch(function(error) {
    console.log("fail : ", error.toString());
  });
```

- ListBuckets example
```js
var webRPC = require('webrpc');
var web = new webRPC("http://localhost:9001/rpc", "my-token")

// Generate Token.
web.ListBuckets()
  .then(function(data) {
    console.log("Success : ", data);
  })
  .catch(function(error) {
    console.log("fail : ", error.toString());
  });
```
