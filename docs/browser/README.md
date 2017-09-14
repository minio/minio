## Minio Browser

Minio Browser uses Json Web Tokens to authenticate JSON RPC requests.

Initial request generates a token for 'AccessKey' and 'SecretKey'
provided by the user.

<blockquote>
Currently these tokens expire after 10hrs, this is not configurable yet.
</blockquote>

### Start minio server

```
minio server /data
```

### JSON RPC APIs.

JSON RPC namespace is `Web`.

#### Server operations.

* ServerInfo - fetches current server information, includes memory statistics, minio binary
  version, golang runtime version and more.
* StorageInfo - fetches disc space availability(Total/Free), Type, Online/Offline status of disc with counts along with ReadQuorum and WriteQuorum counts.   

#### Auth operations

* Login - waits for 'username, password' and on success replies a new Json Web Token (JWT).
* SetAuth - change access credentials with new 'username, password'.
* GetAuth - fetch the current auth from the server.

#### Bucket/Object operations.

* ListBuckets - lists buckets, requires a valid token.
* ListObjects - lists objects, requires a valid token.
* MakeBucket - make a new bucket, requires a valid token.
* RemoveObject - removes an object from a bucket, requires a valid token.
* Upload - uploads a new object from the browser, requires a valid token.
* Download - downloads an object from a bucket, requires a valid token.
