## Minio Browser

Minio Browser uses Json Web Tokens to authenticate JSON RPC requests.

Initial request generates a token for 'AccessKey' and 'SecretKey'
provided by the user.

<blockquote>
Currently these tokens expire after 10hrs, this is not configurable yet.
</blockquote>

### Start minio server

```
minio server <testdir>
```

### JSON RPC APIs.

JSON RPC namespace is `Web`.

#### Auth Operations

* Login - waits for 'username, password' and on success replies a new Json Web Token (JWT).
* ResetToken - resets token, requires password and token.
* Logout - currently a dummy operation.

#### Bucket/Object Operations.

* ListBuckets - lists buckets, requires a valid token.
* ListObjects - lists objects, requires a valid token.
* MakeBucket - make a new bucket, requires a valid token.
* GetObjectURL - generates a URL for download access, requires a valid token.
  (generated URL is valid for 1hr)
* PutObjectURL - generates a URL for upload access, requies a valid token.
  (generated URL is valid for 1hr)

#### Server Operations. 
* DiskInfo - get backend disk statistics.
