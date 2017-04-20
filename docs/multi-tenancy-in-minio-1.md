# Multi-tenancy in minio. [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio)


## Multi-tenancy for applications.
### Presigned URLs
Presigned URLs are a way to give users temporary access to minio storage to upload/download objects directly from their clients (browser/mobile-app). Using presigned-urls you don't have to share minio secret-key with the client software and still provide strict access control to the private storage.

Note that presigned-URLs have to be generated on the server side and then passed to clients.

#### Download to browser
Web browsers can download objects directly from Minio using [Presigned-GET](http://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURL.html). Minio client libraries provide a simple API to generate Presigned-GET URLs.

If you are using nodejs for the server stack you can refer to [presignedGetObject()](https://docs.minio.io/docs/java-client-api-reference#presignedGetObject).

#### Upload from browser
Web browsers can upload objects directly to Minio using [Presigned-PUT](http://docs.aws.amazon.com/AmazonS3/latest/dev/PresignedUrlUploadObject.html) and [Presigned-POST](http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html). Both presigned-PUT and presigned-POST serve the same purpose of uploading objects but POST method provides more control on the size/content-type of the object being uploaded.

If you are using nodejs for the server stack you can refer to [presignedPutObject()](https://docs.minio.io/docs/javascript-client-api-reference#presignedPutObject) and [presignedPostPolicy()](https://docs.minio.io/docs/javascript-client-api-reference#presignedPostPolicy).


## Multi-tenancy for non-applications.
If your object storage requirement is such that each user should have his/her own credentials (access/secret keys) you can deploy one minio instance per user.

### Running multiple minio instances
You can run multiple minio instances (one per user/tenant) on different ports. In the example below, we run 3 minio instances on ports 9001, 9002 and 9002. Credentials of each instance is different as each minio instance uses a different config directory.

*Run Minio-1*:
```
minio --config-dir /data/conf1 server --address :9001 /data/export1
```

*Run Minio-2*:
```
minio --config-dir /data/conf2 server --address :9002 /data/export2
```

*Run Minio-3*:
```
minio --config-dir /data/conf3 server --address :9003 /data/export3
```

### Run minio instances behind a reverse proxy
You can also run multiple minio instances behind a reverse proxy such that the proxy routes the client requests to the right minio instance based on the request's access-key.

In the example config below, the proxy runs on port 80 and reverse proxies the requests to Minio-1 or Minio-2 or Minio-3 depending on the access-key in the client request.

Let's assume:
* Minio-1's access key is ROML2P775VPAT7RLPOWU
* Minio-2's access key is ENT3GYJCKCD1Q79XLP4C
* Minio-3's access key is C988WQ23D98207ELOLPW.

#### Using Nginx
Run nginx with the following config in `/etc/nginx/sites-enabled`:
```
server {
    listen       80;
	location / {
	    proxy_set_header Host $http_host;
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=ROML2P775VPAT7RLPOWU") {
           # proxy the request to Minio-1
	       proxy_pass http://localhost:9001;
	    }
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=ENT3GYJCKCD1Q79XLP4C") {
           # proxy the request to Minio-2
	       proxy_pass http://localhost:9002;
	    }
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=C988WQ23D98207ELOLPW") {
           # proxy the request to Minio-3
	       proxy_pass http://localhost:9003;
	    }
    }
}
```

#### Using Traefik
Traefik can be downloaded from https://traefik.io/

traefik.toml config file:

```
defaultEntryPoints = ["http"]

[entryPoints]
  [entryPoints.http]
  address = ":80"

[file]
watch = true

[backends]
  [backends.backend1]
    [backends.backend1.servers.server1]
      url = "http://localhost:9001"

  [backends.backend2]
    [backends.backend2.servers.server1]
      url = "http://localhost:9002"

  [backends.backend3]
    [backends.backend3.servers.server1]
      url = "http://localhost:9003"

[frontends]
    [frontends.frontend1]
    backend = "backend1"
    passHostHeader = true
    [frontends.frontend1.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=ROML2P775VPAT7RLPOWU"
    
    [frontends.frontend2]
    backend = "backend2"
    passHostHeader = true
    [frontends.frontend2.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=ENT3GYJCKCD1Q79XLP4C"

    [frontends.frontend3]
    backend = "backend3"
    passHostHeader = true
    [frontends.frontend3.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=C988WQ23D98207ELOLPW"

```

