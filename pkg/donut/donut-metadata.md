##### Users Collection

```js

"minio": {
    "version": 1,
    "users": [{
    	"secretAccessKey": String,
    	"accessKeyId": String,
    	"status": String // enum: ok, disabled, deleted
    }],
    "hosts": [{
    	"address": String,
    	"uuid": String,
    	"status": String, // enum: ok, disabled, deleted, busy, offline.
    	"disks": [{
        		"disk": String,
        		"uuid": String,
        		"status": String // ok, offline, disabled, busy.
        }]
    }]
}
```

##### Bucket Collection

```js
"buckets": {
    "bucket": String, // index
    "deleted": Boolean,
    "permissions": String
}
```

##### Object Collection

```js
"objects": {
    "key": String, // index
    "createdAt": Date,
    "hosts[16]": [{
    	"host": String,
    	"disk": String,
    }],
    "deleted": Boolean
}
```

```js
"meta": {
    "key": String, // index
    "type": String // content-type
    // type speific meta
}
```
