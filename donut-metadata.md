##### Users Collection

```js

"minio": {
    "version": 1,
    "users": [{
    	"secret-key": string,
    	"access-key": string,
    	"status": string // enum: ok, disabled, deleted
    }],
    "hosts": [{
    	"address": string,
    	"uuid": string,
    	"status": string, // enum: ok, disabled, deleted, busy, offline,
    	"disks": [{
        		"disk": string,
        		"uuid": string,
        		"status": string // ok, offline, disabled, busy
        }]
    }]
}
```

##### Bucket Collection

```js
"buckets": {
    "bucket": string, // index
    "permissions": string,
    "deleted": bool
}
```

##### Object Collection

```js
"objects": {
    "key": string, // index
    "createdAt": Date,
    "hosts[16]": [{
    	"host": string,
    	"disk": string,
    }]
    "deleted": bool
}
```

```js
"meta": {
    "key": string, // index
    "type": string // content-type
    // type speific meta
}
```
