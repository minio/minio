## Disk based caching

Disk caching can be turned on by updating the "cache" config
settings for minio server. By default, this is at `${HOME}/.minio`.

"cache" takes the drives location, duration to expiry (in days) and any
wildcard patterns to exclude certain content from cache as 
configuration settings. 
```
"cache": {
	"drives": ["/path/drive1", "/path/drive2", "/path/drive3"],
	"expiry": 30,
	"exclude": ["*.png","bucket1/a/b","bucket2/*"]
},
```

The cache settings can also be set by the environment variables
below. When set, environment variables override any cache settings in config.json
```
export MINIO_CACHE_DRIVES="/drive1;/drive2;/drive3"
export MINIO_CACHE_EXPIRY=90
export MINIO_CACHE_EXCLUDE="pattern1;pattern2;pattern3"
```

 - Cache size is 80% of drive capacity. Disk caching requires
   Atime support to be enabled on the cache drive.

 - Expiration of entries takes user provided expiry as a hint,
   and defaults to 90 days if not provided.

 - Garbage collection sweep of the expired entries happens whenever
   disk usage is > 80% of drive capacity until sufficient disk
   space has been freed.
 - Object is cached only when drive has sufficient disk space for 100 times the size of current object

### Behavior

Disk caching happens on both GET and PUT operations.

- GET caches new objects for entries not found in cache.
  Otherwise serves from the cache.

- PUT/POST caches all successfully uploaded objects. Replaces
  existing cached entry for the same object if needed.

When an object is deleted, it is automatically cleared from the cache.

NOTE: Expiration happens automatically based on the configured
interval as explained above, frequently accessed objects stay
alive in cache for a significantly longer time on every cache hit.

The following caveats apply for offline mode
  - GET, LIST and HEAD operations will be served from the disk cache.
  - PUT operations are disallowed when gateway backend is offline.
  - Anonymous operations are not implemented as of now.