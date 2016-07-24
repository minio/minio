## Object caching

Object caching by turned on by default with following settings

  - Default cache size 8GB, can be changed from environment variable
    ``MINIO_CACHE_SIZE`` supports both SI and ISO IEC standard forms
    for input size parameters.

  - Default expiration of entries is 72 hours, can be changed from
    environment variable ``MINIO_CACHE_EXPIRY`` supportings Go
    ``time.Duration`` with valid units  "ns", "us" (or "µs"),
    "ms", "s", "m", "h".

  - Default expiry interval is 1/4th of the expiration hours, so
    expiration sweep happens across the cache every 1/4th the time
    duration of the set entry expiration duration.

### Tricks

Setting MINIO_CACHE_SIZE=0 will turn off caching entirely.
Setting MINIO_CACHE_EXPIRY=0s will turn off cache garbage collections,
all cached objects will never expire.

### Behavior

Caching happens for both GET and PUT.

- GET caches new objects for entries not found in cache,
otherwise serves from the cache.

- PUT/POST caches all successfully uploaded objects.

NOTE: Cache is not populated if there are any errors
      while reading from the disk.

Expiration happens automatically based on the configured
interval as explained above, frequently accessed objects
stay alive for significantly longer time due to the fact
that expiration time is reset for every cache hit.
