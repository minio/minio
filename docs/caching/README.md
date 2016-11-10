## Object caching

Object caching by turned on by default with following settings

 - Default cache size 8GB. Cache size also automatically picks
   a lower value if your local memory size is lower than 8GB.

 - Default expiration of entries happensat 72 hours,
   this option cannot be changed.

 - Default expiry interval is 1/4th of the expiration hours, so
   expiration sweep happens across the cache every 1/4th the time
   duration of the set entry expiration duration.

### Behavior

Caching happens on both GET and PUT operations.

- GET caches new objects for entries not found in cache.

- PUT/POST caches all successfully uploaded objects.

In all other cases if objects are served from cache.

NOTE:

Cache is always populated upon object is successfully
read from the disk.

Expiration happens automatically based on the configured
interval as explained above, frequently accessed objects
stay alive for significantly longer time due to the fact
that expiration time is reset for every cache hit.
