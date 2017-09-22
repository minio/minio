## Object Caching

Object caching is on by default with following settings

 - Cache size is 50% of your RAM size. Caching is disabled
   if your RAM size is smaller than 8GB.

 - Expiration of each entries happen on every 72 hours.

 - Garbage collection sweep of the expired entries happen every
   1/4th the set expiration hours value (every 18 hours).

NOTE: None of the settings can be configured manually.

### Behavior

Caching happens on both GET and PUT operations.

- GET caches new objects for entries not found in cache.
  Otherwise serves from the cache.

- PUT/POST caches all successfully uploaded objects. Replaces
  existing cached entry for the same object if needed.

NOTE: Expiration happens automatically based on the configured
interval as explained above, frequently accessed objects stay
alive in cache for a significantly longer time on every cache hit.
