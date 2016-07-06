```
PACKAGE DOCUMENTATION

package objcache
    import "github.com/minio/minio/pkg/objcache"

    Package objcache implements in memory caching methods.

VARIABLES

var DefaultExpiry = time.Duration(72 * time.Hour) // 72hrs.

    DefaultExpiry represents default time duration value when individual
    entries will be expired.

var ErrCacheFull = errors.New("Not enough space in cache")
    ErrCacheFull - cache is full.

var ErrKeyNotFoundInCache = errors.New("Key not found in cache")
    ErrKeyNotFoundInCache - key not found in cache.

var NoExpiry = time.Duration(0)
    NoExpiry represents caches to be permanent and can only be deleted.

TYPES

type Cache struct {

    // OnEviction - callback function for eviction
    OnEviction func(key string)
    // contains filtered or unexported fields
}
    Cache holds the required variables to compose an in memory cache system
    which also provides expiring key mechanism and also maxSize.

func New(maxSize uint64, expiry time.Duration) *Cache
    New - Return a new cache with a given default expiry duration. If the
    expiry duration is less than one (or NoExpiry), the items in the cache
    never expire (by default), and must be deleted manually.

func (c *Cache) Create(key string, size int64) (w io.WriteCloser, err error)
    Create - validates if object size fits with in cache size limit and
    returns a io.WriteCloser to which object contents can be written and
    finally Close()'d. During Close() we checks if the amount of data
    written is equal to the size of the object, in which case it saves the
    contents to object cache.

func (c *Cache) Delete(key string)
    Delete - delete deletes an entry from the cache.

func (c *Cache) DeleteExpired()
    DeleteExpired - deletes all the expired entries from the cache.

func (c *Cache) Open(key string) (io.ReadSeeker, error)
    Open - open the in-memory file, returns an in memory read seeker.
    returns an error ErrNotFoundInCache, if the key does not exist.

func (c *Cache) StopExpiry()
    StopExpiry sends a message to the expiry routine to stop expiring cached
    entries. NOTE: once this is called, cached entries will not be expired
    if the consume has called this.
```
