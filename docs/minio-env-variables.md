# Minio Environmental varaibles

#### MINIO_ENABLE_FSMETA
When enabled, minio-FS saves the HTTP headers that start with `X-Amz-Meta-` and `X-Minio-Meta`. These header meta data can be retrieved on HEAD and GET requests on the object.

#### MINIO_PROFILER
Used for Go profiling. Supported values are:

`cpu` - for CPU profiling

`mem` - for memory profiling

`block` - for block profiling

#### MINIO_PROFILE_DIR

Path where cpu/mem/block profiling files are dumped

#### MINIO_BROWSER

setting this to `off` disables the minio browser.

#### MINIO_ACCESS_KEY

Minio access key.

#### MINIO_SECRET_KEY

Minio secret key.

#### MINIO_CACHE_SIZE

Set total cache size in NN[GB|MB|KB]. Defaults to 8GB

Ex: MINIO_CACHE_SIZE=2GB

#### MINIO_CACHE_EXPIRY

Set the object cache expiration duration in NN[h|m|s]. Defaults to 72 hours.

Ex. MINIO_CACHE_EXPIRY=24h

#### MINIO_MAXCONN

Limit of the number of concurrent http requests.

Ex. MINIO_MAXCONN=500
