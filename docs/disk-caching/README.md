# Disk Cache Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio's disk caching feature uses *caching disks* to store content closer to the tenants. For example, if an object is downloaded from a `gateway azure` configuration with disk caching enabled, the object is cached and each subsequent request on the object is served directly from the cache drives until it expires. Disk caching delivers objects with the best possible performance, and results in dramatic improvements for the time-to-first-byte on an object.

This topic describes how to get started with the disk caching feature. For more information about advanced uses and limitations, see the [design document](./minio-disk-cache-design.html).

- [Install Minio Server](#install-minio-server) 
- [Configure the Disk Cache Feature](#configure-the-disk-cache-feature) 
- [Run Minio with the Disk Cache Feature](#run-minio-with-the-disk-cache-feature) 
- [Test the Configuration](#test-the-configuration) 
- [Explore Further](#explore-further) 

## <a name="install-minio-server"></a>1. Install Minio Server

Install Minio Server using the instructions in the [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

## <a name="configure-the-disk-cache-feature"></a>2. Configure the Disk Cache Feature

Disk caching can be enabled by updating the `cache` configuration settings for Minio Server. Minio's `cache` configuration settings define the mounted drive(s) or directory paths, cache expiry duration (in days), and any wild card patterns to exclude from being cached:

```json
"cache": {
	"drives": ["/mnt/drive1", "/mnt/drive2", "/mnt/drive3"],
	"expiry": 90,
	"exclude": ["*.pdf","mybucket/*"],
	"maxuse" : 70,
},
```

Use `mc admin config get` to get the current JSON configuration file for the Minio deployment, and save it locally:

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

Update `config.json` in `/tmp/myconfig` for the cluster, and use `mc admin config set` to update the configuration for the deployment:

```sh
$ mc admin config set myminio < /tmp/myconfig
```

Restart Minio Server for the changes to take effect.

The cache settings can also be set through environment variables. The environment variables override any `cache` configuration settings for Minio Server: 

```bash
export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/cache{1...3}"
export MINIO_CACHE_EXPIRY=90
export MINIO_CACHE_EXCLUDE="*.pdf;mybucket/*"
export MINIO_CACHE_MAXUSE=80
```

The example above specifies the following properties for disk caching:
* Use `/mnt/drive1`, `/mnt/drive2` ,`/mnt/cache1` ... `/mnt/cache3` for disk caching.
* Set the expiry to 90 days.
* Exclude all objects with the `.pdf` filename extension and all objects under the bucket `mybucket`.
* Restrict the maximum usage to 80% of the disk's capacity.

## <a name="run-minio-with-the-disk-cache-feature"></a>3. Run Minio with the Disk Cache Feature

Use the following command to start Minio Server with a standalone, erasure-coded configuration:

```
minio server /export{1...24}

```

## <a name="test-the-configuration"></a>4. Test the Configuration

To test this configuration, access Minio Server via the object browser or use [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). The uploaded files should be accessible from all of the Minio endpoints.

## <a name="explore-further"></a>5. Explore Further
- [Disk cache design](https://github.com/minio/minio/blob/master/docs/disk-caching/DESIGN.md)
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
