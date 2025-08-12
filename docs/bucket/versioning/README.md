# Bucket Versioning Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO versioning is designed to keep multiple versions of an object in one bucket. For example, you could store `spark.csv` (version `ede336f2`) and `spark.csv` (version `fae684da`) in a single bucket. Versioning protects you from unintended overwrites, deletions, protect objects with retention policies.

To control data retention and storage usage, use object versioning with [object lifecycle management](https://github.com/minio/minio/blob/master/docs/bucket/lifecycle/README.md).  If you have an object expiration lifecycle policy in your non-versioned bucket and you want to maintain the same permanent delete behavior when on versioning-enabled bucket, you must add a noncurrent expiration policy. The noncurrent expiration lifecycle policy will manage the deletes of the noncurrent object versions in the versioning-enabled bucket. (A version-enabled bucket maintains one current and zero or more noncurrent object versions.)

Versioning must be explicitly enabled on a bucket, versioning is not enabled by default. Object locking enabled buckets have versioning enabled automatically. Enabling and suspending versioning is done at the bucket level.

Only MinIO generates version IDs, and they can't be edited. Version IDs are simply of `DCE 1.1 v4 UUID 4` (random data based), UUIDs are 128 bit numbers which are intended to have a high likelihood of uniqueness over space and time and are computationally difficult to guess. They are globally unique identifiers which can be locally generated without contacting a global registration authority. UUIDs are intended as unique identifiers for both mass tagging objects with an extremely short lifetime and to reliably identifying very persistent objects across a network.

When you PUT an object in a versioning-enabled bucket, the noncurrent version is not overwritten. The following figure shows that when a new version of `spark.csv` is PUT into a bucket that already contains an object with the same name, the original object (ID = `ede336f2`) remains in the bucket, MinIO generates a new version (ID = `fae684da`), and adds the newer version to the bucket.

![put](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/versioning/versioning_PUT_versionEnabled.png)

This protects against accidental overwrites or deletes of objects, allows previous versions to be retrieved.

When you DELETE an object, all versions remain in the bucket and MinIO adds a delete marker, as shown below:

![delete](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/versioning/versioning_DELETE_versionEnabled.png)

Now the delete marker becomes the current version of the object. GET requests by default always retrieve the latest stored version. So performing a simple GET object request when the current version is a delete marker would return `404` `The specified key does not exist` as shown below:

![get](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/versioning/versioning_GET_versionEnabled.png)

GET requests by specifying a version ID as shown below, you can retrieve the specific object version `fae684da`.

![get_version_id](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/versioning/versioning_GET_versionEnabled_id.png)

To permanently delete an object you need to specify the version you want to delete, only the user with appropriate permissions can permanently delete a version.  As shown below DELETE request called with a specific version id permanently deletes an object from a bucket. Delete marker is not added for DELETE requests with version id.

![delete_version_id](https://raw.githubusercontent.com/minio/minio/master/docs/bucket/versioning/versioning_DELETE_versionEnabled_id.png)

## Concepts

- All Buckets on MinIO are always in one of the following states: unversioned (the default) and all other existing deployments, versioning-enabled, or versioning-suspended.
- Versioning state applies to all of the objects in the versioning enabled bucket. The first time you enable a bucket for versioning, objects in the bucket are thereafter always versioned and given a unique version ID.
- Existing or newer buckets can be created with versioning enabled and eventually can be suspended as well. Existing versions of objects stay as is and can still be accessed using the version ID.
- All versions, including delete-markers should be deleted before deleting a bucket.
- **Versioning feature is only available in erasure coded and distributed erasure coded setups**.

## How to configure versioning on a bucket

Each bucket created has a versioning configuration associated with it. By default bucket is unversioned as shown below

```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
</VersioningConfiguration>
```

To enable versioning, you send a request to MinIO with a versioning configuration with Status set to `Enabled`.

```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>
```

Similarly to suspend versioning set the configuration with Status set to `Suspended`.

```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Suspended</Status>
</VersioningConfiguration>
```

## MinIO extension to Bucket Versioning

### Idempotent versions on directory objects

All directory objects such as objects that end with `/`, will only have one versionId (i.e `null`). A delete marker will never be created on these directory objects, instead a DELETE will delete the directory objects. This is done to ensure that directory objects even with multiple overwrites - do not ever need multiple versions in the first place. All overwrite calls on these directory objects are idempotent.

> NOTE: Server side replication is supported for idempotent versions on directory objects.

### Idempotent versions on delete markers

Duplicate delete markers are not created on MinIO buckets with versioning, if an application performs a soft delete on an object repeatedly - that object will only ever have a single DELETE marker for all such successive attempts. This is done to ensure that repeated soft deletes do not ever need multiple versions in the first place.

> NOTE: Server side replication is supported for idempotent versions on delete marked objects.

### Motivation

**PLEASE READ: This feature is meant for advanced use-cases only where the setup is using bucket versioning or with replicated buckets, use this feature to optimize versioning behavior for some specific applications. MinIO experts will evaluate and guide on the benefits for your application, please reach out to us on <https://subnet.min.io>.**

Spark/Hadoop workloads which use Hadoop MR Committer v1/v2 algorithm upload objects to a temporary prefix in a bucket. These objects are 'renamed' to a different prefix on Job commit. Object storage admins are forced to configure separate ILM policies to expire these objects and their versions to reclaim space.

### Solution

To exclude objects under a list of prefix (glob) patterns from being versioned, you can send the following versioning configuration with Status set to `Enabled`.

```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Status>Enabled</Status>
        <ExcludedPrefixes>
          <Prefix>*/_temporary</Prefix>
        </ExcludedPrefixes>
        <ExcludedPrefixes>
          <Prefix>*/__magic</Prefix>
        </ExcludedPrefixes>
        <ExcludedPrefixes>
          <Prefix>*/_staging</Prefix>
        </ExcludedPrefixes>

        <!-- .. up to 10 prefixes in all -->
</VersioningConfiguration>
```

### Features

- Objects matching these prefixes will behave as though versioning were suspended. These objects **will not** be replicated if bucket has replication configured.
- Objects matching these prefixes will also not leave `null` delete markers, dramatically reduces namespace pollution while keeping the benefits of replication.
- Users with explicit permissions or the root credential can configure the versioning state of any bucket.

## Examples of enabling bucket versioning using MinIO Java SDK

### EnableVersioning() API

```
import io.minio.EnableVersioningArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class EnableVersioning {
  /** MinioClient.enableVersioning() example. */
  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    try {
      /* play.min.io for test and development. */
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint("https://play.min.io")
              .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
              .build();

      /* Amazon S3: */
      // MinioClient minioClient =
      //     MinioClient.builder()
      //         .endpoint("https://s3.amazonaws.com")
      //         .credentials("YOUR-ACCESSKEY", "YOUR-SECRETACCESSKEY")
      //         .build();

      // Enable versioning on 'my-bucketname'.
      minioClient.enableVersioning(EnableVersioningArgs.builder().bucket("my-bucketname").build());

      System.out.println("Bucket versioning is enabled successfully");

    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
```

### isVersioningEnabled() API

```
public class IsVersioningEnabled {
  /** MinioClient.isVersioningEnabled() example. */
  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    try {
      /* play.min.io for test and development. */
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint("https://play.min.io")
              .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
              .build();

      /* Amazon S3: */
      // MinioClient minioClient =
      //     MinioClient.builder()
      //         .endpoint("https://s3.amazonaws.com")
      //         .credentials("YOUR-ACCESSKEY", "YOUR-SECRETACCESSKEY")
      //         .build();

      // Create bucket 'my-bucketname' if it doesn`t exist.
      if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("my-bucketname").build())) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket("my-bucketname").build());
        System.out.println("my-bucketname is created successfully");
      }

      boolean isVersioningEnabled =
          minioClient.isVersioningEnabled(
              IsVersioningEnabledArgs.builder().bucket("my-bucketname").build());
      if (isVersioningEnabled) {
        System.out.println("Bucket versioning is enabled");
      } else {
        System.out.println("Bucket versioning is disabled");
      }
      // Enable versioning on 'my-bucketname'.
      minioClient.enableVersioning(EnableVersioningArgs.builder().bucket("my-bucketname").build());
      System.out.println("Bucket versioning is enabled successfully");

      isVersioningEnabled =
          minioClient.isVersioningEnabled(
              IsVersioningEnabledArgs.builder().bucket("my-bucketname").build());
      if (isVersioningEnabled) {
        System.out.println("Bucket versioning is enabled");
      } else {
        System.out.println("Bucket versioning is disabled");
      }

    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
```

## Explore Further

- [Use `minio-java` SDK with MinIO Server](https://docs.min.io/community/minio-object-store/developers/java/minio-java.html)
- [Object Lock and Immutability Guide](https://docs.min.io/community/minio-object-store/administration/object-management/object-retention.html)
- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
