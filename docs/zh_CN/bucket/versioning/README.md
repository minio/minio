# 存储桶版本控制指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO的版本控制，可以让一个存储通里的某个对象具有多个版本。例如，你可以存储`spark.csv` (版本为 `ede336f2`) 和 `spark.csv` (版本为 `fae684da`)在一个存储通中。版本控制保护你免受意外覆盖、删除、应用保留策略和存档对象的影响。

要自定义数据保留和存储使用情况，请在对象生命周期管理中使用对象版本控制。如果你在不受版本控制的存储桶中具有对象到期生命周期策略，并且希望在启用版本控制时保持相同的永久删除行为，则必须添加非当前版本到期策略。非当前版本到期生命周期策略将管理在受版本控制的存储桶中删除非当前对象版本的行为。(启用版本控制的存储桶会维护一个当前对象版本，以及零个或零个以上非当前对象版本。) 

必须在存储桶上显式启用版本控制，默认情况下不启用版本控制。启用对象锁定的存储桶会自动启用版本控制。启用和暂停版本控制是在存储桶级别完成的。

只有MinIO能生成版本ID，并且无法对其进行编辑。版本ID是`DCE 1.1 v4 UUID 4`(基于随机数据)，UUID是128位数字，旨在在空间和时间上具有很高的唯一性，并且难以通过计算猜测。它们是全局唯一的，可以在不联网一个全局注册服务器的情况下在本地生成。UUID旨在用作生命周期非常短的大量标记对象的唯一标识符，并可以可靠地标识整个网络上非常持久的对象。

当您在启用版本控制的存储桶中PUT一个对象时，非当前版本不会被覆盖。下图显示，当将新版本的`spark.csv`放入已经包含相同名称对象的存储桶中时，原始对象（ID = `ede336f2`）保留在存储桶中，MinIO生成新版本（ID = `fae684da`），并将新版本添加到存储桶中。

![put](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/versioning/versioning_PUT_versionEnabled.png)

这意味着对对象的意外覆盖或删除进行了保护，允许检索对象的先前版本。

删除对象时，所有版本都保留在存储桶中，MinIO添加删除标记，如下所示：

![delete](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/versioning/versioning_DELETE_versionEnabled.png)

现在，删除标记成为对象的当前版本。默认情况下，GET请求始终检索最新的存储版本。因此，当当前版本为删除标记时，执行简单的GET对象请求将返回`404` `The specified key does not exist`，如下所示：

![get](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/versioning/versioning_GET_versionEnabled.png)

通过指定如下所示的版本ID进行GET请求，你可以检索特定的对象版本`fae684da`。

![get_version_id](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/versioning/versioning_GET_versionEnabled_id.png)

要永久删除对象，你需要指定要删除的版本，只有具有适当权限的用户才能永久删除版本。如下所示，使用特定版本ID调用的DELETE请求从存储桶中永久删除一个对象。带版本id的DELETE请求不会添加删除标记。

![delete_version_id](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/versioning/versioning_DELETE_versionEnabled_id.png)

## 概念
- MinIO上的所有存储桶始终处于以下状态之一：无版本控制（默认），启用版本控制或暂停版本控制。
- 版本控制状态应用于启用版本控制的存储桶中的所有对象。首次启用存储桶版本控制后，将始终对存储桶中的对象进行版本控制并为其指定唯一的版本ID。
- 现存或者新建的存储通都能启用版本控制，最终也可以将其暂停。 对象的现有版本保持不变，并且仍可以使用版本ID进行访问。
- 在删除存储桶之前，应删除所有版本，包括删除标记。
- **版本控制功能仅在纠删码和分布式纠删码模式下可用**。

## 如何在存储桶上配置版本控制
创建的每个存储桶都有与其关联的版本控制配置。默认情况下，存储桶是无版本控制的，如下所示
```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
</VersioningConfiguration>
```

要启用版本控制，你可以发送一个Status为`Enabled`的版本控制配置请求到MinIO。
```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>
```

同样的，要暂停版本控制，把Status配置设置为`Suspended`即可。
```
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Suspended</Status>
</VersioningConfiguration>
```

只有具有相应的权限或者root用户才能配置任何存储桶的版本控制状态。

## 使用MinIO Java SDK启用存储通版本控制的示例

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

## 进一步探索
- [使用`minio-java` SDK](https://docs.minio.io/cn/java-client-quickstart-guide.html)
- [对象锁定指南](https://docs.minio.io/docs/minio-bucket-object-lock-guide.html)
- [MinIO管理指南](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [MinIO官方文档](https://docs.min.io/cn/)
