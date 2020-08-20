# 存储桶复制指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Bucket replication is designed to replicate selected objects in a bucket to a destination bucket.
存储桶复制功能可以把存储桶中选中的对象复制到目标存储桶。

要想复制一个存储桶上的对象,到同一集群或者不同集群的目标站点上的目标存储桶中，首先要为源存储桶和目标存储桶启用[版本控制功能](https://docs.minio.io/docs/minio-bucket-versioning-guide.html)。最后，需要在源MinIO服务器上配置目标站点和目标存储桶。

##  强调
- 和AWS S3不同，MinIO的源存储桶和目标存储桶名字可以一样，可处理各种情况，例如*Splunk*，*Veeam*站点到站点*DR*。
- 与AWS S3不同，MinIO纯天然支持跨源桶和目标桶的对象锁定保留。
- 比[AWS S3 Bucket Replication Config](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html)实现更简单，像IAM Role, AccessControlTranslation, Metrics and SourceSelectionCriteria这些功能在MinIO中不需要.

## 如何使用?
如下所示在源群集上创建复制目标：

```
mc admin bucket remote add myminio/srcbucket https://accessKey:secretKey@replica-endpoint:9000/destbucket --service replication --region us-east-1
Role ARN = 'arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket'
```

请注意，admin需要具有源集群上的*s3:GetReplicationConfigurationAction*权限。在目标上使用的凭据需要具有*s3:ReplicateObject*权限. 成功创建并授权后，将生成复制目标ARN。下面的命令列出了所有当前授权的复制目标：:

```
mc admin bucket remote ls myminio/srcbucket --service "replication"
Role ARN = 'arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket'
```

现在可以使用带有复制配置JSON文件，把复制配置添加到源存储桶。上面的角色ARN作为配置中的json元素传入。

```json
{
  "Role" :"arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket",
  "Rules": [
    {
      "Status": "Enabled",
      "Priority": 1,
      "DeleteMarkerReplication": { "Status": "Disabled" },
      "Filter" : {
        "And": {
            "Prefix": "Tax",
            "Tags": [
                {
                "Key": "Year",
                "Value": "2019"
                },
                {
                "Key": "Company",
                "Value": "AcmeCorp"
                }
            ]
        }
      },
      "Destination": {
        "Bucket": "arn:aws:s3:::destbucket",
        "StorageClass": "STANDARD"
      }
    }
  ]
}
```

```
mc replicate add myminio/srcbucket/Tax --priority 1 --arn "arn:minio:replication:us-east-1:c5be6b16-769d-432a-9ef1-4567081f3566:destbucket" --tags "Year=2019&Company=AcmeCorp" --storage-class "STANDARD"
Replication configuration applied successfully to myminio/srcbucket.
```

复制配置遵循[AWS S3规范](https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html). 现在，上传到源存储桶中符合复制条件的任何对象都将被MinIO服务器自动复制到远程目标存储桶中。通过禁用配置中的特定规则或者删除复制配置，都可以随时禁用复制。


按照S3规范，当从源存储桶中删除一个对象后，副本不会被删除。

![delete](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/replication/DELETE_bucket_replication.png)

当对象锁定与复制结合使用时，源桶和目标桶都需要启用对象锁定。同理，如果目标也支持加密，则服务器端将复制加密的对象。

复制状态可以在源和目标对象的元数据中看到。在源端，根据复制的结果是成功还是失败，`X-Amz-Replication-Status`会从`PENDING`变更为`COMPLETE`或者 `FAILED`状态。 在目标端，对象成功复制，`X-Amz-Replication-Status`会被设置为`REPLICA`状态。在定期的磁盘扫描周期中，任何复制失败都将自动重新尝试。

![put](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/replication/PUT_bucket_replication.png)

![head](https://raw.githubusercontent.com/minio/minio/master/docs/zh_CN/bucket/replication/HEAD_bucket_replication.png)

## 进一步探索
- [MinIO存储桶版本控制实现](https://docs.minio.io/docs/minio-bucket-versioning-guide.html)
- [MinIO客户端快速入门指南](https://docs.minio.io/cn/minio-client-quickstart-guide.html)
