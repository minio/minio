# Bucket Access Management Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO uses Policy-Based Access Control (PBAC) to define the authorized actions and resources to which an authenticated user has access. Each policy describes one or more `actions` and `conditions` that outline the permissions of a user or group of users.

MinIO PBAC is built for compatibility with AWS IAM policy syntax, structure, and behavior.

## Prerequisites

- Install MinIO - [MinIO Quickstart Guide](https://min.io/docs/minio/linux/index.html#procedure).
- [Use `mc` with MinIO Server](https://min.io/docs/minio/linux/reference/minio-mc.html#quickstart)

## Set bucket access policy

### Policy document structure

```json
{
   "Version" : "2012-10-17",
   "Statement" : [
      {
         "Effect" : "Allow",
         "Action" : [ "s3:<ActionName>", ... ],
         "Resource" : "arn:aws:s3:::*",
         "Condition" : { ... }
      },
      {
         "Effect" : "Deny",
         "Action" : [ "s3:<ActionName>", ... ],
         "Resource" : "arn:aws:s3:::*",
         "Condition" : { ... }
      }
   ]
}
```

### Sample bucket policy document

```json
{
   "Version":"2012-10-17",
   "Id":"PutObjectPolicy",
   "Statement":[{
         "Sid":"DenyObjectsWithInvalidSSEKMS",
         "Effect":"Deny",
         "Principal":"*",
         "Action":"s3:PutObject",
         "Resource":"arn:aws:s3:::multi-key-poc/*",
         "Condition":{
            "StringNotEquals":{
               "s3:x-amz-server-side-encryption-aws-kms-key-id":"minio-default-key"
            }
         }
      }
   ]
}
```
and

```json
{
   "Version":"2012-10-17",
   "Id":"PutObjectPolicy1",
   "Statement":[{
         "Sid":"DenyObjectsThatAreNotSSEKMS",
         "Effect":"Deny",
         "Principal":"*",
         "Action":"s3:PutObject",
         "Resource":"arn:aws:s3:::multi-key-poc/*",
         "Condition":{
            "Null":{
               "s3:x-amz-server-side-encryption-aws-kms-key-id":"true"
            }
         }
      }
   ]
}
```

### Create a policy

```sh
mc admin policy create myminio bucket-without-sse-kms-pol ./sample-pol.json
```

### Verify policy details

```sh
mc admin policy info myminio bucket-without-sse-kms-pol
```

### Add new user to the MinIO instance

```sh
mc admin user add myminio user-1 passwd-1
```

### Attach the policy to the user

```sh
mc admin policy attach myminio bucket-without-sse-kms-pol --user user-1
mc admin policy attach myminio consoleAdmin --user user-1
```

### Verify user's permission for putobject calls

```sh
mc alias set myminio1 http://localhost:9000 user-1 passwd-1
mc cp /etc/hosts myminio1/multi-key-poc/hosts --enc-kms "myminio1/multi-key-poc/hosts=minio-default-key-xxx"   <<=== SHOULD FAIL
mc cp /etc/hosts myminio1/multi-key-poc/hosts --enc-kms "myminio1/multi-key-poc/hosts=minio-default-key"       <<=== SHOULD PASS
```
