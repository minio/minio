# MinIO Multi-user Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
MinIO supports multiple long term users in addition to default user created during server startup. New users can be added after server starts up, and server can be configured to deny or allow access to buckets and resources to each of these users. This document explains how to add/remove users and modify their access rights.


## Get started
In this document we will explain in detail on how to configure multiple users.

### 1. Prerequisites
- Install mc - [MinIO Client Quickstart Guide](https://docs.min.io/docs/minio-client-quickstart-guide.html)
- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide)
- Configure etcd (optional needed only in gateway or federation mode) - [Etcd V3 Quickstart Guide](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### 2. Create a new user with canned policy
Use [`mc admin policy`](https://docs.min.io/docs/minio-admin-complete-guide.html#policies) to create canned policies. Server provides a default set of canned policies namely `writeonly`, `readonly` and `readwrite` *(these policies apply to all resources on the server)*. These can be overridden by custom policies using `mc admin policy` command.

Create new canned policy file `getonly.json`. This policy enables users to download all objects under `my-bucketname`.
```json
cat > getonly.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::my-bucketname/*"
      ],
      "Sid": ""
    }
  ]
}
EOF
```

Create new canned policy by name `getonly` using `getonly.json` policy file.
```
mc admin policy add myminio getonly getonly.json
```

Create a new user `newuser` on MinIO use `mc admin user`, specify `getonly` canned policy for this `newuser`.
```
mc admin user add myminio newuser newuser123 getonly
```

### 3. Disable user
Disable user `newuser`.
```
mc admin user disable myminio newuser
```

### 4. Remove user
Remove the user `newuser`.
```
mc admin user remove myminio newuser
```

### 5. Change user policy
Change the policy for user `newuser` to `putonly` canned policy.
```
mc admin user policy myminio newuser putonly
```

### 5. List all users
List all enabled and disabled users.
```
mc admin user list myminio
```

### 6. Configure `mc`
```
mc config host add myminio-newuser http://localhost:9000 newuser newuser123 --api s3v4
mc cat myminio-newuser/my-bucketname/my-objectname
```

## Explore Further
- [MinIO Client Complete Guide](https://docs.min.io/docs/minio-client-complete-guide)
- [MinIO STS Quickstart Guide](https://docs.min.io/docs/minio-sts-quickstart-guide)
- [MinIO Admin Complete Guide](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [The MinIO documentation website](https://docs.min.io)
