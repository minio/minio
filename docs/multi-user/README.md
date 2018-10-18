# Minio multi-user Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
This document explains how to add, revoke users. Multi-user as name implies means Minio supports long term users other than default credentials, each of these users can be configured to deny or allow access to buckets, resources.

## Get started
In this document we will explain in detail on how to configure multiple users.

### 1. Prerequisites
- Install mc - [Minio Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
- Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)

### 2. Create a new user and policy
Create new canned policy `getonly` with `newuser.json` use `mc admin policies`. This policy enables users to download all objects in my-bucketname.
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

mc admin policies add myminio getonly getonly.json
```

Create a new user `newuser` on Minio use `mc admin users`, additionally specify `getonly` canned policy for this `newuser`.
```
mc admin users add myminio newuser newuser123 getonly
```

### 3. Disable user
Disable user `newuser`.
```
mc admin users disable myminio newuser
```

### 4. Remove user
Remove the user `newuser`.
```
mc admin users remove myminio newuser
```

### 5. List all users
List all enabled and disabled users.
```
mc admin users list myminio
```

## Explore Further
- [Minio STS Quickstart Guide](https://docs.minio.io/docs/minio-sts-quickstart-guide)
- [Minio Admin Complete Guide](https://docs.minio.io/docs/minio-admin-complete-guide.html)
- [The Minio documentation website](https://docs.minio.io)
