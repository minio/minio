# Minio multi-user Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
This document explains how to add, revoke users. Multi-user as name implies means Minio supports long term users other than default credentials, each of these users can be configured to deny or allow access to buckets, resources.

## Get started
In this document we will explain in detail on how to configure multiple users.

### 1. Prerequisites
- Install mc - [Minio Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
- Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)

### 2. Create a new user and policy
Create a new user `newuser` on Minio use `mc admin users`, with a `newuser.json`.
```
mc admin users add myminio newuser newuser123 /tmp/newuser.json
```

An example user policy, enables `newuser` to download all objects in my-bucketname.
```json
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
```

### 3. Revoke user
Temporarily revoke access for `newuser`.
```
mc admin users revoke myminio newuser
```

### 4. Remove user
Remove the user `newuser`.
```
mc admin users remove myminio newuser
```
