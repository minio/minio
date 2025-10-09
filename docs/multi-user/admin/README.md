# MinIO Admin Multi-user Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO supports multiple admin users in addition to default operator credential created during server startup. New admins can be added after server starts up, and server can be configured to deny or allow access to different admin operations for these users. This document explains how to add/remove admin users and modify their access rights.

## Get started

In this document we will explain in detail on how to configure admin users.

### 1. Prerequisites

- Install mc - [MinIO Client Quickstart Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart)
- Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html)

### 2. Create a new admin user with CreateUser, DeleteUser and ConfigUpdate permissions

Use [`mc admin policy`](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin/mc-admin-policy.html#command-mc.admin.policy) to create custom admin policies.

Create new canned policy file `adminManageUser.json`. This policy enables admin user to
manage other users.

```json
cat > adminManageUser.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "admin:CreateUser",
        "admin:DeleteUser",
        "admin:ConfigUpdate"
      ],
      "Effect": "Allow",
      "Sid": ""
    },
    {
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::*"
      ],
      "Sid": ""
    }
  ]
}
EOF
```

Create new canned policy by name `userManager` using `userManager.json` policy file.

```
mc admin policy attach myminio userManager adminManageUser.json
```

Create a new admin user `admin1` on MinIO use `mc admin user`.

```
mc admin user add myminio admin1 admin123
```

Once the user is successfully created you can now apply the `userManage` policy for this user.

```
mc admin policy attach myminio userManager --user=admin1
```

This admin user will then be allowed to perform create/delete user operations via `mc admin user`

### 3. Configure `mc` and create another user user1 with attached policy user1policy

```
mc alias set myminio-admin1 http://localhost:9000 admin1 admin123 --api s3v4

mc admin user add myminio-admin1 user1 user123
mc admin policy attach myminio-admin1 user1policy ~/user1policy.json
mc admin policy attach myminio-admin1 user1policy --user=user1
```

### 4. List of permissions defined for admin operations

#### Config management permissions

- admin:ConfigUpdate

#### User management permissions

- admin:CreateUser
- admin:DeleteUser
- admin:ListUsers
- admin:EnableUser
- admin:DisableUser
- admin:GetUser

#### Service management permissions

- admin:ServerInfo
- admin:ServerUpdate
- admin:StorageInfo
- admin:DataUsageInfo
- admin:TopLocks
- admin:OBDInfo
- admin:Profiling,
- admin:ServerTrace
- admin:ConsoleLog
- admin:KMSKeyStatus
- admin:KMSCreateKey
- admin:ServiceRestart
- admin:ServiceStop
- admin:Prometheus
- admin:ForceUnlock
- admin:TopLocksInfo
- admin:BandwidthMonitor

#### User/Group management permissions

- admin:AddUserToGroup
- admin:RemoveUserFromGroup
- admin:GetGroup
- admin:ListGroups
- admin:EnableGroup
- admin:DisableGroup

#### Policy management permissions

- admin:CreatePolicy
- admin:DeletePolicy
- admin:GetPolicy
- admin:AttachUserOrGroupPolicy
- admin:ListUserPolicies

#### Heal management permissions

- admin:Heal

#### Service account management permissions

- admin:CreateServiceAccount
- admin:UpdateServiceAccount
- admin:RemoveServiceAccount
- admin:ListServiceAccounts

#### Bucket quota management permissions

- admin:SetBucketQuota
- admin:GetBucketQuota

#### Bucket target management permissions

- admin:SetBucketTarget
- admin:GetBucketTarget

#### Remote tier management permissions

- admin:SetTier
- admin:ListTier

#### Give full admin permissions

- admin:*

### 5. Using an external IDP for admin users

Admin users can also be externally managed by an IDP by configuring admin policy with
special permissions listed above. Follow [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html) to manage users with an IDP.

## Explore Further

- [MinIO Client Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc.html)
- [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html)
- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
