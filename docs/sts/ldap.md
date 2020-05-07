# MinIO AD/LDAP Integration [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

**Table of Contents**

- [Introduction](#introduction)
- [Configuring AD/LDAP on MinIO](#configuring-adldap-on-minio)
    - [Variable substitution in AD/LDAP configuration strings](#variable-substitution-in-adldap-configuration-strings)
    - [Notes on configuring with Microsoft Active Directory (AD)](#notes-on-configuring-with-microsoft-active-directory-ad)
- [Managing User/Group Access Policy](#managing-usergroup-access-policy)
- [API Request Parameters](#api-request-parameters)
    - [LDAPUsername](#ldapusername)
    - [LDAPPassword](#ldappassword)
    - [Version](#version)
    - [Policy](#policy)
    - [Response Elements](#response-elements)
    - [Errors](#errors)
- [Sample `POST` Request](#sample-post-request)
- [Sample Response](#sample-response)
- [Testing](#testing)


## Introduction

MinIO provides a custom STS API that allows integration with LDAP based corporate environments. The flow is as follows:

1. User provides their AD/LDAP username and password to the STS API.
2. MinIO logs-in to the AD/LDAP server as the user - if the login succeeds the user is authenticated.
3. MinIO then queries the AD/LDAP server for a list of groups that the user is a member of.
   - This is done via a customizable AD/LDAP search query.
4. MinIO then generates temporary credentials for the user storing the list of groups in a cryptographically secure session token. The temporary access key, secret key and session token are returned to the user.
5. The user can now use these credentials to make requests to the MinIO server.

The administrator will associate IAM access policies with each group and if required with the user too. The MinIO server then evaluates applicable policies on a user (these are the policies associated with the groups along with the policy on the user if any) to check if the request should be allowed or denied.

## Configuring AD/LDAP on MinIO

LDAP configuration is designed to be simple for the MinIO administrator. The full path of a user DN (Distinguished Name) (e.g. `uid=johnwick,cn=users,cn=accounts,dc=minio,dc=io`) is configured as a format string in the **MINIO_IDENTITY_LDAP_USERNAME_FORMAT** environment variable. This allows an AD/LDAP user to not specify this whole string in the AD/LDAP STS API. Instead the user only needs to specify the username portion (i.e. `johnwick` in this example) that will be substituted into the format string configured on the server.

MinIO can be configured to find the groups of a user from AD/LDAP by specifying the **MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER** and **MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE** environment variables. When a user logs in via the STS API, the MinIO server queries the AD/LDAP server with the given search filter and extracts the given attribute from the search results. These values represent the groups that the user is a member of. On each access MinIO applies the IAM policies attached to these groups in MinIO.

MinIO sends LDAP credentials to LDAP server for validation. So we _strongly recommend_ to use MinIO with AD/LDAP server over TLS or StartTLS _only_. Using plain-text connection between MinIO and LDAP server means _credentials can be compromised_ by anyone listening to network traffic.

LDAP is configured via the following environment variables:

```
$ mc admin config set myminio/ identity_ldap  --env
KEY:
identity_ldap  enable LDAP SSO support

ARGS:
MINIO_IDENTITY_LDAP_SERVER_ADDR*             (address)   AD/LDAP server address e.g. "myldapserver.com:636"
MINIO_IDENTITY_LDAP_USERNAME_FORMAT*         (list)      ";" separated list of username bind DNs e.g. "uid=%s,cn=accounts,dc=myldapserver,dc=com"
MINIO_IDENTITY_LDAP_USERNAME_SEARCH_FILTER*  (string)    user search filter, for example "(cn=%s)" or "(sAMAccountName=%s)" or "(uid=%s)"
MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER*     (string)    search filter for groups e.g. "(&(objectclass=groupOfNames)(memberUid=%s))"
MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN*    (list)      ";" separated list of group search base DNs e.g. "dc=myldapserver,dc=com"
MINIO_IDENTITY_LDAP_USERNAME_SEARCH_BASE_DN  (list)      ";" separated list of username search DNs
MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE     (string)    search attribute for group name e.g. "cn"
MINIO_IDENTITY_LDAP_STS_EXPIRY               (duration)  temporary credentials validity duration in s,m,h,d. Default is "1h"
MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY          (on|off)    trust server TLS without verification, defaults to "off" (verify)
MINIO_IDENTITY_LDAP_SERVER_STARTTLS          (on|off)    use StartTLS instead of TLS
MINIO_IDENTITY_LDAP_SERVER_INSECURE          (on|off)    allow plain text connection to AD/LDAP server, defaults to "off"
MINIO_IDENTITY_LDAP_COMMENT                  (sentence)  optionally add a comment to this setting
```

MinIO sends LDAP credentials to LDAP server for validation. So we _strongly recommend_ to use MinIO with AD/LDAP server over TLS or StartTLS _only_. Using plain-text connection between MinIO and LDAP server means _credentials can be compromised_ by anyone listening to network traffic.

If a self-signed certificate is being used, the certificate can be added to MinIO's certificates directory, so it can be trusted by the server. An example setup for development or experimentation:

```shell
export MINIO_IDENTITY_LDAP_SERVER_ADDR=myldapserver.com:636
export MINIO_IDENTITY_LDAP_USERNAME_FORMAT="uid=%s,cn=accounts,dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN="dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER="(&(objectclass=groupOfNames)(memberUid=%s)$)"
export MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE=cn
export MINIO_IDENTITY_LDAP_STS_EXPIRY=60h
export MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY=on
```

### Variable substitution in AD/LDAP configuration strings
`%s` is replaced with *username* automatically for construction bind_dn, search_filter and group_search_filter.

### Notes on configuring with Microsoft Active Directory (AD)

The LDAP STS API also works with Microsoft AD and can be configured as above. The following are some notes on determining the values of the configuration parameters described above.

Once LDAP over TLS is enabled on AD, test access to LDAP works by running a sample search query with the `ldapsearch` utility from [OpenLDAP](https://openldap.org/):

```shell
$ ldapsearch -H ldaps://my.ldap-active-dir-server.com -D "username@minioad.local" -x -w 'secretpassword' -b "dc=minioad,dc=local"
...

# John, Users, minioad.local
dn: CN=John,CN=Users,DC=minioad,DC=local
...

# hpc, Users, minioad.local
dn: CN=hpc,CN=Users,DC=minioad,DC=local
objectClass: top
objectClass: group
cn: hpc
...
member: CN=John,CN=Users,DC=minioad,DC=local
...
```

The lines with "..." represent skipped content not shown here from brevity. Based on the output above, we see that the username format variable looks like `cn=%s,cn=users,dc=minioad,dc=local`.

The group search filter looks like `(&(objectclass=group)(member=%s))` and the group name attribute is clearly `cn`.

Thus the key configuration parameters look like:

```
MINIO_IDENTITY_LDAP_SERVER_ADDR='my.ldap-active-dir-server.com:636'
MINIO_IDENTITY_LDAP_USERNAME_FORMAT='cn=%s,ou=Users,ou=BUS1,ou=LOB,dc=somedomain,dc=com'
MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN='dc=minioad,dc=local'
MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER='(&(objectclass=group)(member=%s))'
MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE='cn'
MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY=on
```

## Managing User/Group Access Policy

Access policies may be configured on a group or on a user directly. Access policies are first defined on the MinIO server using IAM policy JSON syntax. The `mc` tool is used to issue the necessary commands.

**Note that by default no policy is set on a user**. Thus even if they successfully authenticate with AD/LDAP credentials, they have no access to object storage as the default access policy is to deny all access.

To define a new policy, you can use the [AWS policy generator](https://awspolicygen.s3.amazonaws.com/policygen.html). Copy the policy into a text file `mypolicy.json` and issue the command like so:

```sh
mc admin policy add myminio mypolicy mypolicy.json
```

To assign the policy to a user or group, use:

```sh
mc admin policy set myminio mypolicy user=james
```

```sh
mc admin policy set myminio mypolicy group=bigdatausers
```

**Please note that when AD/LDAP is configured, MinIO will not support long term users defined internally.** Only AD/LDAP users are allowed. In addition to this, the server will not support operations on users or groups using `mc admin user` or `mc admin group` commands except `mc admin user info` and `mc admin group info` to list set policies for users and groups. This is because users and groups are defined externally in AD/LDAP.


## API Request Parameters

### LDAPUsername
Is AD/LDAP username to login. Application must ask user for this value to successfully obtain rotating access credentials from AssumeRoleWithLDAPIdentity.

| Params               | Value                                          |
| :--                  | :--                                            |
| *Type*               | *String*                                       |
| *Length Constraints* | *Minimum length of 2. Maximum length of 2048.* |
| *Required*           | *Yes*                                          |


### LDAPPassword
Is AD/LDAP username password to login. Application must ask user for this value to successfully obtain rotating access credentials from AssumeRoleWithLDAPIdentity.

| Params               | Value                                          |
| :--                  | :--                                            |
| *Type*               | *String*                                       |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required*           | *Yes*                                          |

### Version
Indicates STS API version information, the only supported value is '2011-06-15'.  This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *Yes*    |

### Policy
An IAM policy in JSON format that you want to use as an inline session policy. This parameter is optional. Passing policies to this operation returns new temporary credentials. The resulting session's permissions are the intersection of the canned policy name and the policy set here. You cannot use this policy to grant more permissions than those allowed by the canned policy name being assumed.

| Params        | Value                                          |
| :--           | :--                                            |
| *Type*        | *String*                                       |
| *Valid Range* | *Minimum length of 1. Maximum length of 2048.* |
| *Required*    | *No*                                           |

### Response Elements
XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements)

### Errors
XML error response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_Errors)

## Sample `POST` Request
```
http://minio.cluster:9000?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=foouser&LDAPPassword=foouserpassword&Version=2011-06-15
```

## Sample Response
```
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithLDAPIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithLDAPIdentityResult>
    <AssumedRoleUser>
      <Arn/>
      <AssumeRoleId/>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>Y4RJU1RNFGK48LGO9I2S</AccessKeyId>
      <SecretAccessKey>sYLRKS1Z7hSjluf6gEbb9066hnx315wHTiACPAjg</SecretAccessKey>
      <Expiration>2019-08-08T20:26:12Z</Expiration>
      <SessionToken>eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJZNFJKVTFSTkZHSzQ4TEdPOUkyUyIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTQxODExMDcxLCJpYXQiOjE1NDE4MDc0NzEsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiYTBiMjc2MjktZWUxYS00M2JmLTg3MzktZjMzNzRhNGNkYmMwIn0.ewHqKVFTaP-j_kgZrcOEKroNUjk10GEp8bqQjxBbYVovV0nHO985VnRESFbcT6XMDDKHZiWqN2vi_ETX_u3Q-w</SessionToken>
    </Credentials>
  </AssumeRoleWithLDAPIdentity>
  <ResponseMetadata/>
</AssumeRoleWithLDAPIdentityResponse>
```

## Testing

With multiple OU hierarchies for users, and multiple group search base DN's.
```
$ export MINIO_ACCESS_KEY=minio
$ export MINIO_SECRET_KEY=minio123
$ export MINIO_IDENTITY_LDAP_SERVER_ADDR='my.ldap-active-dir-server.com:636'
$ export MINIO_IDENTITY_LDAP_USERNAME_FORMAT='cn=%s,ou=Users,ou=BUS1,ou=LOB,dc=somedomain,dc=com;cn=%s,ou=Users,ou=BUS2,ou=LOB,dc=somedomain,dc=com'
$ export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN='dc=minioad,dc=local;dc=somedomain,dc=com'
$ export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER='(&(objectclass=group)(member=%s))'
$ export MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE='cn'
$ minio server ~/test
```

```
$ go run ldap.go -u foouser -p foopassword

##### Credentials
{
        "accessKey": "NUIBORZYTV2HG2BMRSXR",
        "secretKey": "qQlP5O7CFPc5m5IXf1vYhuVTFj7BRVJqh0FqZ86S",
        "expiration": "2018-08-21T17:10:29-07:00",
        "sessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJOVUlCT1JaWVRWMkhHMkJNUlNYUiIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTM0ODk2NjI5LCJpYXQiOjE1MzQ4OTMwMjksImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiNjY2OTZjZTctN2U1Ny00ZjU5LWI0MWQtM2E1YTMzZGZiNjA4In0.eJONnVaSVHypiXKEARSMnSKgr-2mlC2Sr4fEGJitLcJF_at3LeNdTHv0_oHsv6ZZA3zueVGgFlVXMlREgr9LXA"
}
```
