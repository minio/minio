# AssumeRoleWithLDAPIdentity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

## Introduction

MinIO provides a custom STS API that allows integration with LDAP based corporate environments including Microsoft Active Directory. The MinIO server can be configured in two possible modes: either using a LDAP separate service account, called lookup-bind mode or in username-format mode. In either case the login flow for a user is the same as the STS flow:

1. User provides their AD/LDAP username and password to the STS API.
2. MinIO verifies the login credentials with the AD/LDAP server.
3. On success, MinIO queries the AD/LDAP server for a list of groups that the user is a member of.
   - This is done via a customizable AD/LDAP search query.
4. MinIO then generates temporary credentials for the user storing the list of groups in a cryptographically secure session token. The temporary access key, secret key and session token are returned to the user.
5. The user can now use these credentials to make requests to the MinIO server.

The administrator will associate IAM access policies with each group and if required with the user too. The MinIO server then evaluates applicable policies on a user (these are the policies associated with the groups along with the policy on the user if any) to check if the request should be allowed or denied.

## Configuring AD/LDAP on MinIO

LDAP STS configuration can be performed via MinIO's standard configuration API (i.e. using `mc admin config set/get` commands) or equivalently via environment variables. For brevity we refer to environment variables here.

LDAP is configured via the following environment variables:

```
$ mc admin config set myminio identity_ldap --env
KEY:
identity_ldap  enable LDAP SSO support

ARGS:
MINIO_IDENTITY_LDAP_SERVER_ADDR*            (address)   AD/LDAP server address e.g. "myldapserver.com:636"
MINIO_IDENTITY_LDAP_STS_EXPIRY              (duration)  temporary credentials validity duration in s,m,h,d. Default is "1h"
MINIO_IDENTITY_LDAP_LOOKUP_BIND_DN          (string)    DN for LDAP read-only service account used to perform DN and group lookups
MINIO_IDENTITY_LDAP_LOOKUP_BIND_PASSWORD    (string)    Password for LDAP read-only service account used to perform DN and group lookups
MINIO_IDENTITY_LDAP_USER_DN_SEARCH_BASE_DN  (string)    Base LDAP DN to search for user DN
MINIO_IDENTITY_LDAP_USER_DN_SEARCH_FILTER   (string)    Search filter to lookup user DN
MINIO_IDENTITY_LDAP_USERNAME_FORMAT         (list)      ";" separated list of username bind DNs e.g. "uid=%s,cn=accounts,dc=myldapserver,dc=com"
MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER     (string)    search filter for groups e.g. "(&(objectclass=groupOfNames)(memberUid=%s))"
MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN    (list)      ";" separated list of group search base DNs e.g. "dc=myldapserver,dc=com"
MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY         (on|off)    trust server TLS without verification, defaults to "off" (verify)
MINIO_IDENTITY_LDAP_SERVER_INSECURE         (on|off)    allow plain text connection to AD/LDAP server, defaults to "off"
MINIO_IDENTITY_LDAP_SERVER_STARTTLS         (on|off)    use StartTLS connection to AD/LDAP server, defaults to "off"
MINIO_IDENTITY_LDAP_COMMENT                 (sentence)  optionally add a comment to this setting
```

### Supported modes of operation ###

The two supported modes of LDAP configuration differ in how the MinIO server derives the Distinguished Name (DN) of the user from their username provided in the STS API. _Exactly one must be used in a valid configuration_.

Once a unique DN for the user is derived, the server verifies the user's credentials with the LDAP server and on success, looks up the user's groups via a configured group search query and finally temporary object storage credentials are generated and returned.

#### Lookup-Bind Mode ####

In this mode, the a low-privilege read-only LDAP service account is configured in the MinIO server by providing the account's Distinguished Name (DN) and password. It is the new and preferred mode for LDAP integration.

This service account is used by the MinIO server to lookup a user's DN given their username. The lookup is performed via an LDAP search filter query that is also configured by the administrator.

This mode is enabled by setting the following variables:

```
MINIO_IDENTITY_LDAP_LOOKUP_BIND_DN          (string)    DN for LDAP read-only service account used to perform DN and group lookups
MINIO_IDENTITY_LDAP_LOOKUP_BIND_PASSWORD    (string)    Password for LDAP read-only service account used to perform DN and group lookups
MINIO_IDENTITY_LDAP_USER_DN_SEARCH_BASE_DN  (string)    Base LDAP DN to search for user DN
MINIO_IDENTITY_LDAP_USER_DN_SEARCH_FILTER   (string)    Search filter to lookup user DN
```

If you set an empty lookup bind password, the lookup bind will use the unauthenticated authentication mechanism, as described in [RFC 4513 Section 5.1.2](https://tools.ietf.org/html/rfc4513#section-5.1.2).

#### Username-Format Mode ####

In this mode, the server does not use a separate LDAP service account. Instead, the username and password provided in the STS API call are used to login to the LDAP server and also to lookup the user's groups. This mode preserves older behavior for compatibility, but users are encouraged to use the Lookup-Bind mode.

The DN to use to login to LDAP is computed from a username format configuration parameter. This is a list of possible DN templates to be used. For each such template, the username is substituted and the DN is generated. Each generated DN is tried by the MinIO server to login to LDAP. If exactly one successful DN is found, it is used to perform the groups lookup as well.

This mode is enabled by setting the following variables:

```
MINIO_IDENTITY_LDAP_USERNAME_FORMAT         (list)      ";" separated list of username bind DNs e.g. "uid=%s,cn=accounts,dc=myldapserver,dc=com"
```

### Group membership search

MinIO can be configured to find the groups of a user from AD/LDAP by specifying the folllowing variables:

```
MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER     (string)    search filter for groups e.g. "(&(objectclass=groupOfNames)(memberUid=%s))"
MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN    (list)      ";" separated list of group search base DNs e.g. "dc=myldapserver,dc=com"
```

When a user logs in via the STS API, the MinIO server queries the AD/LDAP server with the given search filter and extracts the DN from the search results. These values represent the groups that the user is a member of. On each access MinIO applies the IAM policies attached to these groups in MinIO.

**MinIO sends LDAP credentials to LDAP server for validation. So we _strongly recommend_ to use MinIO with AD/LDAP server over TLS or StartTLS _only_. Using plain-text connection between MinIO and LDAP server means _credentials can be compromised_ by anyone listening to network traffic.**

If a self-signed certificate is being used, the certificate can be added to MinIO's certificates directory, so it can be trusted by the server. An example setup for development or experimentation:

```shell
export MINIO_IDENTITY_LDAP_SERVER_ADDR=myldapserver.com:636
export MINIO_IDENTITY_LDAP_USERNAME_FORMAT="uid=%s,cn=accounts,dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN="dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER="(&(objectclass=groupOfNames)(memberUid=%s)$)"
export MINIO_IDENTITY_LDAP_STS_EXPIRY=60h
export MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY=on
```

### Variable substitution in AD/LDAP configuration strings ###

In the configuration variables, `%s` is substituted with the *username* from the STS request and `%d` is substituted with the *distinguished username (user DN)* of the LDAP user. Please see the following table for which configuration variables support these substitution variables:

| Variable                                    | Supported substitutions |
|---------------------------------------------|-------------------------|
| `MINIO_IDENTITY_LDAP_USERNAME_FORMAT`       | `%s`                    |
| `MINIO_IDENTITY_LDAP_USER_DN_SEARCH_FILTER` | `%s`                    |
| `MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER`   | `%s` and `%d`           |

## Managing User/Group Access Policy

Access policies may be configured on a group or on a user directly. Access policies are first defined on the MinIO server using IAM policy JSON syntax. The `mc` tool is used to issue the necessary commands.

**Note that by default no policy is set on a user**. Thus even if they successfully authenticate with AD/LDAP credentials, they have no access to object storage as the default access policy is to deny all access.

To define a new policy, you can use the [AWS policy generator](https://awspolicygen.s3.amazonaws.com/policygen.html). Copy the policy into a text file `mypolicy.json` and issue the command like so:

```sh
mc admin policy add myminio mypolicy mypolicy.json
```

To assign the policy to a user or group, use the full DN of the user or group:

```sh
mc admin policy set myminio mypolicy user='uid=james,cn=accounts,dc=myldapserver,dc=com'
```

```sh
mc admin policy set myminio mypolicy group='cn=projectx,ou=groups,ou=hwengg,dc=min,dc=io'
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

## Using LDAP STS API

With multiple OU hierarchies for users, and multiple group search base DN's.
```
$ export MINIO_ROOT_USER=minio
$ export MINIO_ROOT_PASSWORD=minio123
$ export MINIO_IDENTITY_LDAP_SERVER_ADDR='my.ldap-active-dir-server.com:636'
$ export MINIO_IDENTITY_LDAP_USERNAME_FORMAT='cn=%s,ou=Users,ou=BUS1,ou=LOB,dc=somedomain,dc=com;cn=%s,ou=Users,ou=BUS2,ou=LOB,dc=somedomain,dc=com'
$ export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN='dc=minioad,dc=local;dc=somedomain,dc=com'
$ export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER='(&(objectclass=groupOfNames)(member=%d))'
$ minio server ~/test
```
You can make sure it works appropriately using our [example program](https://raw.githubusercontent.com/minio/minio/master/docs/sts/ldap.go):
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

## Explore Further
- [MinIO Admin Complete Guide](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [The MinIO documentation website](https://docs.min.io)
