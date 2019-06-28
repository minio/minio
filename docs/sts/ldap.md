# MinIO LDAP Integration

MinIO provides a custom STS API that allows integration with LDAP
based corporate environments. The flow is as follows:

1. User provides their LDAP username and password to the STS API.
2. MinIO logs-in to the LDAP server as the user - if the login
   succeeds the user is authenticated.
3. MinIO then queries the LDAP server for a list of groups that the
   user is a member of.
   - This is done via a customizable LDAP search query.
4. MinIO then generates temporary credentials for the user storing the
   list of groups in a cryptographically secure session token. The
   temporary access key, secret key and session token are returned to
   the user.
5. The user can now use these credentials to make requests to the
   MinIO server.

The administrator will associate IAM access policies with each group
and if required with the user too. The MinIO server then evaluates
applicable policies on a user (these are the policies associated with
the groups along with the policy on the user if any) to check if the
request should be allowed or denied.

## Configuring LDAP on MinIO

LDAP configuration is designed to be simple for the MinIO
administrator.

The full path of a user DN (Distinguished Name)
(e.g. `uid=johnwick,cn=users,cn=accounts,dc=minio,dc=io`) is
configured as a format string in the
**MINIO_IDENTITY_LDAP_USERNAME_FORMAT** environment variable. This
allows an LDAP user to not specify this whole string in the LDAP STS
API. Instead the user only needs to specify the username portion
(i.e. `johnwick` in this example) that will be substituted into the
format string configured on the server.

MinIO can be configured to find the groups of a user from LDAP by
specifying the **MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER** and
**MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE** environment
variables. When a user logs in via the STS API, the MinIO server
queries the LDAP server with the given search filter and extracts the
given attribute from the search results. These values represent the
groups that the user is a member of. On each access MinIO applies the
IAM policies attached to these groups in MinIO.

LDAP is configured via the following environment variables:

| Variable                                     | Required?                 | Purpose                                             |
|----------------------------------------------|---------------------------|-----------------------------------------------------|
| **MINIO_IDENTITY_LDAP_SERVER_ADDR**               | **YES**                   | LDAP server address                                 |
| **MINIO_IDENTITY_LDAP_USERNAME_FORMAT**      | **YES**                   | Format of full username DN                          |
| **MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN** | **NO**                    | Base DN in LDAP hierarchy to use in search requests |
| **MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER**  | **NO**                    | Search filter to find groups of a user              |
| **MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE** | **NO**                    | Attribute of search results to use as group name    |
| **MINIO_IDENTITY_LDAP_STS_EXPIRY_DURATION**  | **NO** (default: "1h")    | STS credentials validity duration                   |
| **MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY**      | **NO** (default: "false") | Disable TLS certificate verification                |

Please note that MinIO will only access the LDAP server over TLS.

An example setup for development or experimentation:

``` shell
export MINIO_IDENTITY_LDAP_SERVER_ADDR=myldapserver.com:636
export MINIO_IDENTITY_LDAP_USERNAME_FORMAT="uid=${username},cn=accounts,dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN="dc=myldapserver,dc=com"
export MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER="(&(objectclass=groupOfNames)(member=${usernamedn}))"
export MINIO_IDENTITY_LDAP_GROUP_NAME_ATTRIBUTE="cn"
export MINIO_IDENTITY_LDAP_STS_EXPIRY_DURATION=60
export MINIO_IDENTITY_LDAP_TLS_SKIP_VERIFY=true
```

### Variable substitution in LDAP configuration strings

In the configuration values described above, some values support
runtime substitutions. The substitution syntax is simply
`${variable}` - this substring is replaced with the (string) value of
`variable`. The following substitutions will be available:

| Variable     | Example Runtime Value                          | Description                                                                                                                                  |
|--------------|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| *username*   | "james"                                        | The LDAP username of a user.                                                                                                                 |
| *usernamedn* | "uid=james,cn=accounts,dc=myldapserver,dc=com" | The LDAP username DN of a user. This is constructed from the LDAP user DN format string provided to the server and the actual LDAP username. |

The **MINIO_IDENTITY_LDAP_USERNAME_FORMAT** environment variable
supports substitution of the *username* variable only.

The **MINIO_IDENTITY_LDAP_GROUP_SEARCH_FILTER** and
**MINIO_IDENTITY_LDAP_GROUP_SEARCH_BASE_DN** environment variables
support substitution of the *username* and *usernamedn* variables
only.
