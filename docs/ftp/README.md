# MinIO FTP/SFTP Server

MinIO natively supports FTP/SFTP protocol, this allows any ftp/sftp client to upload and download files.

Currently supported `FTP/SFTP` operations are as follows:

| ftp-client commands | supported |
|:-------------------:|:----------|
| get                 | yes       |
| put                 | yes       |
| ls                  | yes       |
| mkdir               | yes       |
| rmdir               | yes       |
| delete              | yes       |
| append              | no        |
| rename              | no        |

MinIO supports following FTP/SFTP based protocols to access and manage data.

- Secure File Transfer Protocol (SFTP) – Defined by the Internet Engineering Task Force (IETF) as an
  extended version of SSH 2.0, allowing file transfer over SSH and for use with Transport Layer
  Security (TLS) and VPN applications.

- File Transfer Protocol over SSL/TLS (FTPS) – Encrypted FTP communication via TLS certificates.

- File Transfer Protocol (FTP) – Defined by RFC114 originally, and replaced by RFC765 and RFC959
  unencrypted FTP communication (Not-recommended)

## Scope

- All IAM Credentials are allowed access excluding rotating credentials, rotating credentials
  are not allowed to login via FTP/SFTP ports, you must use S3 API port for if you are using
  rotating credentials.

- Access to bucket(s) and object(s) are governed via IAM policies associated with the incoming
  login credentials.

- Allows authentication and access for all
  - Built-in IDP users and their respective service accounts
  - LDAP/AD users and their respective service accounts
  - OpenID/OIDC service accounts

- On versioned buckets, FTP/SFTP only operates on latest objects, if you need to retrieve
  an older version you must use an `S3 API client` such as [`mc`](https://github.com/minio/mc).

- All features currently used by your buckets will work as is without any changes
  - SSE (Server Side Encryption)
  - Replication (Server Side Replication)

## Prerequisites

- It is assumed you have users created and configured with relevant access policies, to start with
  use basic "readwrite" canned policy to test all the operations before you finalize on what level
  of restrictions are needed for a user.

- No "admin:*" operations are needed for FTP/SFTP access to the bucket(s) and object(s), so you may
  skip them for restrictions.

## Usage

Start MinIO in a distributed setup, with 'ftp/sftp' enabled.

```
minio server http://server{1...4}/disk{1...4}
   --ftp="address=:8021" --ftp="passive-port-range=30000-40000" \
   --sftp="address=:8022" --sftp="ssh-private-key=/home/miniouser/.ssh/id_rsa"
...
...
```

Following example shows connecting via ftp client using `minioadmin` credentials, and list a bucket named `runner`:

```
ftp localhost -P 8021
Connected to localhost.
220 Welcome to MinIO FTP Server
Name (localhost:user): minioadmin
331 User name ok, password required
Password:
230 Password ok, continue
Remote system type is UNIX.
Using binary mode to transfer files.
ftp> ls runner/
229 Entering Extended Passive Mode (|||39155|)
150 Opening ASCII mode data connection for file list
drwxrwxrwx 1 nobody nobody            0 Jan  1 00:00 chunkdocs/
drwxrwxrwx 1 nobody nobody            0 Jan  1 00:00 testdir/
...
```

Following example shows how to list an object and download it locally via `ftp` client:

```
ftp> ls runner/chunkdocs/metadata
229 Entering Extended Passive Mode (|||44269|)
150 Opening ASCII mode data connection for file list
-rwxrwxrwx 1 nobody nobody           45 Apr  1 06:13 chunkdocs/metadata
226 Closing data connection, sent 75 bytes
ftp> get
(remote-file) runner/chunkdocs/metadata
(local-file) test
local: test remote: runner/chunkdocs/metadata
229 Entering Extended Passive Mode (|||37785|)
150 Data transfer starting 45 bytes
	45        3.58 KiB/s
226 Closing data connection, sent 45 bytes
45 bytes received in 00:00 (3.55 KiB/s)
...
```


Following example shows connecting via sftp client using `minioadmin` credentials, and list a bucket named `runner`:

```
sftp -P 8022 minioadmin@localhost
minioadmin@localhost's password:
Connected to localhost.
sftp> ls runner/
chunkdocs  testdir
```

Following example shows how to download an object locally via `sftp` client:

```
sftp> get runner/chunkdocs/metadata metadata
Fetching /runner/chunkdocs/metadata to metadata
metadata                                                                                                                                                                       100%  226    16.6KB/s   00:00
sftp>
```

## Advanced options

### Change default FTP port

Default port '8021' can be changed via

```
--ftp="address=:3021"
```

### Change FTP passive port range

By default FTP requests OS to give a free port automatically, however you may want to restrict
this to specific ports in certain restricted environments via

```
--ftp="passive-port-range=30000-40000"
```

### Change default SFTP port

Default port '8022' can be changed via

```
--sftp="address=:3022"
```

### TLS (FTP)

Unlike SFTP server, FTP server is insecure by default. To operate under TLS mode, you need to provide certificates via

```
--ftp="tls-private-key=path/to/private.key" --ftp="tls-public-cert=path/to/public.crt"
```

> NOTE: if MinIO distributed setup is already configured to run under TLS, FTP will automatically use the relevant
> certs from the server certificate chain, this is mainly to add simplicity of setup. However if you wish to terminate
> TLS certificates via a different domain for your FTP servers you may choose the above command line options.


### Custom Algorithms (SFTP)

Custom algorithms can be specified via command line parameters.
Algorithms are comma separated. 
Note that valid values does not in all cases represent default values.

`--sftp=pub-key-algos=...` specifies the supported client public key
authentication algorithms. Note that this doesn't include certificate types
since those use the underlying algorithm. This list is sent to the client if
it supports the server-sig-algs extension. Order is irrelevant.

Valid values
```
ssh-ed25519
sk-ssh-ed25519@openssh.com
sk-ecdsa-sha2-nistp256@openssh.com
ecdsa-sha2-nistp256
ecdsa-sha2-nistp384
ecdsa-sha2-nistp521
rsa-sha2-256
rsa-sha2-512
ssh-rsa
ssh-dss
```

`--sftp=kex-algos=...` specifies the supported key-exchange algorithms in preference order.

Valid values: 

```
curve25519-sha256
curve25519-sha256@libssh.org
ecdh-sha2-nistp256
ecdh-sha2-nistp384
ecdh-sha2-nistp521
diffie-hellman-group14-sha256
diffie-hellman-group16-sha512
diffie-hellman-group14-sha1
diffie-hellman-group1-sha1
```

`--sftp=cipher-algos=...` specifies the allowed cipher algorithms. 
If unspecified then a sensible default is used.

Valid values: 
```
aes128-ctr
aes192-ctr
aes256-ctr
aes128-gcm@openssh.com
aes256-gcm@openssh.com
chacha20-poly1305@openssh.com
arcfour256
arcfour128
arcfour
aes128-cbc
3des-cbc
```

`--sftp=mac-algos=...` specifies a default set of MAC algorithms in preference order.
This is based on RFC 4253, section 6.4, but with hmac-md5 variants removed because they have 
reached the end of their useful life.

Valid values: 

```
hmac-sha2-256-etm@openssh.com
hmac-sha2-512-etm@openssh.com
hmac-sha2-256
hmac-sha2-512
hmac-sha1
hmac-sha1-96
```

### Certificate-based authentication

`--sftp=trusted-user-ca-key=...` specifies a file containing public key of certificate authority that is trusted
to sign user certificates for authentication.

Implementation is identical with "TrustedUserCAKeys" setting in OpenSSH server with exception that only one CA
key can be defined.

If a certificate is presented for authentication and has its signing CA key is in this file, then it may be
used for authentication for any user listed in the certificate's principals list. 

Note that certificates that lack a list of principals will not be permitted for authentication using trusted-user-ca-key.
For more details on certificates, see the CERTIFICATES section in ssh-keygen(1).
