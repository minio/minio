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

