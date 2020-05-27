# How to secure access to MinIO server with TLS [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

This guide explains how to configure MinIO Server with TLS certificates on Linux and Windows platforms.

1. [Install MinIO Server](#install-minio-server) 
2. [Use an Existing Key and Certificate with MinIO](#use-an-existing-key-and-certificate-with-minio) 
3. [Generate and use Self-signed Keys and Certificates with MinIO](#generate-use-self-signed-keys-certificates) 
4. [Install Certificates from Third-party CAs](#install-certificates-from-third-party-cas)

## <a name="install-minio-server"></a>1. Install MinIO Server

Install MinIO Server using the instructions in the [MinIO Quickstart Guide](http://docs.min.io/docs/minio-quickstart-guide).

## <a name="use-an-existing-key-and-certificate-with-minio"></a>2. Use an Existing Key and Certificate with MinIO 

This section describes how to use a private key and public certificate that have been obtained from a certificate authority (CA). If these files have not been obtained, skip to [3. Generate Self-signed Certificates](#generate-use-self-signed-keys-certificates) or generate them with [Let's Encrypt](https://letsencrypt.org) using these instructions: [Generate Let's Encrypt certificate using Certbot for MinIO](https://docs.min.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio.html).

Copy the existing private key and public certificate to the `certs` directory. The default certs directory is:
* **Linux:** `${HOME}/.minio/certs`
* **Windows:** `%%USERPROFILE%%\.minio\certs`

**Note:**
* Location of custom certs directory can be specified using `--certs-dir` command line option.
* Inside the `certs` directory, the private key must by named `private.key` and the public key must be named `public.crt`.
* A certificate signed by a CA contains information about the issued identity (e.g. name, expiry, public key) and any intermediate certificates. The root CA is not included.

## <a name="generate-use-self-signed-keys-certificates"></a>3. Generate and use Self-signed Keys and Certificates with MinIO

This section describes how to generate a self-signed certificate using various tools:

* 3.1 [Use generate_cert.go to Generate a Certificate](#using-go) 
* 3.2 [Use OpenSSL to Generate a Certificate](#using-open-ssl) 
* 3.3 [Use OpenSSL (with IP address) to Generate a Certificate](#using-open-ssl-with-ip) 
* 3.4 [Use GnuTLS (for Windows) to Generate a Certificate](#using-gnu-tls) 

**Note:**
* MinIO only supports keys and certificates in PEM format on Linux and Windows.
* MinIO doesn't currently support PFX certificates.

### <a name="using-go"></a>3.1 Use generate_cert.go to Generate a Certificate

Download [`generate_cert.go`](https://golang.org/src/crypto/tls/generate_cert.go?m=text).

`generate_cert.go` is a simple *Go* tool to generate self-signed certificates, and provides SAN certificates with DNS and IP entries:

```sh
go run generate_cert.go -ca --host "10.10.0.3"
```

A response similar to this one should be displayed:

```
2018/11/21 10:16:18 wrote cert.pem
2018/11/21 10:16:18 wrote key.pem
```

Rename `cert.pem` to `public.crt` and `key.pem` to `private.key`.

### <a name="using-open-ssl"></a>3.2 Use OpenSSL to Generate a Certificate

Use one of the following methods to generate a certificate using `openssl`:

* 3.2.1 [Generate a private key with ECDSA](#generate-private-key-with-ecdsa) 
* 3.2.2 [Generate a private key with RSA](#generate-private-key-with-rsa)
* 3.2.3 [Generate a self-signed certificate](#generate-a-self-signed-certificate)

#### 3.2.1 <a name="generate-private-key-with-ecdsa"></a>Generate a private key with ECDSA.

Use the following command to generate a private key with ECDSA:

```sh
openssl ecparam -genkey -name prime256v1 | openssl ec -out private.key
```

A response similar to this one should be displayed:

```
read EC key
writing EC key
```

Alternatively, use the following command to generate a private ECDSA key protected by a password:

```sh
openssl ecparam -genkey -name prime256v1 | openssl ec -aes256 -out private.key -passout pass:PASSWORD
```

**Note:** NIST curves P-384 and P-521 are not currently supported.

#### 3.2.2 <a name="generate-private-key-with-rsa"></a>Generate a private key with RSA.

Use the following command to generate a private key with RSA:

```sh
openssl genrsa -out private.key 2048
```  
A response similar to this one should be displayed:

```
Generating RSA private key, 2048 bit long modulus
............................................+++
...........+++
e is 65537 (0x10001)
```

Alternatively, use the following command to generate a private RSA key protected by a password:

```sh
openssl genrsa -aes256 -out private.key 2048 -passout pass:PASSWORD
```

**Note:** When using a password-protected private key, the password must be provided through the environment variable `MINIO_CERT_PASSWD` using the following command:

```sh
export MINIO_CERT_PASSWD=<PASSWORD>
``` 

The default OpenSSL format for private encrypted keys is PKCS-8, but MinIO only supports PKCS-1. An RSA key that has been formatted with PKCS-8 can be converted to PKCS-1 using the following command:

```sh
openssl rsa -in private-pkcs8-key.key -aes256 -passout pass:PASSWORD -out private.key
```  

#### <a name="generate-a-self-signed-certificate"></a>3.2.3 Generate a self-signed certificate.

Use the following command to generate a self-signed certificate and enter a passphrase when prompted:

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=<domain.com>"
```

**Note:** Replace `<domain.com>` with the development domain name.

Alternatively, use the command below to generate a self-signed wildcard certificate that is valid for all subdomains under `<domain.com>`. Wildcard certificates are useful for deploying distributed MinIO instances, where each instance runs on a subdomain under a single parent domain.

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=<*.domain.com>"
```

### <a name="using-open-ssl-with-ip"></a>3.3 Use OpenSSL (with IP address) to Generate a Certificate

This section describes how to specify an IP address to `openssl` when generating a certificate.

#### 3.3.1 Create a configuration file.

Create a file named `openssl.conf` with the content below. Change `IP.1` to point to the correct IP address:

```sh
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = VA
L = Somewhere
O = MyOrg
OU = MyOU
CN = MyServerName

[v3_req]
subjectAltName = @alt_names

[alt_names]
IP.1 = 127.0.0.1
```

#### 3.3.2 Run `openssl` and specify the configuration file:

```sh
openssl req -x509 -nodes -days 730 -newkey rsa:2048 -keyout private.key -out public.crt -config openssl.conf
```

### <a name="using-gnu-tls"></a>3.4 Use GnuTLS (for Windows) to Generate a Certificate

This section describes how to use GnuTLS on Windows to generate a certificate.

#### 3.4.1 Install and configure GnuTLS.
Download and decompress the Windows version of GnuTLS from [here](http://www.gnutls.org/download.html).

Use PowerShell to add the path of the extracted GnuTLS binary to the system path:

```
setx path "%path%;C:\Users\MyUser\Downloads\gnutls-3.4.9-w64\bin"
```

**Note:** PowerShell may need to be restarted for this change to take effect.

#### 3.4.2 Generate a private key:
Run the following command to generate a private `.key` file:

```
certtool.exe --generate-privkey --outfile private.key
```

A response similar to this one should be displayed:

```
Generating a 3072 bit RSA private key...
```

#### 3.4.3 Generate a public certificate:

Create a file called `cert.cnf` with the content below. This file contains all of the information necessary to generate a certificate using `certtool.exe`:

```
# X.509 Certificate options
#
# DN options

# The organization of the subject.
organization = "Example Inc."

# The organizational unit of the subject.
#unit = "sleeping dept."

# The state of the certificate owner.
state = "Example"

# The country of the subject. Two letter code.
country = "EX"

# The common name of the certificate owner.
cn = "Sally Certowner"

# In how many days, counting from today, this certificate will expire.
expiration_days = 365

# X.509 v3 extensions

# DNS name(s) of the server
dns_name = "localhost"

# (Optional) Server IP address
ip_address = "127.0.0.1"

# Whether this certificate will be used for a TLS server
tls_www_server
```

Run `certtool.exe` and specify the configuration file to generate a certificate:

```
certtool.exe --generate-self-signed --load-privkey private.key --template cert.cnf --outfile public.crt
```

## <a name="install-certificates-from-third-party-cas"></a>4. Install Certificates from Third-party CAs

MinIO can connect to other servers, including MinIO nodes or other server types such as NATs and Redis. If these servers use certificates that were not registered with a known CA, add trust for these certificates to MinIO Server by placing these certificates under one of the following MinIO configuration paths:
* **Linux:** `~/.minio/certs/CAs/`
* **Windows**: `C:\Users\<Username>\.minio\certs\CAs`

# Explore Further
* [TLS Configuration for MinIO server on Kubernetes](https://github.com/minio/minio/tree/master/docs/tls/kubernetes)
* [MinIO Client Complete Guide](https://docs.min.io/docs/minio-client-complete-guide)
* [Generate Let's Encrypt Certificate](https://docs.min.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio)
* [Setup nginx Proxy with MinIO Server](https://docs.min.io/docs/setup-nginx-proxy-with-minio)
