# How to secure access to Minio server with TLS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

In this document, we will configure Minio servers with TLS certificates for both Linux and Windows.

## 1. Prerequisites

* Download Minio server from [here](https://docs.minio.io/docs/minio-quickstart-guide)

## 2. Generate TLS certificate

### Linux

Minio supports only key/certificate in PEM format on Linux.

#### With Let's Encrypt

Please explore [here](https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio)

#### With generate_cert.go (self-signed certificate)

You need to download [generate_cert.go](https://golang.org/src/crypto/tls/generate_cert.go?m=text) which is a simple go tool for generating self-signed certificates but works for the most of cases.

`generate_cert.go` already provides SAN certificates with DNS and IP entries:

```sh
go run generate_cert.go -ca --host "10.10.0.3"
```

#### With OpenSSL:

Generate the private key:

```sh
openssl genrsa -out private.key 1024
```

Generate the self-signed certificate:

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=country/ST=state/L=location/O=organization/CN=domain"
```

### Windows

Minio only supports key/certificate in PEM format on Windows. Currently we do not yet support PFX certificates.

#### Install GnuTLS

Download and decompress the Windows version of GnuTLS from [here](http://www.gnutls.org/download.html)

Make sure to add extracted GnuTLS binary path to your system path.

```
setx path "%path%;C:\Users\MyUser\Downloads\gnutls-3.4.9-w64\bin"
```

You may need to restart your powershell console for this to take affect.

#### Generate private.key

Run the following command to create `private.key`

```
certtool.exe --generate-privkey --outfile private.key 
```

#### Generate public.crt

Create a file `cert.cnf` with all the necessary information to generate a certificate.

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

# Whether this certificate will be used to encrypt data (needed
# in TLS RSA ciphersuites). Note that it is preferred to use different
# keys for encryption and signing.
encryption_key
```

Generate public certificate

```
certtool.exe --generate-self-signed --load-privkey private.key --template cert.cnf --outfile public.crt 
```

## 3. Configure Minio with the generated certificate

Copy the generated key and certificate under `certs` in your Minio config path (by default in your HOME directory `~/.minio` on Linux or `C:\Users\<Username>\.minio` on Windows) using the names `private.key` and `public.crt` for key and certificate files respectively.

## 4. Install third-party CAs

Minio can be configured to connect to other servers, whether Minio nodes or servers like NATs, Redis. If these servers use certificates that are not registered in one of the known certificates authorities, you can make Minio server trust these CAs by dropping these certificates under Minio config path (`~/.minio/certs/CAs/` on Linux or `C:\Users\<Username>\.minio\certs\CAs` on Windows).

# Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
* [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)
