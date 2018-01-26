# How to secure access to Minio server with TLS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This document explains how to configure Minio server with TLS certificates on Linux and Windows platforms.

## 1. Prerequisites

Download Minio server from [here](https://docs.minio.io/docs/minio-quickstart-guide)

## 2. Existing certificates

If you have already acquired private keys and public certificates, copy them under `certs` directory in your Minio config directory. By default config directory is `${HOME}/.minio/` or `%%USERPROFILE%%\.minio\` (based on your operating system). Note that the file should be named as `private.key` and `public.crt` for key and certificate respectively.

If the certificate is signed by a certificate authority (CA), `public.crt` should be the concatenation of the server's certificate, any intermediates, and the CA's root certificate.

If you're looking to generate CA certificate for Minio using Let's Encrypt, follow the docs [here](https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio).

## 3. Generate self-signed certificates

Before generating your self-signed certificate, note that

- Minio supports only key/certificate in PEM format on Linux.
- Minio supports only key/certificate in PEM format on Windows. We don't support PFX certificates currently.

### Using generate_cert.go

Download [generate_cert.go](https://golang.org/src/crypto/tls/generate_cert.go?m=text). This is a simple go tool to generate self-signed certificates.

`generate_cert.go` already provides SAN certificates with DNS and IP entries:

```sh
go run generate_cert.go -ca --host "10.10.0.3"
```

### Using OpenSSL

**Generate the private key**:

1. **ECDSA:**  
```sh
openssl ecparam -genkey -name prime256v1 -out private.key
```
or protect the private key additionally with a password:  
```sh
openssl ecparam -genkey -name prime256v1 | openssl ec -aes256 -out private.key -passout pass:PASSWORD
```
2. **RSA:**
```sh
openssl genrsa -out private.key 2048
```  
or protect the private key additionally with a password:  
```sh
openssl genrsa -aes256 -out private.key 2048 -passout pass:PASSWORD
```

If a password-protected private key is used the password must be provided through the environment variable `MINIO_CERT_PASSWD`:
```sh
export MINIO_CERT_PASSWD=PASSWORD
``` 
Please use your own password instead of PASSWORD.

**Notice:**  
The OpenSSL default format for encrypted private keys is PKCS-8. Minio only supports PKCS-1 encrypted private keys.
An encrypted private PKCS-8 formated RSA key can be converted to an encrypted private PKCS-1 formated RSA key by:
```sh
openssl rsa -in private-pkcs8-key.key -aes256 -passout pass:PASSWORD -out private.key
```  

**Generate the self-signed certificate**:

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=domain"
```

### Using OpenSSL (with IP address)

Create a file called `openssl.conf` and add the below text in the file. Note that you'll need to change the `IP.1` field to point to correct IP address.

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

Then run

```sh
openssl req -x509 -nodes -days 730 -newkey rsa:2048 -keyout private.key -out public.crt -config openssl.conf
```

### Using GnuTLS (for Windows)

Download and decompress the Windows version of GnuTLS from [here](http://www.gnutls.org/download.html)

Make sure to add extracted GnuTLS binary path to your system path.

```
setx path "%path%;C:\Users\MyUser\Downloads\gnutls-3.4.9-w64\bin"
```

You may need to restart your powershell console for this to take affect.

- Run the following command to create `private.key`

```
certtool.exe --generate-privkey --outfile private.key
```

- Create a file `cert.cnf` with all the necessary information to generate a certificate.

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

## 4. Install third-party CAs

Minio can be configured to connect to other servers, whether Minio nodes or servers like NATs, Redis. If these servers use certificates that are not registered in one of the known certificates authorities, you can make Minio server trust these CAs by dropping these certificates under Minio config path (`~/.minio/certs/CAs/` on Linux or `C:\Users\<Username>\.minio\certs\CAs` on Windows).

# Explore Further
* [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)
* [Generate Let's Encrypt Certificate](https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio)
* [Setup Nginx Proxy with Minio Server](https://docs.minio.io/docs/setup-nginx-proxy-with-minio)
