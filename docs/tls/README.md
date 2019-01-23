# How to secure access to Minio server with TLS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This guide explains how to configure Minio Server with TLS certificates on Linux and Windows platforms.

1. [Install Minio Server](#install-minio-server) 
2. [Use an Existing Key and Certificate with Minio](#use-an-existing-key-and-certificate-with-minio) 
3. [Generate and use Self-signed Keys and Certificates with Minio](#generate-use-self-signed-keys-certificates) 
4. [Install Certificates from Third-party CAs](#install-certificates-from-third-party-cas)

## <a name="install-minio-server"></a>1. Install Minio Server

Install Minio Server using the instructions in the [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

## <a name="use-an-existing-key-and-certificate-with-minio"></a>2. Use an Existing Key and Certificate with Minio 

This section describes how to use a private key and public certificate that have been obtained from a certificate authority (CA). If these files have not been obtained, skip to [3. Generate Self-signed Certificates](#generate-use-self-signed-keys-certificates) or generate them with [Let's Encrypt](https://letsencrypt.org) using these instructions: [https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio](https://docs.minio.io/docs/).

Copy the existing private key and public certificate to the `certs` directory. The default certs directory is:
* **Linux:** `${HOME}/.minio/certs`
* **Windows:** `%%USERPROFILE%%\.minio\certs`

> NOTE: Location of custom certs directory can be specified using `--certs-dir` command line option.

**Note:** 
* The key and certificate files must be appended with `.key` and `.crt`, respectively.
* A certificate signed by a CA contains information about the issued identity (e.g. name, expiry, public key) and any intermediate certificates. The root CA is not included.

## <a name="generate-use-self-signed-keys-certificates"></a>3. Generate and use Self-signed Keys and Certificates with Minio

The Minio Server can generate self-signed certificates, for example:

```sh
minio server ~/minio --auto-tls localhost
```

[![Output](https://raw.githubusercontent.com/minio/minio/master/docs/tls/startup.png)](#generate-use-self-signed-keys-certificates)

The `--auto-tls` flag enables self-signed certificate generation and expects a comma-separated list of 
subject alternative names (SAN) - usually domain names or IP addresses.

If there is no valid private key within the `certs` directory the Minio Server will generate a new one.
Additionally, the Minio Server generates a new TLS certificate with the provided SANs from the private key.

The generated certificate has a lifetime of one year. However, self-signed certificates are renewed when they expire.
In contrast the private key is not re-generated. That means that the certificate changes when it expires while 
its public key and the corresponding private key stay the same.

The Minio Server can also encrypt the generated private key using a password before storing it in the `certs` directory.
Therefore the environment variable `MINIO_CERT_PASSWD` must be set **before** starting the Minio Server:

```sh
export MINIO_CERT_PASSWD=DoNotUseThisPassword!
``` 

Further the Minio Server will print two hash values on startup. One is the SHA-256 hash value of the certificate.
This value **should** be equal to the SHA-256 fingerprint which you can view in your browser. However, whenever the
certificate (**not the public key**) changes - for instance if the certificate expires - then this hash value
changes, too.   
The other is the HPKP fingerprint of the public key. This value identifies the server's public key. It is usually 
not changed when the certificate is renewed. Therefore, this value may stay the same even if your certificate expires.

## <a name="install-certificates-from-third-party-cas"></a>4. Install Certificates from Third-party CAs

Minio can connect to other servers, including Minio nodes or other server types such as NATs and Redis. If these servers use certificates that were not registered with a known CA, add trust for these certificates to Minio Server by placing these certificates under one of the following Minio configuration paths:
* **Linux:** `~/.minio/certs/CAs/`
* **Windows**: `C:\Users\<Username>\.minio\certs\CAs`

# Explore Further
* [TLS Configuration for Minio server on Kubernetes](https://github.com/minio/minio/tree/master/docs/tls/kubernetes)
* [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)
* [Generate Let's Encrypt Certificate](https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio)
* [Setup nginx Proxy with Minio Server](https://docs.minio.io/docs/setup-nginx-proxy-with-minio)
