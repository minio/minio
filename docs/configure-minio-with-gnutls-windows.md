
# Generate self signed certificate with GnuTLS under Windows


## 1. Install GnuTLS

Download and decompress the Windows version of GnuTLS from [here](http://www.gnutls.org/download.html)

Add the directory `gnutls-3.4.9-w64/bin` to your PATH environment and restart your console

## 2. Generate private.key

Run the following command to create `private.key`
```
certtool.exe --generate-privkey --outfile private.key 
```

## 3. Generate public.crt

The easiest way is to generate certificate is to specify its information under a file. You can find an example below. We'll call that file `cert.cnf`.

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

Now, it is time to generate the public certificate using this command:

```sh
certtool.exe --generate-self-signed --load-privkey private.key --template cert.cnf --outfile public.crt 
```

That's it.
