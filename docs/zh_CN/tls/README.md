# 使用TLS安全的访问Minio服务[![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

本文，我们讲介绍如何在Linux和Windows上配置Minio服务使用TLS。

## 1. 前提条件

* 下载Minio server [这里](https://docs.minio.io/docs/minio-quickstart-guide)

## 2. 配置已存在的证书

如果你已经有私钥和公钥证书，你需要将它们拷贝到Minio的config/`certs`文件夹,分别取名为`private.key` 和 `public.crt`。

如果这个证书是被证书机构签发的，`public.crt`应该是服务器的证书，任何中间体的证书以及CA的根证书的级联。 

## 3. 生成证书

### Linux

Minio在Linux只支持使用PEM格式的key/certificate。

#### 使用 Let's Encrypt

更多信息，请访问 [这里](https://docs.minio.io/docs/zh_CN/generate-let-s-encypt-certificate-using-concert-for-minio)

#### 使用 generate_cert.go (self-signed certificate)

你需要下载 [generate_cert.go](https://golang.org/src/crypto/tls/generate_cert.go?m=text)，它是一个简单的go工具，可以生成自签名的证书，不过大多数情况下用着都是木有问题的。

`generate_cert.go` 已经提供了带有DNS和IP条目的SAN证书:

```sh
go run generate_cert.go -ca --host "10.10.0.3"
```

#### 使用 OpenSSL:

生成私钥:

```sh
openssl genrsa -out private.key 2048
```

生成自签名证书:

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=domain"
```

### Windows

Minio在Windows上只支持PEM格式的key/certificate，目前不支持PFX证书。

#### 安装 GnuTLS

下载并解压[GnuTLS](http://www.gnutls.org/download.html)

确保将解压后的binary路径加入到系统路径中。

```
setx path "%path%;C:\Users\MyUser\Downloads\gnutls-3.4.9-w64\bin"
```

你可能需要重启powershell控制台来使其生效。

#### 生成private.key

运行下面的命令来生成 `private.key`

```
certtool.exe --generate-privkey --outfile private.key 
```

#### 生成public.crt

创建文件`cert.cnf`，填写必要信息来生成证书。

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

生成公钥证书

```
certtool.exe --generate-self-signed --load-privkey private.key --template cert.cnf --outfile public.crt 
```

## 4. 安装第三方CAs

Minio可以配置成连接其它服务，不管是Minio节点还是像NATs、Redis这些。如果这些服务用的不是在已知证书机构注册的证书，你可以让Minio服务信任这些CA，怎么做呢，将这些证书放到Minio配置路径下(`~/.minio/certs/CAs/` Linux 或者 `C:\Users\<Username>\.minio\certs\CAs` Windows).

# 了解更多
* [Minio快速入门](https://docs.minio.io/docs/zh_CN/minio-quickstart-guide)
* [Minio客户端权威指南](https://docs.minio.io/docs/zh_CN/minio-client-complete-guide)
