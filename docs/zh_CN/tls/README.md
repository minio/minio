# 使用TLS安全访问MinIO服务[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

本指南将介绍如何在Linux和Windows上使用TLS证书配置MinIO服务。

1. [安装MinIO服务](#install-minio-server)  
2. [使用已存在的MinIO键和证书](#use-an-existing-key-and-certificate-with-minio)  
3. [生成和使用MinIO自签名的键和证书](#generate-use-self-signed-keys-certificates)
4. [从第三方CAs安装证书](#install-certificates-from-third-party-cas)

## <a name="install-minio-server"></a>1. 安装MinIO服务

使用[MinIO快速启动指南](https://docs.min.io/docs/minio-quickstart-guide)安装MinIO服务

## <a name="use-an-existing-key-and-certificate-with-minio"></a>2. 使用MinIO已存在的密钥和证书

这个部分将介绍如何使用从证书颁发机构(CA)获得私钥和公钥证书。如果还没有获得这些文件，请跳转至[3.生成自签名证书](#generate-use-self-signed-keys-certificates)或者在这些[指南](https://docs.min.io/docs/)下用[让我们加密吧](https://letsencrypt.org/)生成它们。

复制已存在的私钥和公钥证书到`certs`目录下，默认的certs目录为：

* **Linux：** `${HOME}/.minio/certs`
* **Windows:** `%%USERPRPRO%%\\.minio\certs`

> 注意：自定义的certs目录路径可能有特殊的用途`--certs-dir`命令选项

**注意：**
* 密钥和证书都必须分别添加`.key`和`.crt`
* 一个被CA签名的证书包含关于问题认证（例如名字、期限、公钥）和任何中间证书的信息。根CA并不被包含在内。

## 3. <a name="generate-use-self-signed-keys-certificates"></a>生成和使用MinIO自签名密钥和证书


这个部分介绍了如何用不同的工具生成一个自签名证书：

- 3.1 [使用generate_cert.go生成证书](#using-go)
- 3.2 [使用OpenSSL生成证书](#using-open-ssl)
- 3.3 [使用OpenSSL(具有IP地址)生成证书](#using-open-ssl-with-ip)
- 3.4 [使用GnuTLS(Windows)生成证书](#using-gnu-tls)

**注意：**
- MinIO只支持在Linux和Windows平台上PEM形式的密钥和证书
- MinIO现在不支持PFX证书

### <a name="using-go"></a>3.1 使用 generate_cert.go生成证书

下载 [`generate_cert.go`](https://golang.org/src/crypto/tls/generate_cert.go?m=text).

`generate_cert.go` 是一种简单的Go工具去生成自签名证书并且提供带有DNS和IP条目的SAN证书:

```sh
go run generate_cert.go -ca --host "10.10.0.3"
```

与之相似的响应会被展示为：
```sh
2018/11/21 10:16:18 wrote cert.pem
2018//11/21 10:16:18 wrote key.pem
```

将`cert.pem`重命名为`public.crt`, 并将`key.pem`命名为`private.key`。

### <a name="using-open-ssl"></a>3.2 使用 OpenSSL生成证书

使用下列的一个方法生成一个使用openssl的证书:
- 3.2.1 [生成具有ECDSA的私钥](#generate-private-key-with-ecdsa)
- 3.2.2 [生成具有RSA的私钥](#generate-private-key-with-rsa)
- 3.2.3 [生成一个具有自签名的证书](#generate-a-self-signed-certificate)

#### 3.2.1 <a name="generate-private-key-with-ecdsa"></a>生成ECDSA密钥

使用下列的命令去生成ECDSA私钥：
```sh
openssl ecparam -genkey -name prime256v1 | openssl ec -out private.key
```

与之相似的响应会被展示为:

```
read EC key
writing EC key
```

或者使用下面的命令生成被密码保护的私人ECDSA私钥：

```sh
openssl ecparam -genkey -name prime256v1 | openssl ec -aes256 -out private.key -passout pass:PASSWORD
```

**注意：** NIST curves P-384 和 P-521还没有被支持。

#### 3.2.2 <a name="generate-private-key-with-rsa"></a>生成RSA私钥

使用下列的命令生成私人RSA密钥：
```sh
openssl genrsa -out private.key 2048
```

一个与之相似的响应会被展示为：
```sh
Generating RSA private key, 2048 bit long modules
............................................+++
...........+++
e is 65537(0x10001)
```

或者使用下列的命令生成被密码保护的RSA私人密钥：
```sh
openssl genrsa -aes256 -out private.key 2048 -pass:PASSWORD
pass:PASSWORD
```

**注意：** 当使用被密码保护的私钥时，密码必须通过环境变量`MINIO_CERT_PASSWD`提供，可以用下面的命令实现：

```sh
export MINIO_CERT_PASSWD=<PASSWORD>
```

默认私人密钥的OpenSSL格式为PKCS-8，但是MinIO只支持PKCS-1。一个被置为PKCS-8的RSA密钥可以通过使用下面的命令被转换为PKCS-1：
```sh
openssl rsa -in private-pkcs8-key.key -aes256 -passout pass:PASSWORD -out private.key
```

#### <a name="generate-a-self-signed-certificate"></a>3.2.3 生成自签名证书

使用下面的命令生成自签名证书并在被提示时输入密码：

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=<domain.com>"
```

**注意：** 用开发所用的域名替换<`domain.com`>

或者可以使用下面的命令生成在<`domain.com`>的所有子域都有效的自签名的通配符证书。通配符证书对于部署分布式MinIO实例非常有用，其中每个实例都在单个父域下的子域上运行。

```sh
openssl req -new -x509 -days 3650 -key private.key -out public.crt -subj "/C=US/ST=state/L=location/O=organization/CN=<*.domain.com>"
```

### <a name="using-open-ssl-with-ip"></a>3.3 使用OpenSSL(具有IP地址)生成一个证书

这个部分描述如何在生成证书时为openssl指定一个IP地址。

#### 3.3.1 创建配置文件

创建一个名为`openssl.conf`并具有下面内容的文件。改变`IP.1`，使其指向正确的IP地址：

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

#### 3.3.2 运行openssl并指定配置文件:

```sh
openssl req -x509 -nodes -days 730 -newkey rsa:2048 -keyout private.key -out public.crt -config openssl.conf
```

### <a name="using-gnu-tls"></a>3.4 使用GnuTLS(Windows平台)生成证书

这个部分将介绍如何在Windows上使用GnuTLS来生成证书。

#### 3.4.1 安装并配置GnuTLS

从[这里](http://www.gnutls.org/download.html)下载并解压Windows版本的GnuTLS。

用PowerShell将解压后的binary路径加入到系统路径中。

```
setx path "%path%;C:\Users\MyUser\Downloads\gnutls-3.4.9-w64\bin"
```

**注意：** 你可能需要重启powershell控制台来使其生效。

#### 3.4.2 生成私人密钥

运行下面的命令来生成私人的`.key`文件：

```
certtool.exe --generate-privkey --outfile private.key 
```

与之相似的响应会被展示为：

```sh
Generating a 3072 bit RSA private key...
```


#### 3.4.3 生成公共证书

创建一个包含下面内容的文件`cert.cnf`，这个文件中包含了使用`certtools.exe`生成证书所需要的所有信息：

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

运行`certtools`并且指定配置文件生成证书：

```
certtool.exe --generate-self-signed --load-privkey private.key --template cert.cnf --outfile public.crt 
```

## <a name="install-certificates-from-third-party-cas"></a>4. 安装第三方CAs证书

MinIO可以配置成连接其它服务，包括MinIO节点或者其他像NATs、Redis的服务类型。如果这些服务用的不是在已知证书机构注册的证书，你可以通过将证书放在下列MinIO配置路径中的一个来向MinIO服务添加信任：
- **Linux:** `~/.minio/certs/CAs`
- **Windows:** `C:\Users\<Username>\.minio\certs\CAs`

# 了解更多
* [基于Kubernetes上MinIO服务的TLS配置](https://github.com/minio/minio/tree/master/docs/tls/kubernetes)
* [MinIO客户端权威指南](https://docs.min.io/cn/minio-client-complete-guide)
* [生成Let's Encrypt证书](https://docs.min.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio)
* [用MinIO服务建立niginx Proxy](https://docs.min.io/docs/setup-nginx-proxy-with-minio)