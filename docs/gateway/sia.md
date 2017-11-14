# Minio Sia Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio Sia Gateway adds Amazon S3 compatibility to Sia Decentralized Cloud Storage.

## What is Sia?
Sia is a blockchain-based decentralized storage service with built-in privacy and redundancy that costs up to 10x LESS than Amazon S3 and most other cloud providers! See [sia.tech](https://sia.tech) to learn how awesome Sia truly is.

## Getting Started with Sia

### Install Sia Daemon
To use Sia for backend storage, Minio will need access to a running Sia daemon that is:
1. fully synchronized with the Sia network,
2. has sufficient rental contract allowances, and
3. has an unlocked wallet.

To download and install Sia for your platform, visit [sia.tech](http://sia.tech).

To purchase inexpensive rental contracts with Sia, you have to possess some Siacoin in your wallet. To obtain Siacoin, you will need to purchase some on an exchange such as Bittrex using bitcoin. To obtain bitcoin, you'll need to use a service such as Coinbase to buy bitcoin using a bank account or credit card. If you need help, there are many friendly people active on [Sia's Slack](http://slackin.sia.tech).

### Configuration
Once you have the Sia Daemon running and synchronized, with rental allowances created, you just need to configure the Minio server to use Sia. Configuration is accomplished using environment variables, and is only necessary if the default values need to be changed. On a linux machine using bash shell, you can easily set environment variables by adding export statements to the "~/.bash_profile" file. For example:
```
export MY_ENV_VAR=VALUE
```
Just remember to reload the profile by executing: "source ~/.bash_profile" on the command prompt.

#### Supported Environment Variables
Environment Variable | Description | Default Value
--- | --- | ---
`MINIO_ACCESS_KEY` | The access key required to access Minio. | (empty)
`MINIO_SECRET_KEY` | The secret key required to access Minio. | (empty)
`SIA_TEMP_DIR` | The local directory to use for temporary storage. | .sia_temp
`SIA_API_PASSWORD` | The API password required to access the Sia daemon, if needed. | (empty)

### Running Minio with Sia Gateway
```
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway sia [SIA_DAEMON_ADDRESS]
```
The [SIA_DAEMON_ADDRESS] is optional, and it defaults to "127.0.0.1:9980".
Access information should then be presented on-screen. To connect to the server and upload files using your web browser, open a web browser and point it to the address displayed for "Browser Access." Then log in using the "AccessKey" and "SecretKey" that are also displayed on-screen. You should then be able to create buckets (folders) and upload files.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)

### Known limitations

Gateway inherits the following Sia limitations:

- Multipart uploads are not currently supported.
- Bucket policies are not currently supported.

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)

