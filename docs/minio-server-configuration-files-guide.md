# Minio Server configuration files Guide [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

In this document we will walk through the configuration files of Minio Server.

## Minio Server configuration directory
Minio Server configurations are stored in file name ``.minio``.  It's a hidden file which resides on user's home directory.

**This how the structure of the directory looks like:**

```sh
$ tree ~/.minio/
/Users/supernova/.minio/
├── certs
├── config.json
└── config.json.old

1 directory, 2 files
```
### Files and directories.

##### ``certs`` directory 
``certs`` directory stores key & cert information, which are needed to run Minio in ``HTTPS``. You can read more on running Minio with ``HTTPS`` with Let's Encrypt cert with Concert [here](https://docs.minio.io/docs/generate-let-s-encypt-certificate-using-concert-for-minio) 

##### ``config.json``
config.json is the configuration file for Minio, it gets generated after you install and start Minio.

```sh

$ cat config.json
{
	"version": "6",
	"credential": {
		"accessKey": "YI7S1CKXB76RGOGT6R8W",
		"secretKey": "FJ9PWUVNXGPfiI72WMRFepN3LsFgW3MjsxSALroV"
	},
	"region": "us-east-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "fatal"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": "error"
		},
		"syslog": {
			"enable": false,
			"address": "",
			"level": "debug"
		}
	},
	"notify": {
		"amqp": {
			"1": {
				"enable": false,
				"url": "",
				"exchange": "",
				"routineKey": "",
				"exchangeType": "",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
		"elasticsearch": {
			"1": {
				"enable": false,
				"url": "",
				"index": ""
			}
		},
		"redis": {
			"1": {
				"enable": false,
				"address": "",
				"password": "",
				"key": ""
			}
		}
	}
}


```

``version`` :  Represents `version` number of current configuration file.

``credential`` :  Represents authentication credentials for the server, value is automatically generated upon first server start.

``region`` :  Represents deployment region for the server,  value defaults to `us-east-1`. 

``logger `` : Represents various logging types supported for server error logs, console logger is enabled by default.

``notify``:  Represents various notification types supported. These notification types should be configured prior to using bucket


##### ``config.json.old``
This file keeps previous config file version details.

## Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)





