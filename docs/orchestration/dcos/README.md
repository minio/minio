## Prerequisites

- DC/OS 1.8 or later
- [Marathon-LB](https://dcos.io/docs/1.8/usage/service-discovery/marathon-lb/usage/) must be installed and running
- IP address of the public agent(s) where Marathon-LB or an available hostname configured to point to the public agent(s) where Marathon-LB is running.

## Setting up Minio 

Before starting, identify the IP address or hostname of a public agent where Marathon-LB is running. 

- Visit the Universe page in DC/OS, and click on the "Install Package" button underneath Minio.

![Install Minio](img/install.png)

- Click on "Advanced Installation" and navigate to the "networking" tab. Specify the IP address or hostname of the public agent where Marathon-LB is running. Make sure you remove the leading http:// and the trailing / from the IP. 

![Configure IP](img/ip.png)

- We're ready to install! Click the green "Review and Install" button, verify your settings are correct and then click "Install". Navigate to the services UI to see Minio being deployed.
 
- Once Minio has been deployed, navigate to the IP/hostname you used earlier for virtual host. You should see the following login page.

![Minio browser](img/browser.png)

## Using Browser Console

- The access key and secret key for the browser console can be obtained from minio service logs.

![Minio browser](img/logs.png)

- Navigate to services UI and click on Minio and go to the logs section. Copy the _AccessKey_ and _SecretKey_ and use it to log into the browser console.

![Minio browser](img/use-keys.png)

- Once you have successfully loggedin you should see the following screen. 

![Minio browser](img/home.png)

## Install Minio Client

Minio Client (mc) is a CLI tool which provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc, to operate on filesystems and Amazon S3 compatible cloud storage service.

[Click here](https://docs.minio.io/docs/minio-client-quickstart-guide) for instructions on installing mc.

## Configure Minio Client

```sh
mc config host add <ALIAS> <YOUR-S3-ENDPOINT> <YOUR-ACCESS-KEY> <YOUR-SECRET-KEY> <API-SIGNATURE>
```

*Example: Create a new bucket named "my-bucket" on http://52.53.213.170:9000*

```sh
mc config host add minio-dcos http://52.53.213.179:9000 2TT97MX8MWWZGCBWQULV mdXXJwo0bxO7XUfOuOMaUu255u0QKYsddEXjVBzd
mc mb minio-dcos/my-bucket
Bucket created successfully ‘minio-dcos/my-bucket’.
```
