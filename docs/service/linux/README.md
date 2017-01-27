# Running Minio as a service on Linux

## Install Minio service

Systemctl is a controller for systemd system and service manager. This document assumes operating system to be Ubuntu 16.04 (LTS) with `systemctl`.

Create a default minio startup config file at `/etc/default/minio`. `MINIO_VOLUMES` should be updated with the correct path.
```
cat <<EOT >> /etc/default/minio
# Local export path.
MINIO_VOLUMES="/mnt/export"
# Use if you want to run Minio on a custom port.
# MINIO_OPTS="--address :9001"

EOT
```

Optionally you can also override your Minio access credentials as shown below.
```
cat <<EOT >> /etc/default/minio
# Access key of the server.
MINIO_ACCESS_KEY=YOUR-ACCESSKEY
# Secret key of the server.
MINIO_SECRET_KEY=YOUR-SECRETKEY

EOT
```

Download `minio.service` into  `/etc/systemd/system/`
```
( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/minio/minio-systemd/master/minio.service )
```

## Enable Minio service

Once we have successfully copied the `minio.service` we will enable it to start on boot.
```
systemctl enable minio.service
```

## Disable Minio service
```
systemctl disable minio.service
```
