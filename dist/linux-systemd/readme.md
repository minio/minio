# Readme
Service script for minio service for systemd.

# Installation
```
mkdir /etc/minio/
mkdir -p /usr/share/minio/bin/
wget -O /usr/share/minio/bin/minio http://...
chmod +x /usr/share/minio/bin/minio
```

Create minio user.
```
useradd minio
```

Create default configuration. Don't forget to update MINIO_VOLUMES with the correct path(s).
```
cat <<EOT >> /etc/default/minio
MINIO_OPTS="--address :9000"
MINIO_VOLUMES="/tmp/minio/"
EOT
```

# Systemctl

Put minio.service in /etc/systemd/system/
```
( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/minio/minio/master/dist/linux-systemd/minio.service )
```

Enable startup on boot
```
systemctl enable minio.service
```

