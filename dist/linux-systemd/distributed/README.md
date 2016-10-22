
- Systemd script is configured to run the binary from /usr/share/minio/bin/.

  `$ mkdir -p /usr/share/minio/bin/`

- Download the binary. Find the relevant links for the binary at https://minio.io/downloads/#minio-server.

  `$ wget -O /usr/share/minio/bin/minio https://dl.minio.io/server/minio/release/linux-amd64/minio`

- Give execute permission to the Minio binary. 

  `$ chmod +x /usr/share/minio/bin/minio`

- Create user minio. Systemd is configured with User=minio . 

  `$ useradd minio`
  
- Create the Environment configuration file. 

  `$ cat <<EOT >> /etc/default/minio.conf`
  
  ```
   MINIO_VOLUMES=node1:/tmp/drive1 node2:/drive1 node3:/tmp/drive1 minio4:/tmp/drive1
   
   MINIO_ACCESS_KEY=Server-Access-Key
   
   MINIO_SECRET_KEY=Server-Secret-Key
   
   EOT
   ```
- Download and put `minio.service` in  `/etc/systemd/system/`

  `$ ( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/minio/minio/master/dist/linux-systemd/distributed/minio.service )`

- Enable startup on boot.

  `$ systemctl enable minio.service`
   

  
  
