
- Systemd script is configured to run the binary from /usr/share/minio/bin/.
  ```sh
  $ mkdir -p /usr/share/minio/bin/
  ```

- Download the binary. Find the relevant links for the binary at https://minio.io/downloads/#minio-server.
  ```sh
  $ wget -O /usr/share/minio/bin/minio https://dl.minio.io/server/minio/release/linux-amd64/minio
  ```

- Give execute permission to the Minio binary. 
  ```sh
  $ chmod +x /usr/share/minio/bin/minio
  ```

- Create user minio. Systemd is configured with User=minio . 
  ```sh
  $ useradd minio
  ```
  
- Create the Environment configuration file. 
  ```sh
  $ cat <<EOT >> /etc/default/minio.conf
  ```

  ```
   # Remote node configuration. 
   MINIO_VOLUMES=node1:/tmp/drive1 node2:/drive1 node3:/tmp/drive1 minio4:/tmp/drive1
   # Use if you want to run Minio on a custom port.
   MINIO_OPTS="--address :9199" 
   # Acess Key of the server.
   MINIO_ACCESS_KEY=Server-Access-Key
   # Secret key of the server. 
   MINIO_SECRET_KEY=Server-Secret-Key
   
   EOT
   ```

- Download and put `minio.service` in  `/etc/systemd/system/`
  ```sh
  $ ( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/minio/minio/master/dist/linux-systemd/distributed/minio.service )
  ```
- Enable startup on boot.
  ```sh
  $ systemctl enable minio.service
  ```
   

  
  
