# Running Minio as a service on Windows

## Install Minio service

NSSM is an opensource alternative to srvany. NSSM is still being updated, and works on Windows 10 as well. 

Download [NSSM](http://nssm.cc/download) and extract the 64 bit nssm.exe to a known path. 

`
c:\nssm.exe install Minio c:\minio\bin\minio.exe server c:\data\
`

### Configuring startup type

When you start services, look for the Minio service and start and stop (or make it automatically start at reboots) the service. 

![](./screenshots/windows-configure-startup-type.png?raw=true)

### Configuring user

It is a good (and secure) practice to create a new user, assign rights to the data folder to this user and change the service Log On info to use the newly created user.

![](./screenshots/windows-configure-user.png?raw=true)

## Delete Minio service

`
c:\nssm.exe remove Minio
`
