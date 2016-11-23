# Running Minio as a service on Windows

## Install Minio service

Download and install the [Windows Server 2003 Resource Kit Tools](https://www.microsoft.com/en-us/download/details.aspx?id=17657). It 
is old, but still works on recent Windows versions. On the latest Windows versions you'll get a warning message, but you can run
it anyhow by clicking on "Run the program without getting help".

Start cmd.exe and install the Minio service:

```
sc create Minio binPath="C:\Program Files (x86)\Windows Resource Kits\Tools\srvany.exe" DisplayName="Minio Cloudstorage"
```

Now we need to configure srvany using regedit. Start regedit and go to HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services\Minio and
create a new key called Parameters. 

![](./screenshots/windows-configure-registry.png?raw=true)

Within the new key create a new String value called Application and enter the path of your Minio binary. (eg. C:\Minio\bin\minio.exe) And create
another String value called AppParameters with value `server c:\data` where c:\data will be the location of the data folder.

When you start services, look for the Minio service and start and stop (or make it automatically start at reboots) the service. 

![](./screenshots/windows-configure-startup-type.png?raw=true)

It is a good (and secure) practice to create a new user, assign rights to the data folder to this user and change the service Log On info to use the newly created user.

![](./screenshots/windows-configure-user.png?raw=true)

## Delete Minio service

If you want to remove the Minio service, just enter the following command from the command line.

```
sc delete Minio
```
