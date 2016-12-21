# Golang Admin Client API Reference [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Initialize Minio Admin Client object.

##  Minio

```go

package main

import (
	"fmt"

	"github.com/minio/minio/pkg/madmin"
)

func main() {
	// Use a secure connection.
	ssl := true

	// Initialize minio client object.
	mdmClnt, err := madmin.New("your-minio.example.com:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETKEY", ssl)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Fetch service status.
	st, err := mdmClnt.ServiceStatus()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%#v\n", st)
}

```

| Service operations|LockInfo operations|Healing operations|
|:---|:---|:---|
|[`ServiceStatus`](#ServiceStatus)| | |
|[`ServiceStop`](#ServiceStop)| | |
|[`ServiceRestart`](#ServiceRestart)| | |

## 1. Constructor
<a name="Minio"></a>

### New(endpoint string, accessKeyID string, secretAccessKey string, ssl bool) (*AdminClient, error)
Initializes a new admin client object.

__Parameters__


|Param   |Type   |Description   |
|:---|:---| :---|
|`endpoint`   | _string_  |Minio endpoint.   |
|`accessKeyID`  |_string_   | Access key for the object storage endpoint.  |
|`secretAccessKey`  | _string_  |Secret key for the object storage endpoint.   |
|`ssl`   | _bool_  | Set this value to 'true' to enable secure (HTTPS) access.  |


## 2. Service operations

<a name="ServiceStatus"></a>
### ServiceStatus() (ServiceStatusMetadata, error)
Fetch service status, replies disk space used, backend type and total disks offline/online (XL). 

| Param  | Type  | Description  |
|---|---|---|
|`serviceStatus`  | _ServiceStatusMetadata_  | Represents current server status info in following format: | 


| Param  | Type  | Description  |
|---|---|---|
|`st.Total`  | _int64_  | Total disk space. | 
|`st.Free`  | _int64_  | Free disk space. |
|`st.Backend`| _struct{}_ | Represents backend type embedded structure. |

| Param | Type | Description |
|---|---|---|
|`backend.Type` | _BackendType_ | Type of backend used by the server currently only FS or XL. |
|`backend.OnlineDisks`| _int_ | Total number of disks online (only applies to XL backend), is empty for FS. |
|`backend.OfflineDisks` | _int_ | Total number of disks offline (only applies to XL backend), is empty for FS. |
|`backend.ReadQuorum` | _int_ | Current total read quorum threshold before reads will be unavailable, is empty for FS. |
|`backend.WriteQuorum` | _int_ | Current total write quorum threshold before writes will be unavailable, is empty for FS. |


 __Example__

 
 ```go

	st, err := madmClnt.ServiceStatus()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", st)

 ```

<a name="ServiceStop"></a>
### ServiceStop() (error)
If successful shuts down the running minio service, for distributed setup stops all remote minio servers.

 __Example__

 
 ```go

	st, err := madmClnt.ServiceStop()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Succes")

 ```

<a name="ServiceRestart"></a>
### ServiceRestart() (error)
If successful restarts the running minio service, for distributed setup restarts all remote minio servers.

 __Example__

 
 ```go


	st, err := madmClnt.ServiceRestart()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Succes")

 ```

