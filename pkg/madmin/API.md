# Golang Admin Client API Reference [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

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

| Service operations         | Info operations  | LockInfo operations         | Healing operations                    | Config operations         | Misc                                |
|:------------------------------------|:----------------------------|:----------------------------|:--------------------------------------|:--------------------------|:------------------------------------|
| [`ServiceStatus`](#ServiceStatus)   | [`ServerInfo`](#ServerInfo) | [`ListLocks`](#ListLocks)   | [`Heal`](#Heal)             | [`GetConfig`](#GetConfig) | [`SetCredentials`](#SetCredentials) |
| [`ServiceSendAction`](#ServiceSendAction) | | [`ClearLocks`](#ClearLocks) |            | [`SetConfig`](#SetConfig) |                                     |


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

## 2. Admin API Version

<a name="VersionInfo"></a>
### VersionInfo() (AdminAPIVersionInfo, error)
Fetch server's supported Administrative API version.

 __Example__

``` go

	info, err := madmClnt.VersionInfo()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%s\n", info.Version)

```

## 3. Service operations

<a name="ServiceStatus"></a>
### ServiceStatus() (ServiceStatusMetadata, error)
Fetch service status, replies disk space used, backend type and total disks offline/online (applicable in distributed mode).

| Param  | Type  | Description  |
|---|---|---|
|`serviceStatus`  | _ServiceStatusMetadata_  | Represents current server status info in following format: |


| Param  | Type  | Description  |
|---|---|---|
|`st.ServerVersion.Version`  | _string_  | Server version. |
|`st.ServerVersion.CommitID`  | _string_  | Server commit id. |
|`st.Uptime` | _time.Duration_ | Server uptime duration in seconds. |

 __Example__

 ```go

	st, err := madmClnt.ServiceStatus()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", st)

 ```

<a name="ServiceSendAction"></a>
### ServiceSendAction(act ServiceActionValue) (error)
Sends a service action command to service - possible actions are restarting and stopping the server.

 __Example__


 ```go
        // to restart
	st, err := madmClnt.ServiceSendAction(ServiceActionValueRestart)
        // or to stop
        // st, err := madmClnt.ServiceSendAction(ServiceActionValueStop)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Success")
 ```

## 4. Info operations

<a name="ServerInfo"></a>
### ServerInfo() ([]ServerInfo, error)
Fetches information for all cluster nodes, such as server properties, storage information, network statistics, etc.

| Param | Type | Description |
|---|---|---|
|`si.Addr` | _string_ | Address of the server the following information is retrieved from. |
|`si.ConnStats` | _ServerConnStats_ | Connection statistics from the given server. |
|`si.HTTPStats` | _ServerHTTPStats_ | HTTP connection statistics from the given server. |
|`si.Properties` | _ServerProperties_ | Server properties such as region, notification targets. |
|`si.Data.StorageInfo.Total`  | _int64_  | Total disk space. |
|`si.Data.StorageInfo.Free`  | _int64_  | Free disk space. |
|`si.Data.StorageInfo.Backend`| _struct{}_ | Represents backend type embedded structure. |

| Param | Type | Description |
|---|---|---|
|`ServerProperties.Uptime`| _time.Duration_ | Total duration in seconds since server is running. |
|`ServerProperties.Version`| _string_ | Current server version. |
|`ServerProperties.CommitID` | _string_ | Current server commitID. |
|`ServerProperties.Region` | _string_ | Configured server region. |
|`ServerProperties.SQSARN` | _[]string_ | List of notification target ARNs. |

| Param | Type | Description |
|---|---|---|
|`ServerConnStats.TotalInputBytes` | _uint64_ | Total bytes received by the server. |
|`ServerConnStats.TotalOutputBytes` | _uint64_ | Total bytes sent by the server. |

| Param | Type | Description |
|---|---|---|
|`ServerHTTPStats.TotalHEADStats`| _ServerHTTPMethodStats_ | Total statistics regarding HEAD operations |
|`ServerHTTPStats.SuccessHEADStats`| _ServerHTTPMethodStats_ | Total statistics regarding successful HEAD operations |
|`ServerHTTPStats.TotalGETStats`| _ServerHTTPMethodStats_ |  Total statistics regarding GET operations |
|`ServerHTTPStats.SuccessGETStats`| _ServerHTTPMethodStats_ | Total statistics regarding successful GET operations |
|`ServerHTTPStats.TotalPUTStats`| _ServerHTTPMethodStats_ | Total statistics regarding PUT operations |
|`ServerHTTPStats.SuccessPUTStats`| _ServerHTTPMethodStats_ | Total statistics regarding successful PUT operations |
|`ServerHTTPStats.TotalPOSTStats`| _ServerHTTPMethodStats_ | Total statistics regarding POST operations |
|`ServerHTTPStats.SuccessPOSTStats`| _ServerHTTPMethodStats_ | Total statistics regarding successful POST operations |
|`ServerHTTPStats.TotalDELETEStats`| _ServerHTTPMethodStats_ | Total statistics regarding DELETE operations |
|`ServerHTTPStats.SuccessDELETEStats`| _ServerHTTPMethodStats_ | Total statistics regarding successful DELETE operations |


| Param | Type | Description |
|---|---|---|
|`ServerHTTPMethodStats.Count` | _uint64_ | Total number of operations. |
|`ServerHTTPMethodStats.AvgDuration` | _string_ | Average duration of Count number of operations. |

| Param | Type | Description |
|---|---|---|
|`Backend.Type` | _BackendType_ | Type of backend used by the server currently only FS or Erasure. |
|`Backend.OnlineDisks`| _int_ | Total number of disks online (only applies to Erasure backend), is empty for FS. |
|`Backend.OfflineDisks` | _int_ | Total number of disks offline (only applies to Erasure backend), is empty for FS. |
|`Backend.StandardSCData` | _int_ | Data disks set for standard storage class, is empty for FS. |
|`Backend.StandardSCParity` | _int_ | Parity disks set for standard storage class, is empty for FS. |
|`Backend.RRSCData` | _int_ | Data disks set for reduced redundancy storage class, is empty for FS. |
|`Backend.RRSCParity` | _int_ | Parity disks set for reduced redundancy storage class, is empty for FS. |
|`Backend.Sets` | _[][]DriveInfo_ | Represents topology of drives in erasure coded sets. |

| Param | Type | Description |
|---|---|---|
|`DriveInfo.UUID`| _string_ | Unique ID for each disk provisioned by server format. |
|`DriveInfo.Endpoint` | _string_ | Endpoint location of the remote/local disk. |
|`DriveInfo.State` | _string_ | Current state of the disk at endpoint. |

 __Example__

 ```go

	serversInfo, err := madmClnt.ServerInfo()
	if err != nil {
		log.Fatalln(err)
	}

	for _, peerInfo := range serversInfo {
		log.Printf("Node: %s, Info: %v\n", peerInfo.Addr, peerInfo.Data)
	}

 ```


## 5. Lock operations

<a name="ListLocks"></a>
### ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error)
If successful returns information on the list of locks held on ``bucket`` matching ``prefix`` for  longer than ``duration`` seconds.

__Example__

``` go
    volLocks, err := madmClnt.ListLocks("mybucket", "myprefix", 30 * time.Second)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("List of locks: ", volLocks)

```

<a name="ClearLocks"></a>
### ClearLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error)
If successful returns information on the list of locks cleared on ``bucket`` matching ``prefix`` for longer than ``duration`` seconds.

__Example__

``` go
    volLocks, err := madmClnt.ClearLocks("mybucket", "myprefix", 30 * time.Second)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("List of locks cleared: ", volLocks)

```

## 6. Heal operations

<a name="Heal"></a>
### Heal(bucket, prefix string, healOpts HealOpts, clientToken string, forceStart bool) (start HealStartSuccess, status HealTaskStatus, err error)

Start a heal sequence that scans data under given (possible empty)
`bucket` and `prefix`. The `recursive` bool turns on recursive
traversal under the given path. `dryRun` does not mutate on-disk data,
but performs data validation. `incomplete` enables healing of
multipart uploads that are in progress. `removeBadFiles` removes
unrecoverable files. `statisticsOnly` turns off detailed
heal-operations reporting in the status call.

Two heal sequences on overlapping paths may not be initiated.

The progress of a heal should be followed using the `HealStatus`
API. The server accumulates results of the heal traversal and waits
for the client to receive and acknowledge them using the status
API. When the statistics-only option is set, the server only maintains
aggregates statistics - in this case, no acknowledgement of results is
required.

__Example__

``` go

    healPath, err := madmClnt.HealStart("", "", true, false, true, false, false)
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("Heal sequence started at %s", healPath)

```

#### HealTaskStatus structure

| Param | Type | Description |
|----|--------|--------|
| s.Summary | _string_ | Short status of heal sequence |
| s.FailureDetail | _string_ | Error message in case of heal sequence failure |
| s.HealSettings | _HealOpts_ | Contains the booleans set in the `HealStart` call |
| s.Items | _[]HealResultItem_ | Heal records for actions performed by server |
| s.Statistics | _HealStatistics_ | Aggregate of heal records from beginning |

#### HealResultItem structure

| Param | Type | Description |
|------|-------|---------|
| ResultIndex | _int64_ | Index of the heal-result record |
| Type | _HealItemType_ | Represents kind of heal operation in the heal record |
| Bucket | _string_ | Bucket name |
| Object | _string_ | Object name |
| Detail | _string_ | Details about heal operation |
| DiskInfo.AvailableOn | _[]int_ | List of disks on which the healed entity is present and healthy |
| DiskInfo.HealedOn | _[]int_ | List of disks on which the healed entity was restored |

#### HealStatistics structure

Most parameters represent the aggregation of heal operations since the
start of the heal sequence.

| Param | Type | Description |
|-------|-----|----------|
| NumDisks | _int_ | Number of disks configured in the backend |
| NumBucketsScanned | _int64_ | Number of buckets scanned |
| BucketsMissingByDisk | _map[int]int64_ | Map of disk to number of buckets missing |
| BucketsAvailableByDisk | _map[int]int64_ | Map of disk to number of buckets available |
| BucketsHealedByDisk | _map[int]int64_ | Map of disk to number of buckets healed on |
| NumObjectsScanned | _int64_ | Number of objects scanned |
| NumUploadsScanned | _int64_ | Number of uploads scanned |
| ObjectsByAvailablePC | _map[int64]_ | Map of available part counts (after heal) to number of objects |
| ObjectsByHealedPC | _map[int64]_ | Map of healed part counts to number of objects |
| ObjectsMissingByDisk | _map[int64]_ | Map of disk number to number of objects with parts missing on that disk |
| ObjectsAvailableByDisk | _map[int64]_ | Map of disk number to number of objects available on that disk |
| ObjectsHealedByDisk | _map[int64]_ | Map of disk number to number of objects healed on that disk |

__Example__

``` go

    res, err := madmClnt.HealStatus("", "")
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("Heal sequence status data %#v", res)

```

## 7. Config operations

<a name="GetConfig"></a>
### GetConfig() ([]byte, error)
Get config.json of a minio setup.

__Example__

``` go
    configBytes, err := madmClnt.GetConfig()
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    // Pretty-print config received as json.
    var buf bytes.Buffer
    err = json.Indent(buf, configBytes, "", "\t")
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    log.Println("config received successfully: ", string(buf.Bytes()))
```


<a name="SetConfig"></a>
### SetConfig(config io.Reader) (SetConfigResult, error)
Set config.json of a minio setup and restart setup for configuration
change to take effect.


| Param  | Type  | Description  |
|---|---|---|
|`st.Status`            | _bool_  | true if set-config succeeded, false otherwise. |
|`st.NodeSummary.Name`  | _string_  | Network address of the node. |
|`st.NodeSummary.ErrSet`   | _bool_ | Bool representation indicating if an error is encountered with the node.|
|`st.NodeSummary.ErrMsg`   | _string_ | String representation of the error (if any) on the node.|


__Example__

``` go
    config := bytes.NewReader([]byte(`config.json contents go here`))
    result, err := madmClnt.SetConfig(config)
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    var buf bytes.Buffer
    enc := json.NewEncoder(&buf)
    enc.SetEscapeHTML(false)
    enc.SetIndent("", "\t")
    err = enc.Encode(result)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("SetConfig: ", string(buf.Bytes()))
```

## 8. Misc operations

<a name="SetCredentials"></a>
### SetCredentials() error
Set new credentials of a Minio setup.

__Example__

``` go
    err = madmClnt.SetCredentials("YOUR-NEW-ACCESSKEY", "YOUR-NEW-SECRETKEY")
    if err != nil {
            log.Fatalln(err)
    }
    log.Println("New credentials successfully set.")

```
