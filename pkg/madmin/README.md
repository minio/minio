# Golang Admin Client API Reference [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
The MinIO Admin Golang Client SDK provides APIs to manage MinIO services.

This quickstart guide will show you how to install the MinIO Admin client SDK, connect to MinIO admin service, and provide a walkthrough of a simple file uploader.

This document assumes that you have a working [Golang setup](https://docs.min.io/docs/how-to-install-golang).

## Initialize MinIO Admin Client object.

##  MinIO

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

| Service operations                        | Info operations                             | Healing operations | Config operations                 | Top operations          | IAM operations                        | Misc                                              |
|:------------------------------------------|:--------------------------------------------|:-------------------|:----------------------------------|:------------------------|:--------------------------------------|:--------------------------------------------------|
| [`ServiceStatus`](#ServiceStatus)         | [`ServerInfo`](#ServerInfo)                 | [`Heal`](#Heal)    | [`GetConfig`](#GetConfig)         | [`TopLocks`](#TopLocks) | [`AddUser`](#AddUser)                 |                                                   |
| [`ServiceSendAction`](#ServiceSendAction) | [`ServerCPULoadInfo`](#ServerCPULoadInfo)   |                    | [`SetConfig`](#SetConfig)         |                         | [`SetUserPolicy`](#SetUserPolicy)     | [`StartProfiling`](#StartProfiling)               |
|                                           | [`ServerMemUsageInfo`](#ServerMemUsageInfo) |                    | [`GetConfigKeys`](#GetConfigKeys) |                         | [`ListUsers`](#ListUsers)             | [`DownloadProfilingData`](#DownloadProfilingData) |
|                                           |                                             |                    | [`SetConfigKeys`](#SetConfigKeys) |                         | [`AddCannedPolicy`](#AddCannedPolicy) |                                                   |


## 1. Constructor
<a name="MinIO"></a>

### New(endpoint string, accessKeyID string, secretAccessKey string, ssl bool) (*AdminClient, error)
Initializes a new admin client object.

__Parameters__

| Param             | Type     | Description                                               |
|:------------------|:---------|:----------------------------------------------------------|
| `endpoint`        | _string_ | MinIO endpoint.                                           |
| `accessKeyID`     | _string_ | Access key for the object storage endpoint.               |
| `secretAccessKey` | _string_ | Secret key for the object storage endpoint.               |
| `ssl`             | _bool_   | Set this value to 'true' to enable secure (HTTPS) access. |

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

| Param           | Type                    | Description                                                |
|-----------------|-------------------------|------------------------------------------------------------|
| `serviceStatus` | _ServiceStatusMetadata_ | Represents current server status info in following format: |


| Param                       | Type            | Description                        |
|-----------------------------|-----------------|------------------------------------|
| `st.ServerVersion.Version`  | _string_        | Server version.                    |
| `st.ServerVersion.CommitID` | _string_        | Server commit id.                  |
| `st.Uptime`                 | _time.Duration_ | Server uptime duration in seconds. |

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

| Param                           | Type               | Description                                                        |
|---------------------------------|--------------------|--------------------------------------------------------------------|
| `si.Addr`                       | _string_           | Address of the server the following information is retrieved from. |
| `si.ConnStats`                  | _ServerConnStats_  | Connection statistics from the given server.                       |
| `si.HTTPStats`                  | _ServerHTTPStats_  | HTTP connection statistics from the given server.                  |
| `si.Properties`                 | _ServerProperties_ | Server properties such as region, notification targets.            |
| `si.Data.StorageInfo.Used`      | _int64_            | Used disk space.                                                   |
| `si.Data.StorageInfo.Total`     | _int64_            | Total disk space.                                                  |
| `si.Data.StorageInfo.Available` | _int64_            | Available disk space.                                              |
| `si.Data.StorageInfo.Backend`   | _struct{}_         | Represents backend type embedded structure.                        |

| Param                       | Type            | Description                                        |
|-----------------------------|-----------------|----------------------------------------------------|
| `ServerProperties.Uptime`   | _time.Duration_ | Total duration in seconds since server is running. |
| `ServerProperties.Version`  | _string_        | Current server version.                            |
| `ServerProperties.CommitID` | _string_        | Current server commitID.                           |
| `ServerProperties.Region`   | _string_        | Configured server region.                          |
| `ServerProperties.SQSARN`   | _[]string_      | List of notification target ARNs.                  |

| Param                              | Type     | Description                         |
|------------------------------------|----------|-------------------------------------|
| `ServerConnStats.TotalInputBytes`  | _uint64_ | Total bytes received by the server. |
| `ServerConnStats.TotalOutputBytes` | _uint64_ | Total bytes sent by the server.     |

| Param                                | Type                    | Description                                             |
|--------------------------------------|-------------------------|---------------------------------------------------------|
| `ServerHTTPStats.TotalHEADStats`     | _ServerHTTPMethodStats_ | Total statistics regarding HEAD operations              |
| `ServerHTTPStats.SuccessHEADStats`   | _ServerHTTPMethodStats_ | Total statistics regarding successful HEAD operations   |
| `ServerHTTPStats.TotalGETStats`      | _ServerHTTPMethodStats_ | Total statistics regarding GET operations               |
| `ServerHTTPStats.SuccessGETStats`    | _ServerHTTPMethodStats_ | Total statistics regarding successful GET operations    |
| `ServerHTTPStats.TotalPUTStats`      | _ServerHTTPMethodStats_ | Total statistics regarding PUT operations               |
| `ServerHTTPStats.SuccessPUTStats`    | _ServerHTTPMethodStats_ | Total statistics regarding successful PUT operations    |
| `ServerHTTPStats.TotalPOSTStats`     | _ServerHTTPMethodStats_ | Total statistics regarding POST operations              |
| `ServerHTTPStats.SuccessPOSTStats`   | _ServerHTTPMethodStats_ | Total statistics regarding successful POST operations   |
| `ServerHTTPStats.TotalDELETEStats`   | _ServerHTTPMethodStats_ | Total statistics regarding DELETE operations            |
| `ServerHTTPStats.SuccessDELETEStats` | _ServerHTTPMethodStats_ | Total statistics regarding successful DELETE operations |


| Param                               | Type     | Description                                     |
|-------------------------------------|----------|-------------------------------------------------|
| `ServerHTTPMethodStats.Count`       | _uint64_ | Total number of operations.                     |
| `ServerHTTPMethodStats.AvgDuration` | _string_ | Average duration of Count number of operations. |

| Param                      | Type            | Description                                                                       |
|----------------------------|-----------------|-----------------------------------------------------------------------------------|
| `Backend.Type`             | _BackendType_   | Type of backend used by the server currently only FS or Erasure.                  |
| `Backend.OnlineDisks`      | _int_           | Total number of disks online (only applies to Erasure backend), is empty for FS.  |
| `Backend.OfflineDisks`     | _int_           | Total number of disks offline (only applies to Erasure backend), is empty for FS. |
| `Backend.StandardSCData`   | _int_           | Data disks set for standard storage class, is empty for FS.                       |
| `Backend.StandardSCParity` | _int_           | Parity disks set for standard storage class, is empty for FS.                     |
| `Backend.RRSCData`         | _int_           | Data disks set for reduced redundancy storage class, is empty for FS.             |
| `Backend.RRSCParity`       | _int_           | Parity disks set for reduced redundancy storage class, is empty for FS.           |
| `Backend.Sets`             | _[][]DriveInfo_ | Represents topology of drives in erasure coded sets.                              |

| Param                | Type     | Description                                           |
|----------------------|----------|-------------------------------------------------------|
| `DriveInfo.UUID`     | _string_ | Unique ID for each disk provisioned by server format. |
| `DriveInfo.Endpoint` | _string_ | Endpoint location of the remote/local disk.           |
| `DriveInfo.State`    | _string_ | Current state of the disk at endpoint.                |

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

<a name="ServerDrivesPerfInfo"></a>
### ServerDrivesPerfInfo() ([]ServerDrivesPerfInfo, error)

Fetches drive performance information for all cluster nodes. Returned value is in Bytes/s.

| Param           | Type               | Description                                                        |
|-----------------|--------------------|--------------------------------------------------------------------|
| `di.Addr`       | _string_           | Address of the server the following information is retrieved from. |
| `di.Error`      | _string_           | Errors (if any) encountered while reaching this node               |
| `di.DrivesPerf` | _disk.Performance_ | Path of the drive mount on above server and read, write speed.     |

| Param                         | Type      | Description                                            |
|-------------------------------|-----------|--------------------------------------------------------|
| `disk.Performance.Path`       | _string_  | Path of drive mount.                                   |
| `disk.Performance.Error`      | _string_  | Error (if any) encountered while accessing this drive. |
| `disk.Performance.WriteSpeed` | _float64_ | Write speed on above path in Bytes/s.                  |
| `disk.Performance.ReadSpeed`  | _float64_ | Read speed on above path in Bytes/s.                   |

<a name="ServerCPULoadInfo"></a>
### ServerCPULoadInfo() ([]ServerCPULoadInfo, error)

Fetches CPU utilization for all cluster nodes. Returned value is in Bytes.

| Param          | Type       | Description                                                         |
|----------------|------------|---------------------------------------------------------------------|
| `cpui.Addr`    | _string_   | Address of the server the following information  is retrieved from. |
| `cpui.Error`   | _string_   | Errors (if any) encountered while reaching this node                |
| `cpui.CPULoad` | _cpu.Load_ | The load on the CPU.                                                |

| Param            | Type      | Description                                                     |
|------------------|-----------|-----------------------------------------------------------------|
| `cpu.Load.Avg`   | _float64_ | The average utilization of the CPU measured in a 200ms interval |
| `cpu.Load.Min`   | _float64_ | The minimum utilization of the CPU measured in a 200ms interval |
| `cpu.Load.Max`   | _float64_ | The maximum utilization of the CPU measured in a 200ms interval |
| `cpu.Load.Error` | _string_  | Error (if any) encountered while accesing the CPU info          |

<a name="ServerMemUsageInfo"></a>
### ServerMemUsageInfo() ([]ServerMemUsageInfo, error)

Fetches Mem utilization for all cluster nodes. Returned value is in Bytes.

| Param           | Type        | Description                                                         |
|-----------------|-------------|---------------------------------------------------------------------|
| `memi.Addr`     | _string_    | Address of the server the following information  is retrieved from. |
| `memi.Error`    | _string_    | Errors (if any) encountered while reaching this node                |
| `memi.MemUsage` | _mem.Usage_ | The utilitzation of Memory                                          |

| Param             | Type     | Description                                            |
|-------------------|----------|--------------------------------------------------------|
| `mem.Usage.Mem`   | _uint64_ | The total number of bytes obtained from the OS         |
| `mem.Usage.Error` | _string_ | Error (if any) encountered while accesing the CPU info |

## 6. Heal operations

<a name="Heal"></a>
### Heal(bucket, prefix string, healOpts HealOpts, clientToken string, forceStart bool, forceStop bool) (start HealStartSuccess, status HealTaskStatus, err error)

Start a heal sequence that scans data under given (possible empty)
`bucket` and `prefix`. The `recursive` bool turns on recursive
traversal under the given path. `dryRun` does not mutate on-disk data,
but performs data validation.

Two heal sequences on overlapping paths may not be initiated.

The progress of a heal should be followed using the same API `Heal`
by providing the `clientToken` previously obtained from a `Heal`
API. The server accumulates results of the heal traversal and waits
for the client to receive and acknowledge them using the status
request by providing `clientToken`.

__Example__

``` go

    opts := madmin.HealOpts{
            Recursive: true,
            DryRun:    false,
    }
    forceStart := false
    forceStop := false
    healPath, err := madmClnt.Heal("", "", opts, "", forceStart, forceStop)
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("Heal sequence started at %s", healPath)

```

#### HealStartSuccess structure

| Param             | Type        | Description                                                                                                                      |
|-------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------|
| `s.ClientToken`   | _string_    | A unique token for a successfully started heal operation, this token is used to request realtime progress of the heal operation. |
| `s.ClientAddress` | _string_    | Address of the client which initiated the heal operation, the client address has the form "host:port".                           |
| `s.StartTime`     | _time.Time_ | Time when heal was initially started.                                                                                            |

#### HealTaskStatus structure

| Param             | Type               | Description                                       |
|-------------------|--------------------|---------------------------------------------------|
| `s.Summary`       | _string_           | Short status of heal sequence                     |
| `s.FailureDetail` | _string_           | Error message in case of heal sequence failure    |
| `s.HealSettings`  | _HealOpts_         | Contains the booleans set in the `HealStart` call |
| `s.Items`         | _[]HealResultItem_ | Heal records for actions performed by server      |

#### HealResultItem structure

| Param                  | Type           | Description                                                     |
|------------------------|----------------|-----------------------------------------------------------------|
| `ResultIndex`          | _int64_        | Index of the heal-result record                                 |
| `Type`                 | _HealItemType_ | Represents kind of heal operation in the heal record            |
| `Bucket`               | _string_       | Bucket name                                                     |
| `Object`               | _string_       | Object name                                                     |
| `Detail`               | _string_       | Details about heal operation                                    |
| `DiskInfo.AvailableOn` | _[]int_        | List of disks on which the healed entity is present and healthy |
| `DiskInfo.HealedOn`    | _[]int_        | List of disks on which the healed entity was restored           |

## 7. Config operations

<a name="GetConfig"></a>
### GetConfig() ([]byte, error)
Get current `config.json` of a MinIO server.

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
### SetConfig(config io.Reader) error
Set a new `config.json` for a MinIO server.

__Example__

``` go
    config := bytes.NewReader([]byte(`config.json contents go here`))
    if err := madmClnt.SetConfig(config); err != nil {
        log.Fatalf("failed due to: %v", err)
    }
    log.Println("SetConfig was successful")
```

<a name="GetConfigKeys"></a>
### GetConfigKeys(keys []string) ([]byte, error)
Get a json document which contains a set of keys and their values from config.json.

__Example__

``` go
    configBytes, err := madmClnt.GetConfigKeys([]string{"version", "notify.amqp.1"})
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


<a name="SetConfigKeys"></a>
### SetConfigKeys(params map[string]string) error
Set a set of keys and values for MinIO server or distributed setup and restart the MinIO
server for the new configuration changes to take effect.

__Example__

``` go
    err := madmClnt.SetConfigKeys(map[string]string{"notify.webhook.1": "{\"enable\": true, \"endpoint\": \"http://example.com/api\"}"})
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    log.Println("New configuration successfully set")
```

## 8. Top operations

<a name="TopLocks"></a>
### TopLocks() (LockEntries, error)
Get the oldest locks from MinIO server.

__Example__

``` go
    locks, err := madmClnt.TopLocks()
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    out, err := json.Marshal(locks)
    if err != nil {
        log.Fatalf("Marshal failed due to: %v", err)
    }

    log.Println("TopLocks received successfully: ", string(out))
```

## 9. IAM operations

<a name="AddCannedPolicy"></a>
### AddCannedPolicy(policyName string, policy string) error
Create a new canned policy on MinIO server.

__Example__

```
	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Resource": ["arn:aws:s3:::my-bucketname/*"],"Sid": ""}]}`

    if err = madmClnt.AddCannedPolicy("get-only", policy); err != nil {
		log.Fatalln(err)
	}
```

<a name="AddUser"></a>
### AddUser(user string, secret string) error
Add a new user on a MinIO server.

__Example__

``` go
	if err = madmClnt.AddUser("newuser", "newstrongpassword"); err != nil {
		log.Fatalln(err)
	}
```

<a name="SetUserPolicy"></a>
### SetUserPolicy(user string, policyName string) error
Enable a canned policy `get-only` for a given user on MinIO server.

__Example__

``` go
	if err = madmClnt.SetUserPolicy("newuser", "get-only"); err != nil {
		log.Fatalln(err)
	}
```

<a name="ListUsers"></a>
### ListUsers() (map[string]UserInfo, error)
Lists all users on MinIO server.

__Example__

``` go
	users, err := madmClnt.ListUsers();
    if err != nil {
		log.Fatalln(err)
	}
    for k, v := range users {
        fmt.Printf("User %s Status %s\n", k, v.Status)
    }
```

## 10. Misc operations

<a name="StartProfiling"></a>
### StartProfiling(profiler string) error
Ask all nodes to start profiling using the specified profiler mode

__Example__

``` go
    startProfilingResults, err = madmClnt.StartProfiling("cpu")
    if err != nil {
            log.Fatalln(err)
    }
    for _, result := range startProfilingResults {
        if !result.Success {
            log.Printf("Unable to start profiling on node `%s`, reason = `%s`\n", result.NodeName, result.Error)
        } else {
            log.Printf("Profiling successfully started on node `%s`\n", result.NodeName)
        }
    }

```

<a name="DownloadProfilingData"></a>
### DownloadProfilingData() ([]byte, error)
Download profiling data of all nodes in a zip format.

__Example__

``` go
    profilingData, err := madmClnt.DownloadProfilingData()
    if err != nil {
            log.Fatalln(err)
    }

    profilingFile, err := os.Create("/tmp/profiling-data.zip")
    if err != nil {
            log.Fatal(err)
    }

    if _, err := io.Copy(profilingFile, profilingData); err != nil {
            log.Fatal(err)
    }

    if err := profilingFile.Close(); err != nil {
            log.Fatal(err)
    }

    if err := profilingData.Close(); err != nil {
            log.Fatal(err)
    }

    log.Println("Profiling data successfully downloaded.")
```
