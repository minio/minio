# Golang Admin Client API Reference [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
The MinIO Admin Golang Client SDK provides APIs to manage MinIO services.

This quickstart guide will show you how to install the MinIO Admin client SDK, connect to MinIO admin service, and provide a walkthrough of a simple file uploader.

This document assumes that you have a working [Golang setup](https://golang.org/doc/install).

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
    st, err := mdmClnt.ServerInfo()
    if err != nil {
        fmt.Println(err)
        return
    }
	for _, peerInfo := range serversInfo {
		log.Printf("Node: %s, Info: %v\n", peerInfo.Addr, peerInfo.Data)
	}
}

```

| Service operations                  | Info operations               | Healing operations | Config operations         |
|:------------------------------------|:------------------------------|:-------------------|:--------------------------|
| [`ServiceTrace`](#ServiceTrace)     | [`ServerInfo`](#ServerInfo)   | [`Heal`](#Heal)    | [`GetConfig`](#GetConfig) |
| [`ServiceStop`](#ServiceStop)       | [`StorageInfo`](#StorageInfo) |                    | [`SetConfig`](#SetConfig) |
| [`ServiceRestart`](#ServiceRestart) |                               |                    |                           |



| Top operations          | IAM operations                        | Misc                                              | KMS                             |
|:------------------------|:--------------------------------------|:--------------------------------------------------|:--------------------------------|
| [`TopLocks`](#TopLocks) | [`AddUser`](#AddUser)                 | [`StartProfiling`](#StartProfiling)               | [`GetKeyStatus`](#GetKeyStatus) |
|                         | [`SetUserPolicy`](#SetUserPolicy)     | [`DownloadProfilingData`](#DownloadProfilingData) |                                 |
|                         | [`ListUsers`](#ListUsers)             | [`ServerUpdate`](#ServerUpdate)                   |                                 |
|                         | [`AddCannedPolicy`](#AddCannedPolicy) |                                                   |                                 |

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

## 2. Service operations

<a name="ServiceStatus"></a>
### ServiceStatus(ctx context.Context) (ServiceStatusMetadata, error)
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

	st, err := madmClnt.ServiceStatus(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", st)

 ```

<a name="ServiceRestart"></a>
### ServiceRestart(ctx context.Context) error
Sends a service action restart command to MinIO server.

 __Example__

```go
   // To restart the service, restarts all servers in the cluster.
   err := madmClnt.ServiceRestart(context.Background())
   if err != nil {
       log.Fatalln(err)
   }
   log.Println("Success")
```

<a name="ServiceStop"></a>
### ServiceStop(ctx context.Context) error
Sends a service action stop command to MinIO server.

 __Example__

```go
   // To stop the service, stops all servers in the cluster.
   err := madmClnt.ServiceStop(context.Background())
   if err != nil {
       log.Fatalln(err)
   }
   log.Println("Success")
```

<a name="ServiceTrace"></a>
### ServiceTrace(ctx context.Context, allTrace bool, doneCh <-chan struct{}) <-chan TraceInfo
Enable HTTP request tracing on all nodes in a MinIO cluster

__Example__

``` go
    doneCh := make(chan struct{})
    defer close(doneCh)
    // listen to all trace including internal API calls
    allTrace := true
    // Start listening on all trace activity.
    traceCh := madmClnt.ServiceTrace(context.Background(), allTrace, doneCh)
    for traceInfo := range traceCh {
        fmt.Println(traceInfo.String())
    }
```

## 3. Info operations

<a name="ServerInfo"></a>
### ServerInfo(ctx context.Context) ([]ServerInfo, error)
Fetches information for all cluster nodes, such as server properties, storage information, network statistics, etc.

| Param                            | Type               | Description                                                        |
|----------------------------------|--------------------|--------------------------------------------------------------------|
| `si.Addr`                        | _string_           | Address of the server the following information is retrieved from. |
| `si.ConnStats`                   | _ServerConnStats_  | Connection statistics from the given server.                       |
| `si.HTTPStats`                   | _ServerHTTPStats_  | HTTP connection statistics from the given server.                  |
| `si.Properties`                  | _ServerProperties_ | Server properties such as region, notification targets.            |

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

| Param                | Type     | Description                                           |
|----------------------|----------|-------------------------------------------------------|
| `DriveInfo.UUID`     | _string_ | Unique ID for each disk provisioned by server format. |
| `DriveInfo.Endpoint` | _string_ | Endpoint location of the remote/local disk.           |
| `DriveInfo.State`    | _string_ | Current state of the disk at endpoint.                |

 __Example__

 ```go

	serversInfo, err := madmClnt.ServerInfo(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	for _, peerInfo := range serversInfo {
		log.Printf("Node: %s, Info: %v\n", peerInfo.Addr, peerInfo.Data)
	}

 ```

<a name="StorageInfo"></a>
### StorageInfo(ctx context.Context) (StorageInfo, error)

Fetches Storage information for all cluster nodes.

| Param                   | Type       | Description                                 |
|-------------------------|------------|---------------------------------------------|
| `storageInfo.Used`      | _[]int64_  | Used disk spaces.                           |
| `storageInfo.Total`     | _[]int64_  | Total disk spaces.                          |
| `storageInfo.Available` | _[]int64_  | Available disk spaces.                      |
| `StorageInfo.Backend`   | _struct{}_ | Represents backend type embedded structure. |

| Param                      | Type            | Description                                                                                                              |
|----------------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------|
| `Backend.Type`             | _BackendType_   | Type of backend used by the server currently only FS or Erasure.                                                         |
| `Backend.OnlineDisks`      | _BackendDisks_  | Total number of disks online per node (only applies to Erasure backend) represented in map[string]int, is empty for FS.  |
| `Backend.OfflineDisks`     | _BackendDisks_  | Total number of disks offline per node (only applies to Erasure backend) represented in map[string]int, is empty for FS. |
| `Backend.StandardSCData`   | _int_           | Data disks set for standard storage class, is empty for FS.                                                              |
| `Backend.StandardSCParity` | _int_           | Parity disks set for standard storage class, is empty for FS.                                                            |
| `Backend.RRSCData`         | _int_           | Data disks set for reduced redundancy storage class, is empty for FS.                                                    |
| `Backend.RRSCParity`       | _int_           | Parity disks set for reduced redundancy storage class, is empty for FS.                                                  |
| `Backend.Sets`             | _[][]DriveInfo_ | Represents topology of drives in erasure coded sets.                                                                     |

__Example__

 ```go

	storageInfo, err := madmClnt.StorageInfo(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

    log.Println(storageInfo)

 ```

## 5. Heal operations

<a name="Heal"></a>
### Heal(ctx context.Context, bucket, prefix string, healOpts HealOpts, clientToken string, forceStart bool, forceStop bool) (start HealStartSuccess, status HealTaskStatus, err error)

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
    healPath, err := madmClnt.Heal(context.Background(), "", "", opts, "", forceStart, forceStop)
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

## 6. Config operations

<a name="GetConfig"></a>
### GetConfig(ctx context.Context) ([]byte, error)
Get current `config.json` of a MinIO server.

__Example__

``` go
    configBytes, err := madmClnt.GetConfig(context.Background())
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
### SetConfig(ctx context.Context, config io.Reader) error
Set a new `config.json` for a MinIO server.

__Example__

``` go
    config := bytes.NewReader([]byte(`config.json contents go here`))
    if err := madmClnt.SetConfig(context.Background(), config); err != nil {
        log.Fatalf("failed due to: %v", err)
    }
    log.Println("SetConfig was successful")
```

## 7. Top operations

<a name="TopLocks"></a>
### TopLocks(ctx context.Context) (LockEntries, error)
Get the oldest locks from MinIO server.

__Example__

``` go
    locks, err := madmClnt.TopLocks(context.Background())
    if err != nil {
        log.Fatalf("failed due to: %v", err)
    }

    out, err := json.Marshal(locks)
    if err != nil {
        log.Fatalf("Marshal failed due to: %v", err)
    }

    log.Println("TopLocks received successfully: ", string(out))
```

## 8. IAM operations

<a name="AddCannedPolicy"></a>
### AddCannedPolicy(ctx context.Context, policyName string, policy *iampolicy.Policy) error
Create a new canned policy on MinIO server.

__Example__

```
	policy, err := iampolicy.ParseConfig(strings.NewReader(`{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Resource": ["arn:aws:s3:::my-bucketname/*"],"Sid": ""}]}`))
    if err != nil {
        log.Fatalln(err)
    }

    if err = madmClnt.AddCannedPolicy(context.Background(), "get-only", policy); err != nil {
		log.Fatalln(err)
	}
```

<a name="AddUser"></a>
### AddUser(ctx context.Context, user string, secret string) error
Add a new user on a MinIO server.

__Example__

``` go
	if err = madmClnt.AddUser(context.Background(), "newuser", "newstrongpassword"); err != nil {
		log.Fatalln(err)
	}
```

<a name="SetUserPolicy"></a>
### SetUserPolicy(ctx context.Context, user string, policyName string) error
Enable a canned policy `get-only` for a given user on MinIO server.

__Example__

``` go
	if err = madmClnt.SetUserPolicy(context.Background(), "newuser", "get-only"); err != nil {
		log.Fatalln(err)
	}
```

<a name="ListUsers"></a>
### ListUsers(ctx context.Context) (map[string]UserInfo, error)
Lists all users on MinIO server.

__Example__

``` go
	users, err := madmClnt.ListUsers(context.Background());
    if err != nil {
		log.Fatalln(err)
	}
    for k, v := range users {
        fmt.Printf("User %s Status %s\n", k, v.Status)
    }
```

## 9. Misc operations

<a name="ServerUpdate"></a>
### ServerUpdate(ctx context.Context, updateURL string) (ServerUpdateStatus, error)
Sends a update command to MinIO server, to update MinIO server to latest release. In distributed setup it updates all servers atomically.

 __Example__

```go
   // Updates all servers and restarts all the servers in the cluster.
   // optionally takes an updateURL, which is used to update the binary.
   us, err := madmClnt.ServerUpdate(context.Background(), updateURL)
   if err != nil {
       log.Fatalln(err)
   }
   if us.CurrentVersion != us.UpdatedVersion {
       log.Printf("Updated server version from %s to %s successfully", us.CurrentVersion, us.UpdatedVersion)
   }
```

<a name="StartProfiling"></a>
### StartProfiling(ctx context.Context, profiler string) error
Ask all nodes to start profiling using the specified profiler mode

__Example__

``` go
    startProfilingResults, err = madmClnt.StartProfiling(context.Background(), "cpu")
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
### DownloadProfilingData(ctx context.Context) ([]byte, error)
Download profiling data of all nodes in a zip format.

__Example__

``` go
    profilingData, err := madmClnt.DownloadProfilingData(context.Background())
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

## 11. KMS

<a name="GetKeyStatus"></a>
### GetKeyStatus(ctx context.Context, keyID string) (*KMSKeyStatus, error)
Requests status information about one particular KMS master key
from a MinIO server. The keyID is optional and the server will
use the default master key (configured via `MINIO_KMS_VAULT_KEY_NAME`
or `MINIO_KMS_MASTER_KEY`) if the keyID is empty.

__Example__

``` go
    keyInfo, err := madmClnt.GetKeyStatus(context.Background(), "my-minio-key")
    if err != nil {
       log.Fatalln(err)
    }
    if keyInfo.EncryptionErr != "" {
       log.Fatalf("Failed to perform encryption operation using '%s': %v\n", keyInfo.KeyID, keyInfo.EncryptionErr)
    }
    if keyInfo.UpdateErr != "" {
       log.Fatalf("Failed to perform key re-wrap operation using '%s': %v\n", keyInfo.KeyID, keyInfo.UpdateErr)
    }
    if keyInfo.DecryptionErr != "" {
       log.Fatalf("Failed to perform decryption operation using '%s': %v\n", keyInfo.KeyID, keyInfo.DecryptionErr)
    }
```
