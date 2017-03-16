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

| Service operations|LockInfo operations|Healing operations|Config operations| Misc |
|:---|:---|:---|:---|:---|
|[`ServiceStatus`](#ServiceStatus)| [`ListLocks`](#ListLocks)| [`ListObjectsHeal`](#ListObjectsHeal)|[`GetConfig`](#GetConfig)| [`SetCredentials`](#SetCredentials)|
|[`ServiceRestart`](#ServiceRestart)| [`ClearLocks`](#ClearLocks)| [`ListBucketsHeal`](#ListBucketsHeal)|[`SetConfig`](#SetConfig)||
| | |[`HealBucket`](#HealBucket) |||
| | |[`HealObject`](#HealObject)|||
| | |[`HealFormat`](#HealFormat)|||
| | |[`ListUploadsHeal`](#ListUploadsHeal)|||

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
Fetch service status, replies disk space used, backend type and total disks offline/online (applicable in distributed mode).

| Param  | Type  | Description  |
|---|---|---|
|`serviceStatus`  | _ServiceStatusMetadata_  | Represents current server status info in following format: |


| Param  | Type  | Description  |
|---|---|---|
|`st.ServerVersion.Version`  | _string_  | Server version. |
|`st.ServerVersion.CommitID`  | _string_  | Server commit id. |
|`st.StorageInfo.Total`  | _int64_  | Total disk space. |
|`st.StorageInfo.Free`  | _int64_  | Free disk space. |
|`st.StorageInfo.Backend`| _struct{}_ | Represents backend type embedded structure. |

| Param | Type | Description |
|---|---|---|
|`backend.Type` | _BackendType_ | Type of backend used by the server currently only FS or Erasure. |
|`backend.OnlineDisks`| _int_ | Total number of disks online (only applies to Erasure backend), is empty for FS. |
|`backend.OfflineDisks` | _int_ | Total number of disks offline (only applies to Erasure backend), is empty for FS. |
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

<a name="ServiceRestart"></a>
### ServiceRestart() (error)
If successful restarts the running minio service, for distributed setup restarts all remote minio servers.

 __Example__


 ```go


	st, err := madmClnt.ServiceRestart()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Success")

 ```

## 3. Lock operations

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

## 4. Heal operations

<a name="ListObjectsHeal"></a>
### ListObjectsHeal(bucket, prefix string, recursive bool, doneCh <-chan struct{}) (<-chan ObjectInfo, error)
If successful returns information on the list of objects that need healing in ``bucket`` matching ``prefix``.

__Example__

``` go
    // Create a done channel to control 'ListObjectsHeal' go routine.
    doneCh := make(chan struct{})

    // Indicate to our routine to exit cleanly upon return.
    defer close(doneCh)

    // Set true if recursive listing is needed.
    isRecursive := true
    // List objects that need healing for a given bucket and
    // prefix.
    healObjectCh, err := madmClnt.ListObjectsHeal("mybucket", "myprefix", isRecursive, doneCh)
    if err != nil {
        fmt.Println(err)
        return
    }
    for object := range healObjectsCh {
        if object.Err != nil {
            log.Fatalln(err)
            return
        }
        if object.HealObjectInfo != nil {
            switch healInfo := *object.HealObjectInfo; healInfo.Status {
            case madmin.CanHeal:
                fmt.Println(object.Key, " can be healed.")
            case madmin.QuorumUnavailable:
                fmt.Println(object.Key, " can't be healed until quorum is available.")
            case madmin.Corrupted:
                fmt.Println(object.Key, " can't be healed, not enough information.")
            }
        }
        fmt.Println("object: ", object)
    }
```

<a name="ListBucketsHeal"></a>
### ListBucketsHeal() error
If successful returns information on the list of buckets that need healing.

__Example__

``` go
    // List buckets that need healing
    healBucketsList, err := madmClnt.ListBucketsHeal()
    if err != nil {
        fmt.Println(err)
        return
    }
    for bucket := range healBucketsList {
        if bucket.HealBucketInfo != nil {
            switch healInfo := *object.HealBucketInfo; healInfo.Status {
            case madmin.CanHeal:
                fmt.Println(bucket.Key, " can be healed.")
            case madmin.QuorumUnavailable:
                fmt.Println(bucket.Key, " can't be healed until quorum is available.")
            case madmin.Corrupted:
                fmt.Println(bucket.Key, " can't be healed, not enough information.")
            }
        }
        fmt.Println("bucket: ", bucket)
    }
```

<a name="HealBucket"></a>
### HealBucket(bucket string, isDryRun bool) error
If bucket is successfully healed returns nil, otherwise returns error indicating the reason for failure. If isDryRun is true, then the bucket is not healed, but heal bucket request is validated by the server. e.g, if the bucket exists, if bucket name is valid etc.

__Example__

``` go
    isDryRun := false
    err := madmClnt.HealBucket("mybucket", isDryRun)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("successfully healed mybucket")

```

<a name="HealObject"></a>
### HealObject(bucket, object string, isDryRun bool) error
If object is successfully healed returns nil, otherwise returns error indicating the reason for failure. If isDryRun is true, then the object is not healed, but heal object request is validated by the server. e.g, if the object exists, if object name is valid etc.

__Example__

``` go
    isDryRun := false
    err := madmClnt.HealObject("mybucket", "myobject", isDryRun)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println("successfully healed mybucket/myobject")

```

<a name="HealFormat"></a>
### HealFormat(isDryRun bool) error
Heal storage format on available disks. This is used when disks were replaced or were found with missing format. This is supported only for erasure-coded backend.

__Example__

``` go
    isDryRun := true
    err := madmClnt.HealFormat(isDryRun)
    if err != nil {
        log.Fatalln(err)
    }

    isDryRun = false
    err = madmClnt.HealFormat(isDryRun)
    if err != nil {
        log.Fatalln(err)
    }

    log.Println("successfully healed storage format on available disks.")

```

## 5. Config operations

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

## 6. Misc operations

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

<a name="ListUploadsHeal"> </a>
### ListUploadsHeal(bucket, prefix string, recursive bool, doneCh <-chan struct{}) (<-chan UploadInfo, error)
List ongoing multipart uploads that need healing.

| Param  | Type  | Description  |
|---|---|---|
|`ui.Key` | _string_ | Name of the object being uploaded |
|`ui.UploadID` | _string_ | UploadID of the ongoing multipart upload |
|`ui.HealUploadInfo.Status` | _healStatus_| One of `Healthy`, `CanHeal`, `Corrupted`, `QuorumUnavailable`|
|`ui.Err`| _error_ | non-nil if fetching fetching healing information failed |

__Example__

``` go

    // Set true if recursive listing is needed.
    isRecursive := true
    // List objects that need healing for a given bucket and
    // prefix.
    healUploadsCh, err := madmClnt.ListUploadsHeal(bucket, prefix, isRecursive, doneCh)
    if err != nil {
        log.Fatalln("Failed to get list of uploads to be healed: ", err)
    }

    for upload := range healUploadsCh {
        if upload.Err != nil {
            log.Println("upload listing error: ", upload.Err)
        }

        if upload.HealUploadInfo != nil {
            switch healInfo := *upload.HealUploadInfo; healInfo.Status {
            case madmin.CanHeal:
                fmt.Println(upload.Key, " can be healed.")
            case madmin.QuorumUnavailable:
                fmt.Println(upload.Key, " can't be healed until quorum is available.")
            case madmin.Corrupted:
                fmt.Println(upload.Key, " can't be healed, not enough information.")
            }
        }
    }

```
