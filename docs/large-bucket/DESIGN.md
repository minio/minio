# Large Bucket Support Design Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This topic explains the design approach, advanced use cases, and limits of the large-bucket feature. To get started with large bucket support, see the [Large Bucket Support Quickstart Guide](https://github.com/minio/minio/blob/master/docs/large-bucket/README.md).

- [General Command-line Usage](#general-command-line-usage) 
- [Usage for Standalone and Distributed Erasure-coded Configurations](#usage-for-standalone-and-distributed-erasure-coded-configurations) 
- [Other Usages](#other-usages) 
- [Backend Changes](#backend-changes) 
- [Limits](#limits)

## <a name="general-command-line-usage"></a>1. General Command-line Usage

The following shows the general usage for Minio Server:

```
NAME:
  minio server - Start object storage server.

USAGE:
  minio server [FLAGS] DIR1 [DIR2..]
  minio server [FLAGS] DIR{1...64}

DIR:
  DIR points to a directory on a file system. When combining multiple drives
  into a single large system, pass one directory per file system separated by a space.
  The `...` convention can also be used to abbreviate the directory arguments. Remote
  directories in a distributed configuration are encoded as HTTP(s) URIs.
```

## <a name="usage-for-standalone-and-distributed-erasure-coded-configurations"></a>2. Usage for Standalone and Distributed Erasure-coded Configurations

Use the following command for a standalone erasure-coded configuration with 4 sets, each consisting of 16 drives:

```
minio server dir{1...64}
```

Use the following command for a distributed erasure-coded configuration with 64 sets, each consisting of 16 drives:

```
minio server http://host{1...16}/export{1...64}
```

## <a name="other-usages"></a>3. Other Usages

### 3.1 Advanced Use Cases with Multiple Ellipses

Use the following command for a standalone erasure-coded configuration with 4 sets, each consisting of 16 drives, which spawns drives across controllers:

```
minio server /mnt/controller{1...4}/data{1...16}
```

Use the following command for a standalone erasure-coded configuration with 16 sets, each consisting of 16 drives, across mounts and controllers:

```
minio server /mnt{1..4}/controller{1...4}/data{1...16}
```

Use the following command for a distributed erasure-coded configuration with 2 sets, each consisting of 16 drives across hosts:

```
minio server http://host{1...32}/disk1
```

Use the following command for a distributed erasure-coded configuration using rack-level redundancy with 32 sets, each consisting of 16 drives:

```
minio server http://rack{1...4}-host{1...8}.example.net/export{1...16}
```

Use the following command for a distributed erasure-coded configuration with no rack-level redundancy, but the arguments split into 32 sets, each consisting of 16 drives:

```
minio server http://rack1-host{1...8}.example.net/export{1...16} http://rack2-host{1...8}.example.net/export{1...16} http://rack3-host{1...8}.example.net/export{1...16} http://rack4-host{1...8}.example.net/export{1...16}
```

### 3.2 Expected Expansion for Double Ellipses

The follow example shows how Minio Server internally expands ellipses passed as arguments:

```
minio server http://host{1...4}/export{1...8}
```

Expected expansion:

```
> http://host1/export1
> http://host2/export1
> http://host3/export1
> http://host4/export1
> http://host1/export2
> http://host2/export2
> http://host3/export2
> http://host4/export2
> http://host1/export3
> http://host2/export3
> http://host3/export3
> http://host4/export3
> http://host1/export4
> http://host2/export4
> http://host3/export4
> http://host4/export4
> http://host1/export5
> http://host2/export5
> http://host3/export5
> http://host4/export5
> http://host1/export6
> http://host2/export6
> http://host3/export6
> http://host4/export6
> http://host1/export7
> http://host2/export7
> http://host3/export7
> http://host4/export7
> http://host1/export8
> http://host2/export8
> http://host3/export8
> http://host4/export8
```

## <a name="backend-changes"></a>4. Backend Changes

### 4.1 Changes to Fields in `format.json`

In the backed, `format.json` has new fields:
- `disk` is changed to `this`.
- `jbod` is changed to `sets`. Along with this change, `sets` is also a two dimensional list representing the total number of sets and drives per set.

The following shows an example of `format.json` with these new fields:

```json
{
  "version": "1",
  "format": "xl",
  "xl": {
    "version": "2",
    "this": "4ec63786-3dbd-4a9e-96f5-535f6e850fb1",
    "sets": [
    [
      "4ec63786-3dbd-4a9e-96f5-535f6e850fb1",
      "1f3cf889-bc90-44ca-be2a-732b53be6c9d",
      "4b23eede-1846-482c-b96f-bfb647f058d3",
      "e1f17302-a850-419d-8cdb-a9f884a63c92"
    ], [
      "2ca4c5c1-dccb-4198-a840-309fea3b5449",
      "6d1e666e-a22c-4db4-a038-2545c2ccb6d5",
      "d4fa35ab-710f-4423-a7c2-e1ca33124df0",
      "88c65e8b-00cb-4037-a801-2549119c9a33"
       ]
    ],
    "distributionAlgo": "CRCMOD"
  }
}
```

### 4.2 Changes to `format-xl.go`

The new `format-xl.go` behavior contains a format structure that is used as an opaque type, where the `format` field signifies the format of the backend. Once the format has been identified, it is the job of the identified backend to further interpret the next structures and validate them.

```go
type formatType string

const (
     formatFS formatType = "fs"
     formatXL            = "xl"
)

type format struct {
     Version string
     Format  BackendFormat
}
```

#### 4.1. Current Format

```go
type formatXLV1 struct{
     format
     XL struct{
        Version string
        Disk string
        JBOD []string
     }
}
```

#### 4.2 New Format

```go
type formatXLV2 struct {
        Version string `json:"version"`
        Format  string `json:"format"`
        XL      struct {
                Version          string     `json:"version"`
                This             string     `json:"this"`
                Sets             [][]string `json:"sets"`
                DistributionAlgo string     `json:"distributionAlgo"`
        } `json:"xl"`
}
```

## <a name="limits"></a>5. Limits
Large-bucket support has the following limitations:
* A minimum of 4 drives are needed for erasure-coded configurations.
* A maximum of 32 distinct nodes are supported for distributed configurations.
