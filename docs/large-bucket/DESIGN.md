## Command-line
```
NAME:
  minio server - Start object storage server.

USAGE:
  minio server [FLAGS] DIR1 [DIR2..]
  minio server [FLAGS] DIR{1...64}

DIR:
  DIR points to a directory on a filesystem. When you want to combine multiple drives
  into a single large system, pass one directory per filesystem separated by space.
  You may also use a `...` convention to abbreviate the directory arguments. Remote
  directories in a distributed setup are encoded as HTTP(s) URIs.
```

## Limitations
- Minimum of 4 disks are needed for distributed erasure coded configuration.
- Maximum of 32 distinct nodes are supported in distributed configuration.

## Common usage
Single disk filesystem export
```
minio server dir1
```

Standalone erasure coded configuration with 4 disks.
```
minio server dir1 dir2 dir3 dir4
```

Standalone erasure coded configuration with 4 sets with 16 disks each.
```
minio server dir{1...64}
```

Distributed erasure coded configuration with 64 sets with 16 disks each.
```
minio server http://host{1...16}/export{1...64} - good
```

## Other usages

### Advanced use cases with multiple ellipses

Standalone erasure coded configuration with 4 sets with 16 disks each, which spawns disks across controllers.
```
minio server /mnt/controller{1...4}/data{1...16}
```

Standalone erasure coded configuration with 16 sets 16 disks per set, across mnts, across controllers.
```
minio server /mnt{1..4}/controller{1...4}/data{1...16}
```

Distributed erasure coded configuration with 2 sets 16 disks per set across hosts.
```
minio server http://host{1...32}/disk1
```

Distributed erasure coded configuration with rack level redundancy 32 sets in total, 16 disks per set.
```
minio server http://rack{1...4}-host{1...8}.example.net/export{1...16}
```

Distributed erasure coded configuration with no rack level redundancy but redundancy with in the rack we split the arguments, 32 sets in total, 16 disks per set.
```
minio server http://rack1-host{1...8}.example.net/export{1...16} http://rack2-host{1...8}.example.net/export{1...16} http://rack3-host{1...8}.example.net/export{1...16} http://rack4-host{1...8}.example.net/export{1...16}
```

### Expected expansion for double ellipses
```
minio server http://host{1...4}/export{1...8}
```

Expected expansion
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

## Backend `format.json` changes
New `format.json` has new fields

- `disk` is changed to `this`
- `jbod` is changed to `sets` , along with this change sets is also a two dimensional list representing total sets and disks per set.

A sample `format.json` looks like below
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

New `format-xl.go` behavior is format structure is used as a opaque type, `Format` field signifies the format of the backend. Once the format has been identified it is now the job of the identified backend to further interpret the next structures and validate them.

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

### Current format
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

### New format
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
