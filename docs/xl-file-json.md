### file.json

``file.json`` is a special file captured and written by XL storage API layer
to interpret, manage and extract erasured data from multiple disks.

```json
{
    "version": "1.0.0",
    "stat": {
        "size": 24256,
        "modTime": "2016-04-28T00:11:37.843Z",
        "delete": false,
        "version": 0
    },
    "erasure": {
        "data": 5,
        "parity": 5,
        "blockSize": 4194304
    ],
    "minio": {
        "release": "RELEASE.2016-04-28T00-09-47Z"
    }
}
```

#### JSON meaning.

- "version" // Version of the meta json file.

- "stat" // Stat value of written file.

  - "size"     // Size of the file.
  - "modTime"  // Modified time of the file.
  - "deleted"  // Field to track if the file is deleted when disks are down.
  - "version"  // File version tracked when disks are down.

- "erasure" // Erasure metadata for the written file.

  - "data"       // Data blocks parts of the file.
  - "parity"     // Parity blocks parts of the file.
  - "blockSize"  // BlockSize read/write chunk size.
