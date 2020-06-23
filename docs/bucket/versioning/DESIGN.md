# Bucket Versioning Design Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Description of `xl.meta`

`xl.meta` is a new self describing backend format used by MinIO to support AWS S3 compatible versioning. This file is the source of truth for each `version` at rest. `xl.meta` is a msgpack file serialized from a well defined data structure. To understand `xl.meta` here are the few things to start with

`xl.meta` carries first 8 bytes an XL header which describes the current format and the format version, allowing the unmarshaller's to automatically use the right data structures to parse the subsequent content in the stream.

These are the current entries
```go
var (
        // XL header specifies the format
        // one extra byte left for future use
        xlHeader = [4]byte{'X', 'L', '2', ' '}

        // XLv2 version 1 specifies the current
        // version of the XLv2 format, 3 extra bytes
        // left for future use.
        xlVersionV1 = [4]byte{'1', ' ', ' ', ' '}
)
```

Once the header is validated, we proceed to the actual data structure of the `xl.meta` format. `xl.meta` carries three types of object entries which designate the type of version object stored.

- ObjectType (default)
- LegacyObjectType (preserves existing deployments and older xl.json format)
- DeleteMarker (a versionId to capture the DELETE sequences implemented primarily for AWS spec compatibility)

A sample msgpack-JSON `xl.meta`, you can debug the content inside `xl.meta` using [xl-meta-to-json.go][./xl-meta-to-json.go] program.
```json
{
  "Versions": [
    {
      "Type": 1,
      "V2Obj": {
        "ID": "KWUs8S+8RZq4Vp5TWy6KFg==",
        "DDir": "X3pDAFu8Rjyft7QD6t7W5g==",
        "EcAlgo": 1,
        "EcM": 2,
        "EcN": 2,
        "EcBSize": 10485760,
        "EcIndex": 3,
        "EcDist": [
          3,
          4,
          1,
          2
        ],
        "CSumAlgo": 1,
        "PartNums": [
          1
        ],
        "PartETags": [
          ""
        ],
        "PartSizes": [
          314
        ],
        "PartASizes": [
          282
        ],
        "Size": 314,
        "MTime": 1591820730,
        "MetaSys": {
          "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "bXktbWluaW8ta2V5",
          "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "ZXlKaFpXRmtJam9pUVVWVExUSTFOaTFIUTAwdFNFMUJReTFUU0VFdE1qVTJJaXdpYVhZaU9pSkJMMVZzZFVnelZYVjZSR2N6UkhGWUwycEViRmRCUFQwaUxDSnViMjVqWlNJNklpdE9lbkJXVWtseFlWSlNVa2t2UVhNaUxDSmllWFJsY3lJNklrNDBabVZsZG5WU1NWVnRLMFoyUWpBMVlYTk9aMU41YVhoU1RrNUpkMDlhTkdKa2RuaGpLMjFuVDNnMFFYbFJhbE15V0hkU1pEZzNRMk54ZUN0SFFuSWlmUT09",
          "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "REFSRXYyLUhNQUMtU0hBMjU2",
          "X-Minio-Internal-Server-Side-Encryption-Iv": "bW5YRDhRUGczMVhkc2pJT1V1UVlnbWJBcndIQVhpTUN1dnVBS0QwNUVpaz0=",
          "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "SUFBZkFPeUo5ZHVVSEkxYXFLU0NSRkJTTnM0QkVJNk9JWU1QcFVTSXFhK2dHVThXeE9oSHJCZWwwdnRvTldUNE8zS1BtcWluR0cydmlNNFRWa0N0Mmc9PQ=="
        },
        "MetaUsr": {
          "content-type": "application/octet-stream",
          "etag": "20000f00f58c508b40720270929bd90e9f07b9bd78fb605e5432a67635fc34722e4fc53b1d5fab9ff8400eb9ded4fba2"
        }
      }
    }
  ]
}
```
