# Bucket Versioning Design Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Description of `xl.meta`

`xl.meta` is a new self describing backend format used by MinIO to support AWS S3 compatible versioning.
This file is the source of truth for each `version` at rest. `xl.meta` is a msgpack file serialized from a
well defined data structure. To understand `xl.meta` here are the few things to start with

`xl.meta` carries first 8 bytes an XL header which describes the current format and the format version,
allowing the unmarshaller's to automatically use the right data structures to parse the subsequent content in the stream.

### v1.0

| Entry     | Encoding    | Content
| ----------|-------------|----------------------------------------
| xlHeader  | [4]byte     | `'X', 'L', '2', ' '`
| xlVersion | [4]byte     | `'1', ' ', ' ', ' '`
| xlMetaV2  | msgp object | All versions as single messagepack object
| [EOF] | |

### v1.1+

Version 1.1 added inline data, which will be placed after the metadata.

Therefore, the metadata is wrapped as a binary array for easy skipping.

| Entry          | Encoding       | Content
| ---------------|----------------|----------------------------------------
| xlHeader       | [4]byte        | `'X', 'L', '2', ' '`
| xlVersionMajor | uint16         | Major xl-meta version.
| xlVersionMinor | uint16         | Minor xl-meta version.
| xlMetaV2       | msgp bin array | Bin array with serialized metadata
| crc            | msgp uint      | Lower 32 bits of 64 bit xxhash of previous array contents (v1.2+ only)
| inline data    | binary         | Inline data if any, see Inline Data section for encoding.  
| [EOF] | |

## v1.0-v1.2 Versions

`xl.meta` carries three types of object entries which designate the type of version object stored.

- ObjectType (default)
- LegacyObjectType (preserves existing deployments and older xl.json format)
- DeleteMarker (a versionId to capture the DELETE sequences implemented primarily for AWS spec compatibility)

A sample msgpack-JSON `xl.meta`, you can debug the content inside `xl.meta` using [xl-meta.go](https://github.com/minio/minio/tree/master/docs/debugging#decoding-metadata) program.

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
        "EcDist": [3, 4, 1, 2],
        "CSumAlgo": 1,
        "PartNums": [1],
        "PartETags": [""],
        "PartSizes": [314],
        "PartASizes": [282],
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

### v1.3+ versions

Version 1.3 introduces changes to help with [faster metadata reads and updates](https://blog.min.io/minio-versioning-metadata-deep-dive/)

| Entry           | Encoding                    | Content
| ----------------|-----------------------------|----------------------------------------
| xlHeaderVersion | msgp uint                   | header version identifier
| xlMetaVersion   | msgp uint                   | metadata version identifier
| versions        | msgp int                    | Number of versions following
| header_1        | msgp bin array              | Header of version 1
| metadata_1      | msgp bin array              | Metadata of version 1
| ...header_n     | msgp bin array              | Header of last version
| ...metadata_n   | msgp bin array              | Metadata of last version

Each header contains a mspg array (tuple) encoded object:

xlHeaderVersion version == 1:

```
//msgp:tuple xlMetaV2VersionHeader
type xlMetaV2VersionHeader struct {
 VersionID [16]byte  // Version UUID, raw.
 ModTime   int64     // Unix nanoseconds.
 Signature [4]byte   // Signature of metadata.
 Type      uint8     // Type if the version
 Flags     uint8
}
```

The following flags are defined:

```
const (
 FreeVersion = 1 << 0
 UsesDataDir = 1 << 1
 InlineData  = 1 << 2
)
```

The "Metadata" section contains a single version, encoded in similar fashion as each version in the `Versions` array
of the previous version.

## Inline Data

Inline data is optional. If no inline data is present, it is encoded as 0 bytes.

| Entry               | Encoding                    | Content
| --------------------|-----------------------------|----------------------------------------
| xlMetaInlineDataVer | byte                        | version identifier
| id -> data          | msgp `map[string][]byte`      | Map of string id -> byte content

Currently only xlMetaInlineDataVer == 1 exists.

The ID is the string encoded Version ID of which the data corresponds.
