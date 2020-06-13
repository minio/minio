# Bucket Versioning Design Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Example of a version enabled bucket `engineering`
```
/mnt/data02/engineering/backup.tar.gz
├─ 0379f361-695c-4509-b0b8-a4842d414579
│  └─ part.1
├─ 804fea2c-c21e-490b-98ff-cdb647047cb6
│  └─ part.1
├─ e063d138-4d6e-4e68-8576-12d1b4509cc3
│  └─ part.1
├─ e675c1f6-476d-4b46-be31-281c887aea7b
│  └─ part.1
└─ xl.meta

/mnt/data03/engineering/backup.tar.gz
├─ 0379f361-695c-4509-b0b8-a4842d414579
│  └─ part.1
├─ 804fea2c-c21e-490b-98ff-cdb647047cb6
│  └─ part.1
├─ e063d138-4d6e-4e68-8576-12d1b4509cc3
│  └─ part.1
├─ e675c1f6-476d-4b46-be31-281c887aea7b
│  └─ part.1
└─ xl.meta

/mnt/data04/engineering/backup.tar.gz
├─ 0379f361-695c-4509-b0b8-a4842d414579
│  └─ part.1
├─ 804fea2c-c21e-490b-98ff-cdb647047cb6
│  └─ part.1
├─ e063d138-4d6e-4e68-8576-12d1b4509cc3
│  └─ part.1
├─ e675c1f6-476d-4b46-be31-281c887aea7b
│  └─ part.1
└─ xl.meta

/mnt/data05/engineering/backup.tar.gz
├─ 0379f361-695c-4509-b0b8-a4842d414579
│  └─ part.1
├─ 804fea2c-c21e-490b-98ff-cdb647047cb6
│  └─ part.1
├─ e063d138-4d6e-4e68-8576-12d1b4509cc3
│  └─ part.1
├─ e675c1f6-476d-4b46-be31-281c887aea7b
│  └─ part.1
└─ xl.meta
```

`xl.meta` is a msgpack file with following data structure, this is converted from binary format to JSON for convenience.
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
