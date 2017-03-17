# Minio Erasure Code Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio protects data against hardware failures and silent data corruption using erasure code and checksums. You may lose  half the number (N/2) of drives and still be able to recover the data.

## 1. Prerequisites:

Install Minio - [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)

## What is Erasure Code?

Erasure code is a mathematical algorithm to reconstruct missing or corrupted data. Minio uses Reed-Solomon code to shard objects into N/2 data and N/2 parity blocks. This means that in a 12 drive setup, an object is sharded across as 6 data and 6 parity blocks. You can lose as many as 6 drives (be it parity or data) and still reconstruct the data reliably from the remaining drives.

## Why is Erasure Code useful?

Erasure code protects data from multiple drives failure unlike RAID or replication. For eg RAID6 can protect against 2 drive failure whereas in Minio erasure code you can lose as many as half number of drives and still the data remains safe. Further Minio's erasure code is at object level and can heal one object at a time. For RAID, healing can only be performed at volume level which translates into huge down time. As Minio encodes each object individually with a high parity count. Storage servers once deployed should not require drive replacement or healing for the lifetime of the server. Minio's erasure coded backend is designed for operational efficiency and takes full advantage of hardware acceleration whenever available.

![Erasure](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/erasure-code.png?raw=true)

## What is Bit Rot protection?

Bit Rot also known as Data Rot or Silent Data Corruption is a serious data loss issue faced by disk drives today. Data on the drive may silently get corrupted without signalling an error has occurred. This makes Bit Rot more dangerous than permanent hard drive failure.

Minio's erasure coded backend uses high speed [BLAKE2](https://blog.minio.io/accelerating-blake2b-by-4x-using-simd-in-go-assembly-33ef16c8a56b#.jrp1fdwer) hash based checksums to protect against Bit Rot.  

## Deployment Scenarios

Minio server runs on a variety of hardware, operating systems and virtual/container environments.

Minio erasure code backend is limited by design to a minimum of 4 drives and a maximum of 16 drives. The hard limit of 16 drives comes from operational experience. Failure domain becomes too large beyond 16 drives. If you need to scale beyond 16 drives, you may run multiple instances of Minio server on different ports.

#### Reference Physical Hardware:

* [SMC 5018A-AR12L (Intel Atom)](http://www.supermicro.com/products/system/1U/5018/SSG-5018A-AR12L.cfm?parts=SHOW) - SMC 1U SoC Atom C2750 platform with 12x 3.5” drive bays
* [Quanta Grid D51B-2U (OCP Compliant) ](http://www.qct.io/Product/Servers/Rackmount-Servers/2U/QuantaGrid-D51B-2U-p256c77c70c83c118)- Quanta 2U DP E5-2600v3 platform with 12x 3.5” drive bays
* [Cisco UCS C240 M4 Rack Server](http://www.cisco.com/c/en/us/products/servers-unified-computing/ucs-c240-m4-rack-server/index.html) - Cisco 2U DP E5-2600v3 platform with 12x 3.5” drive bays
* [Intel® Server System R2312WTTYSR](http://ark.intel.com/products/88286) - Intel 2U DP E5-2600v3 platform with 12x 3.5” drive bays

#### Reference Cloud Hosting Providers:

* [Packet](https://www.packet.net): Packet is the baremetal cloud provider.
* [Digital Ocean](https://www.digitalocean.com): Deploy an SSD cloud server in 55 seconds.
* [OVH](https://www.ovh.com/us): Build your own infrastructure with OVH public cloud.
* [Onlinetech](http://www.onlinetech.com): Secure, compliant enterprise cloud.
* [SSD Nodes](https://www.ssdnodes.com): Simple, high performance cloud provider with truly personalized support.

## 2. Run Minio Server with Erasure Code.

Example: Start Minio server in a 12 drives setup.

```sh
minio server /mnt/export1/backend /mnt/export2/backend /mnt/export3/backend /mnt/export4/backend /mnt/export5/backend /mnt/export6/backend /mnt/export7/backend /mnt/export8/backend /mnt/export9/backend /mnt/export10/backend /mnt/export11/backend /mnt/export12/backend
```

## 3. Test your setup

You may unplug drives randomly and continue to perform I/O on the system.
