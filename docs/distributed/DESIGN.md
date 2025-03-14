# Distributed Server Design Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

This document explains the design, architecture and advanced use cases of the MinIO distributed server.

## Command-line

```
NAME:
  minio server - start object storage server

USAGE:
  minio server [FLAGS] DIR1 [DIR2..]
  minio server [FLAGS] DIR{1...64}
  minio server [FLAGS] DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
```

## Common usage

Standalone erasure coded configuration with 4 sets with 16 drives each.

```
minio server dir{1...64}
```

Distributed erasure coded configuration with 64 sets with 16 drives each.

```
minio server http://host{1...16}/export{1...64}
```

## Architecture

Expansion of ellipses and choice of erasure sets based on this expansion is an automated process in MinIO. Here are some of the details of our underlying erasure coding behavior.

- Erasure coding used by MinIO is [Reed-Solomon](https://github.com/klauspost/reedsolomon) erasure coding scheme, which has a total shard maximum of 256 i.e 128 data and 128 parity. MinIO design goes beyond this limitation by doing some practical architecture choices.

- Erasure set is a single erasure coding unit within a MinIO deployment. An object is sharded within an erasure set. Erasure set size is automatically calculated based on the number of drives. MinIO supports unlimited number of drives but each erasure set can be up to 16 drives and a minimum of 2 drives.

- We limited the number of drives to 16 for erasure set because, erasure code shards more than 16 can become chatty and do not have any performance advantages. Additionally since 16 drive erasure set gives you tolerance of 8 drives per object by default which is plenty in any practical scenario.

- Choice of erasure set size is automatic based on the number of drives available, let's say for example if there are 32 servers and 32 drives which is a total of 1024 drives. In this scenario 16 becomes the erasure set size. This is decided based on the greatest common divisor (GCD) of acceptable erasure set sizes ranging from *4 to 16*.

- *If total drives has many common divisors the algorithm chooses the minimum amounts of erasure sets possible for a erasure set size of any N*.  In the example with 1024 drives - 4, 8, 16 are GCD factors. With 16 drives we get a total of 64 possible sets, with 8 drives we get a total of 128 possible sets, with 4 drives we get a total of 256 possible sets. So algorithm automatically chooses 64 sets, which is *16* 64 = 1024* drives in total.

- *If total number of nodes are of odd number then GCD algorithm provides affinity towards odd number erasure sets to provide for uniform distribution across nodes*. This is to ensure that same number of drives are pariticipating in any erasure set. For example if you have 2 nodes with 180 drives then GCD is 15 but this would lead to uneven distribution, one of the nodes would participate more drives. To avoid this the affinity is given towards nodes which leads to next best GCD factor of 12 which provides uniform distribution.

- In this algorithm, we also make sure that we spread the drives out evenly. MinIO server expands ellipses passed as arguments. Here is a sample expansion to demonstrate the process.

```
minio server http://host{1...2}/export{1...8}
```

Expected expansion

```
> http://host1/export1
> http://host2/export1
> http://host1/export2
> http://host2/export2
> http://host1/export3
> http://host2/export3
> http://host1/export4
> http://host2/export4
> http://host1/export5
> http://host2/export5
> http://host1/export6
> http://host2/export6
> http://host1/export7
> http://host2/export7
> http://host1/export8
> http://host2/export8
```

*A noticeable trait of this expansion is that it chooses unique hosts such the setup provides maximum protection and availability.*

- Choosing an erasure set for the object is decided during `PutObject()`, object names are used to find the right erasure set using the following pseudo code.

```go
// hashes the key returning an integer.
func sipHashMod(key string, cardinality int, id [16]byte) int {
        if cardinality <= 0 {
                return -1
        }
        sip := siphash.New(id[:])
        sip.Write([]byte(key))
        return int(sip.Sum64() % uint64(cardinality))
}
```

Input for the key is the object name specified in `PutObject()`, returns a unique index. This index is one of the erasure sets where the object will reside. This function is a consistent hash for a given object name i.e for a given object name the index returned is always the same.

- Write and Read quorum are required to be satisfied only across the erasure set for an object. Healing is also done per object within the erasure set which contains the object.

- MinIO does erasure coding at the object level not at the volume level, unlike other object storage vendors. This allows applications to choose different storage class by setting `x-amz-storage-class=STANDARD/REDUCED_REDUNDANCY` for each object uploads so effectively utilizing the capacity of the cluster. Additionally these can also be enforced using IAM policies to make sure the client uploads with correct HTTP headers.

- MinIO also supports expansion of existing clusters in server pools. Each pool is a self contained entity with same SLA's (read/write quorum) for each object as original cluster. By using the existing namespace for lookup validation MinIO ensures conflicting objects are not created. When no such object exists then MinIO simply uses the least used pool to place new objects.

### There are no limits on how many server pools can be combined

```
minio server http://host{1...32}/export{1...32} http://host{1...12}/export{1...12}
```

In above example there are two server pools

- 32 * 32 = 1024 drives pool1
- 12 * 12 = 144 drives pool2

> Notice the requirement of common SLA here original cluster had 1024 drives with 16 drives per erasure set with default parity of '4', second pool is expected to have a minimum of 8 drives per erasure set to match the original cluster SLA (parity count) of '4'. '12' drives stripe per erasure set in the second pool satisfies the original pool's parity count.

Refer to the sizing guide with details on the default parity count chosen for different erasure stripe sizes [here](https://github.com/minio/minio/blob/master/docs/distributed/SIZING.md)

MinIO places new objects in server pools based on proportionate free space, per pool. Following pseudo code demonstrates this behavior.

```go
func getAvailablePoolIdx(ctx context.Context) int {
        serverPools := z.getServerPoolsAvailableSpace(ctx)
        total := serverPools.TotalAvailable()
        // choose when we reach this many
        choose := rand.Uint64() % total
        atTotal := uint64(0)
        for _, pool := range serverPools {
                atTotal += pool.Available
                if atTotal > choose && pool.Available > 0 {
                        return pool.Index
                }
        }
        // Should not happen, but print values just in case.
        panic(fmt.Errorf("reached end of serverPools (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
}
```

## Other usages

### Advanced use cases with multiple ellipses

Standalone erasure coded configuration with 4 sets with 16 drives each, which spawns drives across controllers.

```
minio server /mnt/controller{1...4}/data{1...16}
```

Standalone erasure coded configuration with 16 sets, 16 drives per set, across mounts and controllers.

```
minio server /mnt{1...4}/controller{1...4}/data{1...16}
```

Distributed erasure coded configuration with 2 sets, 16 drives per set across hosts.

```
minio server http://host{1...32}/disk1
```

Distributed erasure coded configuration with rack level redundancy 32 sets in total, 16 drives per set.

```
minio server http://rack{1...4}-host{1...8}.example.net/export{1...16}
```
