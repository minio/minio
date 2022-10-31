# Pool rebalance

## Introduction
A MinIO distributed setup consists of one or more server pools. MinIO distributes objects uploaded among these pools proportional to their available drive space. A good proportion of objects uploaded will land on the recently added pools. Over time, objects would be uniformly distributed across all the pools.

Certain workloads may require that objects are distributed uniformly across all pools as much as possible, at all times. To support this, MinIO needs to redistribute objects from existing pools to the newly added pools. This is now possible with the Pool rebalance feature.

## Mechanism
Pool rebalance operates with the goal of converging percentage used space across all server pools. This is done by moving objects from pools that have more objects to those which have lesser, usually because they were recently added.

Let's take an example of a MinIO setup with a single server pool with 300TiB capacity. Over time, let's say its usage reached 200TiB. Now, say a new pool of 300TiB is added. 

```
   | Pool ID| Used space | Capacity |
   |--------|------------|----------|
   | pool-1 |  200 TiB   |  300 TiB |
   | pool-2 |    0 TiB   |  300 TiB |
   |--------|------------|----------|
                         |  600 TiB |
```

After rebalance, assuming no new objects are uploaded meanwhile, we will have,

```
   | Pool ID| Used space | Capacity |
   |--------|------------|----------|
   | pool-1 |  100 TiB   |  300 TiB |
   | pool-2 |  100 TiB   |  300 TiB |
```

## Usage
Pool rebalance can be managed using `mc`. There are 3 subcommands, namely `rebalance-start`, `rebalance-status` and `rebalance-stop`.

### To start a rebalance operation
```
$ mc admin rebalance start myminio/
Rebalance started for myminio
```

### To fetch the current status of an ongoing rebalance operation
```
$ mc admin rebalance status myminio/
Per-pool usage:
┌────────┬────────┐
│ Pool-0 │ Pool-1 │
│ 0.06%  │ 0.06%  │
└────────┴────────┘
Summary: 
Data: 0 B (0 objects, 0 versions) 
Time: 18.377703ms (0s to completion)
```

### To stop an ongoing rebalance operation
```
$ mc admin rebalance stop myminio/
Rebalance stopped for myminio
```

Note: Rebalance operation stops running once it reaches its original goal of converging used space across all pools.

## Monitoring
An ongoing rebalance operation can be monitored using `mc admin trace` like so,

```
$ mc admin trace --call rebalance ALIAS/
2022-08-04T11:02:01.546 [REBALANCE] rebalance.RebalanceObject minio1:9000 mybucket 19/60/1960b8ee-bd7c-4632-bd91-25b14c818ea7 183.677875ms
2022-08-04T11:02:01.729 [REBALANCE] rebalance.RebalanceRemoveObject minio1:9000 mybucket 19/60/1960b8ee-bd7c-4632-bd91-25b14c818ea7 1.719018ms
2022-08-04T11:02:01.732 [REBALANCE] rebalance.RebalanceObject minio1:9000 mybucket 22/2a/222a37c9-f015-437b-bde8-4e1cdf74397d 238.361427ms
2022-08-04T11:02:01.970 [REBALANCE] rebalance.RebalanceRemoveObject minio1:9000 mybucket 22/2a/222a37c9-f015-437b-bde8-4e1cdf74397d 6.284292ms
2022-08-04T11:02:01.980 [REBALANCE] rebalance.RebalanceObject minio1:9000 mybucket 24/d6/24d6943c-ac85-4d2c-aac0-01de030c447d 368.959504ms
2022-08-04T11:02:02.349 [REBALANCE] rebalance.RebalanceRemoveObject minio1:9000 mybucket 24/d6/24d6943c-ac85-4d2c-aac0-01de030c447d 2.131113ms
2022-08-04T11:02:02.352 [REBALANCE] rebalance.RebalanceObject minio1:9000 mybucket 2c/a2/2ca2b424-e8e9-45d3-8eba-ff9ac03b13b9 175.417146ms
...
```
