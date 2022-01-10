## Decommissioning

### How to decommission a pool?
```
λ mc admin decommission start alias/ http://minio{1...2}/data{1...4}
```

### Status decommissioning a pool

#### Decommissioning without args lists all pools
```
λ mc admin decommission status alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬────────┐
│ ID  │ Pools                           │ Capacity                         │ Status │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Active │
│ 2nd │ http://minio{3...4}/data{1...4} │ 329 GiB (used) / 421 GiB (total) │ Active │
└─────┴─────────────────────────────────┴──────────────────────────────────┴────────┘
```

#### Decommissioning status
```
λ mc admin decommission status alias/ http://minio{1...2}/data{1...4}
Progress: ===================> [1GiB/sec] [15%] [4TiB/50TiB]
Time Remaining: 4 hours (started 3 hours ago)
```

#### A pool not under decommissioning will throw an error
```
λ mc admin decommission status alias/ http://minio{1...2}/data{1...4}
ERROR: This pool is not scheduled for decommissioning currently.
```

### Canceling a decommission?
Stop an on-going decommission in progress, mainly used in situations when the load may be
too high and you may want to schedule the decommission at a later point in time.

`mc admin decommission cancel` without an argument, lists out any on-going decommission in progress.

```
λ mc admin decommission cancel alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬──────────┐
│ ID  │ Pools                           │ Capacity                         │ Status   │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining │
└─────┴─────────────────────────────────┴──────────────────────────────────┴──────────┘
```

> NOTE: Canceled decommission will not make the pool active again, since we might have
> Potentially partial duplicate content on the other pools, to avoid this scenario be
> absolutely sure to start decommissioning as a planned activity.

```
λ mc admin decommission cancel alias/ http://minio{1...2}/data{1...4}
┌─────┬─────────────────────────────────┬──────────────────────────────────┬────────────────────┐
│ ID  │ Pools                           │ Capacity                         │ Status             │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining(Canceled) │
└─────┴─────────────────────────────────┴──────────────────────────────────┴────────────────────┘
```

If for some reason decommission fails in between, the `status` will indicate decommission as failed instead.
```
λ mc admin decommission status alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬──────────────────┐
│ ID  │ Pools                           │ Capacity                         │ Status           │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining(Failed) │
│ 2nd │ http://minio{3...4}/data{1...4} │ 329 GiB (used) / 421 GiB (total) │ Active           │
└─────┴─────────────────────────────────┴──────────────────────────────────┴──────────────────┘
```

### Restart a canceled or failed decommission?

```
λ mc admin decommission start alias/ http://minio{1...2}/data{1...4}
```
