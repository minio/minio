# Decommissioning

Decommissiong is a mechanism in MinIO to drain older pools (usually with old hardware) and migrate the content from such pools to a newer pools (usually better hardware). Decommissioning spreads the data across all pools - for example, if you decommission `pool1`, all the data from `pool1` spreads across `pool2` and `pool3`.

## Features

- A pool in decommission still allows READ access to all its contents, newer WRITEs will automatically be scheduled to only pools not in decommission status.
- All versioned buckets maintain the same order for "versions" for each object after being decommissioned to the other pools.
- A pool interrupted during the decommission process, such as for a cluster restart, resumes from where it left off.

## How to decommission a pool

```
λ mc admin decommission start alias/ http://minio{1...2}/data{1...4}
```

## Status decommissioning a pool

### Decommissioning without args lists all pools

```
λ mc admin decommission status alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬────────┐
│ ID  │ Pools                           │ Capacity                         │ Status │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Active │
│ 2nd │ http://minio{3...4}/data{1...4} │ 329 GiB (used) / 421 GiB (total) │ Active │
└─────┴─────────────────────────────────┴──────────────────────────────────┴────────┘
```

### Decommissioning status

```
λ mc admin decommission status alias/ http://minio{1...2}/data{1...4}
Decommissioning rate at 36 MiB/sec [4 TiB/50 TiB]
Started: 1 minute ago
```

Once it is **Complete**

```
λ mc admin decommission status alias/ http://minio{1...2}/data{1...4}
Decommission of pool http://minio{1...2}/data{1...4} is complete, you may now remove it from server command line
```

### A pool not under decommissioning will throw an error

```
λ mc admin decommission status alias/ http://minio{1...2}/data{1...4}
ERROR: This pool is not scheduled for decommissioning currently.
```

## Canceling a decommission

Stop an on-going decommission in progress, mainly used in situations when the load may be too high and you may want to schedule the decommission at a later point in time.

`mc admin decommission cancel` without an argument, lists out any on-going decommission in progress.

```
λ mc admin decommission cancel alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬──────────┐
│ ID  │ Pools                           │ Capacity                         │ Status   │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining │
└─────┴─────────────────────────────────┴──────────────────────────────────┴──────────┘
```

> NOTE: Canceled decommission will not make the pool active again, since we might have  potentially partial namespace on the other pools, to avoid this scenario be absolutely sure to make decommissioning a planned well thought activity. This is not to be run on a daily basis.

```
λ mc admin decommission cancel alias/ http://minio{1...2}/data{1...4}
┌─────┬─────────────────────────────────┬──────────────────────────────────┬────────────────────┐
│ ID  │ Pools                           │ Capacity                         │ Status             │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining(Canceled) │
└─────┴─────────────────────────────────┴──────────────────────────────────┴────────────────────┘
```

If the decommission process fails for any reason, the status indicates failed.

```
λ mc admin decommission status alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬──────────────────┐
│ ID  │ Pools                           │ Capacity                         │ Status           │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Draining(Failed) │
│ 2nd │ http://minio{3...4}/data{1...4} │ 329 GiB (used) / 421 GiB (total) │ Active           │
└─────┴─────────────────────────────────┴──────────────────────────────────┴──────────────────┘
```

## Restart a canceled or failed decommission

```
λ mc admin decommission start alias/ http://minio{1...2}/data{1...4}
```

## When decommission is 'Complete'

Once decommission is complete, it will be indicated with *Complete* status.  *Complete* means that now you can now safely remove the first pool argument from the MinIO command line.

```
λ mc admin decommission status alias/
┌─────┬─────────────────────────────────┬──────────────────────────────────┬──────────┐
│ ID  │ Pools                           │ Capacity                         │ Status   │
│ 1st │ http://minio{1...2}/data{1...4} │ 439 GiB (used) / 561 GiB (total) │ Complete │
│ 2nd │ http://minio{3...4}/data{1...4} │ 329 GiB (used) / 421 GiB (total) │ Active   │
└─────┴─────────────────────────────────┴──────────────────────────────────┴──────────┘
```

- On baremetal setups, if you have `MINIO_VOLUMES="http://minio{1...2}/data{1...4} http://minio{3...4}/data{1...4}"`, you can remove the first argument `http://minio{1...2}/data{1...4}` to update your `MINIO_VOLUMES` setting, then restart all the servers in the setup in parallel using `systemctl restart minio`.

- On Kubernetes setups, the statefulset specification needs to be modified by changing the command line input for the MinIO container. Once the relevant changes are done, proceed to execute `kubectl apply -f statefulset.yaml`.

- On Operator based MinIO deployments, you need to modify the `tenant.yaml` specification and modify the `pools:` section from two entries to a single entry. After making relevant changes, proceed to execute `kubectl apply -f tenant.yaml`.

> Without a 'Complete' status any 'Active' or 'Draining' pool(s) are not allowed to be removed once configured.

## NOTE

- Empty delete markers (such as for objects with no other successor versions) do not transition to the new pool to avoid creating empty metadata on the other pool(s). If you believe transitioning empty delete markers is required, open a GitHub issue.

## TODO

- Richer progress UI is not present at the moment, this will be addressed in subsequent releases. Currently however a RATE of data transfer and usage increase is displayed via `mc`.
- Transitioned Hot Tier's as pooled setups are not currently supported, attempting to decommission buckets with ILM Transition will be rejected by the server. This will be supported in future releases.
- Embedded Console UI does not support Decommissioning through the UI yet. This will be supported in future releases.
