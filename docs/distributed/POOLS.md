## Decommissioning

### How to decommission a pool?
```
$ mc admin decomission start alias/ http://minio{1...2}/data{1...4}
```

### Status decomissioning a pool

#### Decommissioning without args lists all pools
```
$ mc admin decomission status alias/
| ID | Pools                           | Capacity             | Status   |
|----|---------------------------------|----------------------|----------|
| 0  | http://minio{1...2}/data{1...4} | N (used) / M (total) | Active   |
| 1  | http://minio{3...4}/data{1...4} | N (used) / M (total) | Draining |
```

#### Decommissioning status
```
$ mc admin decomission status alias/ http://minio{1...2}/data{1...4}
Progress: ===================> [1GiB/sec] [15%] [4TiB/50TiB]
Time Remaining: 4 hours (started 3 hours ago)
```

#### A pool not under decomissioning will throw an error
```
$ mc admin decomission status alias/ http://minio{1...2}/data{1...4}
ERROR: This pool is not scheduled for decomissioning currently.
```

### Canceling a decommissioned pool (to make it again active)
```
$ mc admin decomission cancel alias/
| ID | Pools                           | Capacity             | Status   |
|----|---------------------------------|----------------------|----------|
| 0  | http://minio{1...2}/data{1...4} | N (used) / M (total) | Draining |
```

```
~ mc admin decomission cancel alias/  http://minio{1...2}/data{1...4}
| ID | Pools                           | Capacity             | Status |
|----|---------------------------------|----------------------|--------|
| 0  | http://minio{1...2}/data{1...4} | N (used) / M (total) | Active |
```
