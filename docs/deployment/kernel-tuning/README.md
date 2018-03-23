# Kernel Tuning for Minio Production Deployment on Linux Servers [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

## Tuning Network Parameters

Following network parameter settings can help ensure optimal Minio server performance on production workloads.

- *`tcp_fin_timeout`* : A socket left in memory takes approximately 1.5Kb of memory. It makes sense to close the unused sockets preemptively to ensure no memory leakage. This way, even if a peer doesn't close the socket due to some reason, the system itself closes it after a timeout. `tcp_fin_timeout` variable defines this timeout and tells kernel how long to keep sockets in the state FIN-WAIT-2. We recommend setting it to 30. You can set it as shown below

```sh
sysctl -w net.ipv4.tcp_fin_timeout=30
```

- *`tcp_keepalive_probes`* : This variable defines the number of unacknowledged probes to be sent before considering a connection dead. You can set it as shown below

```sh
sysctl -w net.ipv4.tcp_keepalive_probes=5
```

- *`wmem_max`*: This parameter sets the max OS send buffer size for all types of connections.

```sh
sysctl -w net.core.wmem_max=540000
```

- *`rmem_max`*: This parameter sets the max OS receive buffer size for all types of connections.

```sh
sysctl -w net.core.rmem_max=540000
```

## Tuning Virtual Memory

Recommended virtual memory settings are as follows.

- *`swappiness`* : This parameter controls the relative weight given to swapping out runtime memory, as opposed to dropping pages from the system page cache. It takes values from 0 to 100, both inclusive. We recommend setting it to 10.

```sh
sysctl -w vm.swappiness=10
```

- *`dirty_background_ratio`*: This is the percentage of system memory that can be filled with `dirty` pages, i.e. memory pages that still need to be written to disk. We recommend writing the data to the disk as soon as possible. To do this, set the `dirty_background_ratio` to 1.

```sh
sysctl -w vm.dirty_background_ratio=1
```

- *`dirty_ratio`*: This defines is the absolute maximum amount of system memory that can be filled with dirty pages before everything must get committed to disk.

```sh
sysctl -w vm.dirty_ratio=5
```

- *`Transparent Hugepage Support`*: This is a Linux kernel feature intended to improve performance by making more efficient use of processor’s memory-mapping hardware. But this may cause [problems](https://blogs.oracle.com/linux/performance-issues-with-transparent-huge-pages-thp) for non-optimized applications. As most Linux distributions set it to `enabled=always` by default, we recommend changing this to `enabled=madvise`. This will allow applications optimized for transparent hugepages to obtain the performance benefits, while preventing the associated problems otherwise.

```sh
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```

Also, set `transparent_hugepage=madvise` on your kernel command line (e.g. in /etc/default/grub) to persistently set this value.

## Tuning Scheduler

Proper scheduler configuration makes sure Minio process gets adequate CPU time. Here are the recommended scheduler settings

- *`sched_min_granularity_ns`*: This parameter decides the minimum time a task will be be allowed to run on CPU before being pre-empted out. We recommend setting it to 10ms.

```sh
sysctl -w kernel.sched_min_granularity_ns=10000000
```

- *`sched_wakeup_granularity_ns`*: Lowering this parameter improves wake-up latency and throughput for latency critical tasks, particularly when a short duty cycle load component must compete with CPU bound components.

```sh
sysctl -w kernel.sched_wakeup_granularity_ns=15000000
```

## Tuning Disks

The recommendations for disk tuning are conveniently packaged in a well commented [shell script](https://github.com/minio/minio/blob/master/docs/deployment/kernel-tuning/disk-tuning.sh). Please review the shell script for our recommendations.
