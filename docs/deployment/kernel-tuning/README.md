# Kernel Tuning for MinIO Production Deployment on Linux Servers [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

Following are the recommended settings, a copy of this [script](https://github.com/minio/minio/blob/master/docs/deployment/kernel-tuning/sysctl.sh) is available here to be applied on Linux servers.

> NOTE: Although these settings are generally good on Linux servers, users must be careful on any premature tuning. These tunings are generally considered good to have but not mandatory, these settings do not fix any hardware issues and should not be considered as an alternative to boost performance. Under most circumstances this tuning is to be done after performing baseline performance tests for the hardware with expected results.

```
#!/bin/bash
cat > sysctl.conf <<EOF
# maximum number of open files/file descriptors
fs.file-max = 4194303

# use as little swap space as possible
vm.swappiness = 1

# prioritize application RAM against disk/swap cache
vm.vfs_cache_pressure = 10

# minimum free memory
vm.min_free_kbytes = 1000000

# maximum receive socket buffer (bytes)
net.core.rmem_max = 268435456

# maximum send buffer socket buffer (bytes)
net.core.wmem_max = 268435456

# default receive buffer socket size (bytes)
net.core.rmem_default = 67108864

# default send buffer socket size (bytes)
net.core.wmem_default = 67108864

# maximum number of packets in one poll cycle
net.core.netdev_budget = 1200

# maximum ancillary buffer size per socket
net.core.optmem_max = 134217728

# maximum number of incoming connections
net.core.somaxconn = 65535

# maximum number of packets queued
net.core.netdev_max_backlog = 250000

# maximum read buffer space
net.ipv4.tcp_rmem = 67108864 134217728 268435456

# maximum write buffer space
net.ipv4.tcp_wmem = 67108864 134217728 268435456

# enable low latency mode
net.ipv4.tcp_low_latency = 1

# socket buffer portion used for TCP window
net.ipv4.tcp_adv_win_scale = 1

# queue length of completely established sockets waiting for accept
net.ipv4.tcp_max_syn_backlog = 30000

# maximum number of sockets in TIME_WAIT state
net.ipv4.tcp_max_tw_buckets = 2000000

# reuse sockets in TIME_WAIT state when safe
net.ipv4.tcp_tw_reuse = 1

# time to wait (seconds) for FIN packet
net.ipv4.tcp_fin_timeout = 5

# disable icmp send redirects
net.ipv4.conf.all.send_redirects = 0

# disable icmp accept redirect
net.ipv4.conf.all.accept_redirects = 0

# drop packets with LSR or SSR
net.ipv4.conf.all.accept_source_route = 0

# MTU discovery, only enable when ICMP blackhole detected
net.ipv4.tcp_mtu_probing = 1

EOF

echo "Enabling system level tuning params"
sysctl --quiet --load sysctl.conf && rm -f sysctl.conf

# `Transparent Hugepage Support`*: This is a Linux kernel feature intended to improve
# performance by making more efficient use of processor’s memory-mapping hardware.
# But this may cause https://blogs.oracle.com/linux/performance-issues-with-transparent-huge-pages-thp
# for non-optimized applications. As most Linux distributions set it to `enabled=always` by default,
# we recommend changing this to `enabled=madvise`. This will allow applications optimized
# for transparent hugepages to obtain the performance benefits, while preventing the
# associated problems otherwise. Also, set `transparent_hugepage=madvise` on your kernel
# command line (e.g. in /etc/default/grub) to persistently set this value.

echo "Enabling THP madvise"
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
```
