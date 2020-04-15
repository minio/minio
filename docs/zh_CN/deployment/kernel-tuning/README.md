# Linux服务器上MinIO生产环境的内核调优 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)  [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## 调优网络参数

以下网络参数设置可帮助确保Minio服务器在生产环境负载上的最佳性能。

- *`tcp_fin_timeout`* : 一个socket连接大约需要1.5KB的内存，关闭未使用的socket连接可以减少内存占用，避免出现内存泄露。即使另一方由于某种原因没有关闭socket连接，系统本身也会在到达超时时间时断开连接。 `tcp_fin_timeout`参数定义了内核保持sockets在FIN-WAIT-2状态的超时时间。我们建议设成20，你可以按下面的示例进行设置。

```sh
sysctl -w net.ipv4.tcp_fin_timeout=30
```

- *`tcp_keepalive_probes`* : 这个参数定义了经过几次无回应的探测之后，认为连接断开了。你可以按下面的示例进行设置。

```sh
sysctl -w net.ipv4.tcp_keepalive_probes=5
```

- *`wmem_max`*: 这个参数定义了针对所有类型的连接，操作系统的最大发送buffer大小。

```sh
sysctl -w net.core.wmem_max=540000
```

- *`rmem_max`*: 这个参数定义了针对所有类型的连接，操作系统最大接收buffer大小。

```sh
sysctl -w net.core.rmem_max=540000
```

## 调优虚拟内存

下面是推荐的虚拟内存设置。

- *`swappiness`* : 此参数控制了交换运行时内存的相对权重，而不是从page缓存中删除page。取值范围是[0,100],我们建议设成10。

```sh
sysctl -w vm.swappiness=10
```

- *`dirty_background_ratio`*: 这个是`脏`页可以占系统内存的百分比，内存页仍需要写到磁盘上。我们建议要尽早将数据写到磁盘上，越早越好。为了达到这个目的，将 `dirty_background_ratio` 设成1。

```sh
sysctl -w vm.dirty_background_ratio=1
```

- *`dirty_ratio`*: 这定义了在所有事务必须提交到磁盘之前，可以用脏页填充的系统内存的绝对最大数量。

```sh
sysctl -w vm.dirty_ratio=5
```

## 调优调度程序

正确的调度程序配置确保Minio进程获得足够的CPU时间。 以下是推荐的调度程序设置。

- *`sched_min_granularity_ns`*: 此参数定义了一个任务在被其它任务抢占时，可在CPU一次运行的最短时间，我们建议设成10ms。

```sh
sysctl -w kernel.sched_min_granularity_ns=10000000
```

- *`sched_wakeup_granularity_ns`*: 降低该参数值可以减少唤醒延迟，提高吞吐量。

```sh
sysctl -w kernel.sched_wakeup_granularity_ns=15000000
```

## 调优磁盘

我们将磁盘调优的建议整合到了注释完备的 [shell script](https://github.com/minio/minio/blob/master/docs/deployment/kernel-tuning/disk-tuning.sh)，敬请查看。
