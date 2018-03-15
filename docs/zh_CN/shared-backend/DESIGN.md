介绍 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
------------

该特性可以让多个Minio实例使用一个共享的NAS存储，而且不需要做什么特殊的设置。文件默认已经做了同步以及加锁。

目的
----------

由于Minio的设计理念是为单租户场景服务，所以用户希望采用在一个存储后端上运行多个Minio实例，这个存储后端可能是一个已有的NAS。Minio支持这种共享存储后端的特性，而且不需要用户做额外的设置。


限制
------------

* 如果正在执行GetObject()，则PutObject()会阻塞并等待。
* 如果正在执行PutObject()或者GetObject()，则CompleteMultipartUpload()会阻塞并等待。
* 无法
* A CompleteMultipartUpload() is blocked and waits if another PutObject() or GetObject() is in progress.
* 无法运行FS模式作为remote disk RPC。

## 如何运行?

运行共享存储后端的Minio和直接运行在一块独立磁盘的Minio没有啥区别，不需要做额外设置来开启这个特性。访问NAS上的文件默认就会加锁和同步。以下示例将对您选择的每个操作系统上的操作进行阐述：

### Ubuntu 16.04 LTS

示例1: 运行Minio实例在持载在`/path/to/nfs-volume`路径下的共享后端存储。

On linux server1
```shell
minio server /path/to/nfs-volume
```

On linux server2
```shell
minio server /path/to/nfs-volume
```

### Windows 2012 Server

示例1: 运行Minio实例在持载在`\\remote-server\cifs`路径下的共享后端存储。 

On windows server1
```cmd
minio.exe server \\remote-server\cifs\data
```

On windows server2
```cmd
minio.exe server \\remote-server\cifs\data
```

或者共享存储挂载在`D:\`盘.

On windows server1
```cmd
minio.exe server D:\data
```

On windows server2
```cmd
minio.exe server D:\data
```

架构
------------------

## POSIX/Win32 Locks

### Lock process

在同一个Minio实例中，lock由现有的内存命名空间锁（** sync.RWMutex **等）处理。 为了在许多Minio实例之间同步锁，我们利用Unix上的POSIX`fcntl（）`锁定和Windows`LockFileEx（）`Win32 API）。 如果相邻Minio实例在同一路径上有任何读锁，则写锁请求会被阻塞。 如果有正在进行的写锁，读锁也是如此。

### Unlock process


关闭文件描述符（fd）就会将之前获得的锁释放。关闭fd将告诉内核放弃当前进程在路径上保留的所有锁。当相同进程在同一路径上有多个读操作时，这会变得更加棘手，这意味着关闭一个fd也会为所有并发读释放锁。 为了正确地处理这种情况，实现了简单的fd引用计数，多个读操作之间共享相同的fd。 当读操作开始关闭fd时，我们开始减少引用计数，一旦引用计数达到零，我们可以确保没有更多的活跃读操作。 所以我们继续关闭底层文件描述符，这将放弃在路径上保留的读锁。

这个不适用于写操作，因为对于每个对象总是有一个写和多个读。

## 处理并发。

这里的一个例子显示了如何使用GetObject()处理争用。

GetObject()持有`fs.json`的一个读锁。

```go
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	rlk, err := fs.rwPool.Open(fsMetaPath)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer rlk.Close()

... you can perform other operations here ...

	_, err = io.CopyBuffer(writer, reader, buf)

... after successful copy operation unlocks the read lock ...
```

对同一个对象的并发PutObject操作
在同一个对象上请求一个并发的PutObject, PutObject()尝试获取一个`fs.json`上的写锁。

```go
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	wlk, err := fs.rwPool.Create(fsMetaPath)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	// This close will allow for locks to be synchronized on `fs.json`.
	defer wlk.Close()
```

现在从上面的代码片段可以看到，直到GetObject()返回。 以下部分代码将被阻塞。

```go
	wlk, err := fs.rwPool.Create(fsMetaPath)
```

这咱限制是必须的，以避免给客户端返回损坏的数据。反之亦然，PutObject(),GetObject()也会等待PutObject()完成之后再执行。

### 警告 (并发)

假设有3个Minio服务共享一个存储后端

minio1

- DeleteObject(object1) --> 在object1删除操作时持有`fs.json`的锁。

minio2

- PutObject(object1) --> 等DeleteObject完毕后进行锁定。

minio3

- PutObject(object1) --> (concurrent request during PutObject minio2 checking if `fs.json` exists)

一旦获取到锁之后，minio2验证文件是否真的存在，以避免获得已被删除的fd的锁。但是这种情况与minio3存在竞争，因为minio3也在尝试写同一个文件。这就存在一种可能，`fs.json`已经被创建了，所以minio2获得的锁就无效，这样就可能会导致数据不一致。

这是一种已知的问题，而且没办法通过POSIX fcntl锁来解决。这种情况是共享存储后端的限制，请你知晓。
