Introduction
============
Minio is an open source object storage released under Apache license v2.

It uses ``Rubberband Erasure`` coding to dynamically protect the data.
Minio was inspired by Amazon S3 API and Haystack Object Format.

Overview
============
<center>
<picture>
    <source src=https://github.com/Minio-io/minio/raw/master/Minio-Overview.webp type=image/webp >
    <source src=https://github.com/Minio-io/minio/raw/master/Minio-Overview.png type=image/png >
    <img src="https://github.com/Minio-io/minio/raw/master/Minio-Overview.png" alt="Minio Overview Diagram">
</picture>
</center>

Dependencies
============
* go1.3.3
* godep
  * go get github.com/tools/godep
* yasm
* cover
  * go get code.google.com/p/go.tools/cmd/cover or yum install golang-cover

Dependency management
=====================

Install or updating a new dependency
------------------------------------
```sh
go get -u github.com/example/dependency
# import github.com/example/dependency in go src code
godep save ./...
```

Commit all Godep/ modifications, including vendorized files.

Restoring dev environment dependencies
--------------------------------------
```sh
godep restore
```


Compiling
=========
```sh
make
```
