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
