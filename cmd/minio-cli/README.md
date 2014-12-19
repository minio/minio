## Introduction

`minio-cli` is cli option stub builder for ``minio`` project on top of [codegangsta/cli](https://github.com/codegangsta/cli),

Ideal for rapid prototyping and encouraging new contributors to the project

## Usage

You just need to set its command name and options:

```bash
$ minio-cli -options option1,option2,option3 [command]
```

Generates three files namely [command].go, [command]-options.go, [application].md

## Example

If you want to start to building `bucket` command which has options `get`, `put`, `list`:

```bash
$ minio-cli -options get,put,list foo
$ ls foo/
foo-options.go  foo.go  foo.md
```

## Installation

```bash
$ go get github.com/minio-io/minio
$ make install
```
