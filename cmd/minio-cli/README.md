## Introduction

`minio-cli` is option stub builder for ``minio`` project using [codegangsta/cli](https://github.com/codegangsta/cli),

The idea is to be able to do rapid prototyping and facilitate new contributors to the project

## Usage

You just need to set its application name and options:

```bash
$ minio-cli -options option1,option2,option3 [application]
```

Generates three files namely [application].go, [application]-options.go, [application].md

## Example

If you want to start to building `bucket` application which has subcommands `get`, `put`, `list`:

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
