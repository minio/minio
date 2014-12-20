## Introduction

`minio-cli` is a stub builder for new commands,options on top of [codegangsta/cli](https://github.com/codegangsta/cli),

Idea behind providing a simple tool for rapid prototyping and encouraging new contributors to the project

## Usage

You just need to set its command name and options:

```bash
$ minio-cli -options option1,option2,option3 [command]
```

Generates three files namely [command].go, [command]-options.go, [command].md

## Example

If you want to start to building `bucket` command which has options `get`, `put`, `list`:

```bash
$ minio-cli -options get,put,list bucket
$ ls bucket/
bucket-options.go  bucket.go  bucket.md
```
