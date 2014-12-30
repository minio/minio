## Introduction

`new-cmd` is a stub builder for new commands,options on top of [codegangsta/cli](https://github.com/codegangsta/cli),

Idea behind providing a simple tool for rapid prototyping and encouraging new contributors to the project

## Usage

You just need to set its command name and options:

```bash
$ new-cmd --options option1,option2,option3 --usage "This command is best" [commandname]
```

Generates three files [commandname].go, [commandname]-options.go, [commandname].md respectively

## Example

If you want to start to building `bucket` command which has options `get`, `put`, `list`:

```bash
$ new-cmd --options get,put,list --usage "Bucket operations" bucket
$ ls bucket/
bucket-options.go  bucket.go  bucket.md
```
