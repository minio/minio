# For maintainers only

## Responsibilities

Please go through this link [Maintainer Responsibility](https://gist.github.com/abperiasamy/f4d9b31d3186bbd26522)

### Making new releases

Edit `libraryVersion` constant in `api.go`.

```
$ grep libraryVersion api.go
      libraryVersion = "0.3.0"
```

```
$ git tag 0.3.0
$ git push --tags
```