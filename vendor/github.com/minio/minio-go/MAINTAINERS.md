# For maintainers only

## Responsibilities

Please go through this link [Maintainer Responsibility](https://gist.github.com/abperiasamy/f4d9b31d3186bbd26522)

### Making new releases
Tag and sign your release commit, additionally this step requires you to have access to Minio's trusted private key.
```sh
$ export GNUPGHOME=/media/${USER}/minio/trusted
$ git tag -s 4.0.0
$ git push
$ git push --tags
```

### Update version
Once release has been made update `libraryVersion` constant in `api.go` to next to be released version.

```sh
$ grep libraryVersion api.go
      libraryVersion = "4.0.1"
```

Commit your changes
```
$ git commit -a -m "Update version for next release" --author "Minio Trusted <trusted@minio.io>"
```

### Announce
Announce new release by adding release notes at https://github.com/minio/minio-go/releases from `trusted@minio.io` account. Release notes requires two sections `highlights` and `changelog`. Highlights is a bulleted list of salient features in this release and Changelog contains list of all commits since the last release.

To generate `changelog`
```sh
$ git log --no-color --pretty=format:'-%d %s (%cr) <%an>' <last_release_tag>..<latest_release_tag>
```
