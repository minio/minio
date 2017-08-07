# go-update: Build self-updating Go programs [![godoc reference](https://godoc.org/github.com/inconshreveable/go-update?status.png)](https://godoc.org/github.com/inconshreveable/go-update)

Package update provides functionality to implement secure, self-updating Go programs (or other single-file targets)
A program can update itself by replacing its executable file with a new version.

It provides the flexibility to implement different updating user experiences
like auto-updating, or manual user-initiated updates. It also boasts
advanced features like binary patching and code signing verification.

Example of updating from a URL:

```go
import (
    "fmt"
    "net/http"

    "github.com/inconshreveable/go-update"
)

func doUpdate(url string) error {
    resp, err := http.Get(url)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    err := update.Apply(resp.Body, update.Options{})
    if err != nil {
        // error handling
    }
    return err
}
```

## Features

- Cross platform support (Windows too!)
- Binary patch application
- Checksum verification
- Code signing verification
- Support for updating arbitrary files

## [equinox.io](https://equinox.io)
[equinox.io](https://equinox.io) is a complete ready-to-go updating solution built on top of go-update that provides:

- Hosted updates
- Update channels (stable, beta, nightly, ...)
- Dynamically computed binary diffs
- Automatic key generation and code
- Release tooling with proper code signing
- Update/download metrics

## API Compatibility Promises
The master branch of `go-update` is *not* guaranteed to have a stable API over time. For any production application, you should vendor
your dependency on `go-update` with a tool like git submodules, [gb](http://getgb.io/) or [govendor](https://github.com/kardianos/govendor).

The `go-update` package makes the following promises about API compatibility:
1. A list of all API-breaking changes will be documented in this README.
1. `go-update` will strive for as few API-breaking changes as possible.

## API Breaking Changes
- **Sept 3, 2015**: The `Options` struct passed to `Apply` was changed to be passed by value instead of passed by pointer. Old API at `28de026`.
- **Aug 9, 2015**: 2.0 API. Old API at `221d034` or `gopkg.in/inconshreveable/go-update.v0`.

## License
Apache
