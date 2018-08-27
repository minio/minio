profile
=======

Simple profiling support package for Go

[![Build Status](https://travis-ci.org/pkg/profile.svg?branch=master)](https://travis-ci.org/pkg/profile) [![GoDoc](http://godoc.org/github.com/pkg/profile?status.svg)](http://godoc.org/github.com/pkg/profile)


installation
------------

    go get github.com/pkg/profile

usage
-----

Enabling profiling in your application is as simple as one line at the top of your main function

```go
import "github.com/pkg/profile"

func main() {
    defer profile.Start().Stop()
    ...
}
```

options
-------

What to profile is controlled by config value passed to profile.Start. 
By default CPU profiling is enabled.

```go
import "github.com/pkg/profile"

func main() {
    // p.Stop() must be called before the program exits to
    // ensure profiling information is written to disk.
    p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
    ...
}
```

Several convenience package level values are provided for cpu, memory, and block (contention) profiling.

For more complex options, consult the [documentation](http://godoc.org/github.com/pkg/profile).

contributing
------------

We welcome pull requests, bug fixes and issue reports.

Before proposing a change, please discuss it first by raising an issue.
