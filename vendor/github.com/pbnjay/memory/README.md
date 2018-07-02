# memory

Package `memory` provides a single method reporting total physical system memory
accessible to the kernel. It does not account for memory used by other processes.

This package has no external dependency beside the standard library.

Documentation:
[![GoDoc](https://godoc.org/github.com/pbnjay/memory?status.svg)](https://godoc.org/github.com/pbnjay/memory)

This is useful for dynamic code to minimize thrashing and other contention, similar to the stdlib `runtime.NumCPU`
See some history of the proposal at https://github.com/golang/go/issues/21816


## Example

```go
fmt.Printf("Total system memory: %d\n", memory.TotalMemory())
```


## Testing

Tested/working on:
 - macOS 10.12.6 (16G29)
 - Windows 10 1511 (10586.1045)
 - Linux RHEL (3.10.0-327.3.1.el7.x86_64)
 - Raspberry Pi 3 (ARMv8) on Raspbian, ODROID-C1+ (ARMv7) on Ubuntu, C.H.I.P
   (ARMv7).

Tested on virtual machines:
 - Windows 7 SP1 386
 - Debian stretch 386
 - NetBSD 7.1 amd64 + 386
 - OpenBSD 6.1 amd64 + 386
 - FreeBSD 11.1 amd64 + 386
 - DragonFly BSD 4.8.1 amd64

If you have access to untested systems (notably arm) please
test and file bugs if necessary.
