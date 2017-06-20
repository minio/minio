[![Godoc Reference](https://godoc.org/github.com/aead/poly1305?status.svg)](https://godoc.org/github.com/aead/poly1305)

## The poly1305 message authentication code

Poly1305 is a fast, one-time authentication function created by Daniel J. Bernstein.  
It is infeasible for an attacker to generate an authenticator for a message without the key.  
However, a key must only be used for a single message. Authenticating two different messages  
with the same key allows an attacker to forge authenticators for other messages with the same key.

### Installation
Install in your GOPATH: `go get -u github.com/aead/poly1305`

### Requirements
All Go versions >= 1.5.3 are supported.

### Performance

#### AMD64
Hardware: Intel i7-6500U 2.50GHz x 2  
System: Linux Ubuntu 16.04 - kernel: 4.4.0-62-generic  
Go version: 1.8.0  
```
name                 speed              cpb
Sum_64-4             1.67GB/s ± 0%      1.39
Sum_1K-4             2.48GB/s ± 0%      0.93
Write_64-4           1.95GB/s ± 0%      1.19
Write_1K-4           2.51GB/s ± 1%      0.92
```

#### 386
Hardware: Intel i7-6500U 2.50GHz x 2  
System: Linux Ubuntu 16.04 - kernel: 4.4.0-62-generic  
Go version: 1.8.0  
```
name                 speed              cpb
Sum_64-4             163MB/s ± 1%      14.62
Sum_1K-4             234MB/s ± 8%      10.18
Write_64-4           222MB/s ± 2%      10.73
Write_1K-4           250MB/s ± 3%       9.53
```
