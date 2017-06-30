## Polynomial authentication schemes

A polynomial authenticator is basically a message authentication scheme. Therefore
the polynomial authenticator takes a secret key and a message of arbitrary size and
computes a fixed size authentication tag.

It is critical for the security of the authenticator that the one key is never used for more than one message - so never reuse a key. Authenticating two different messages with the same key allows an attacker to forge messages at will.

This package provides a general interface for polynomial authenticators and natively supports two commonly used polynomials:
 - **GHASH** used by the AES-GCM AEAD construction
 - **Poly1305** used by the ChaCha20Poly1305 AEAD construction

Those both polynomial authenticators are used in TLS.

### Install

Install in your GOPATH: `go get github.com/aead/poly`

### Performance

For GHASH benchmarks see: [GHASH implementation](https://github.com/aead/ghash)  
For Poly1305 benchmarks see: [Poly1305 implementation](https://github.com/aead/poly1305)  
