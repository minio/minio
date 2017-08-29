[![Godoc Reference](https://godoc.org/github.com/minio/sio?status.svg)](https://godoc.org/github.com/minio/sio)
[![Travis CI](https://travis-ci.org/minio/sio.svg?branch=master)](https://travis-ci.org/minio/sio)
[![Go Report Card](https://goreportcard.com/badge/minio/sio)](https://goreportcard.com/report/minio/sio)

# Secure IO
## Go implementation of the Data At Rest Encryption (DARE) format.

## Introduction

It is a common problem to store data securely - especially on untrusted remote storage. 
One solution to this problem is cryptography. Before data is stored it is encrypted
to ensure that the data is confidential. Unfortunately encrypting data is not enough to
prevent more sophisticated attacks. Anyone who has access to the stored data can try to
manipulate the data - even if the data is encrypted.

To prevent these kinds of attacks the data must be encrypted in a tamper-resistant way.
This means an attacker should not be able to:
 - Read the stored data - this is achieved by modern encryption algorithms.
 - Modify the data by changing parts of the encrypted data.
 - Rearrange or reorder parts of the encrypted data. 

Authenticated encryption schemes (AE) - like AES-GCM or ChaCha20-Poly1305 - encrypt and
authenticate data. Any modification to the encrypted data (ciphertext) is detected while
decrypting the data. But even an AE scheme alone is not sufficiently enough to prevent all
kinds of data manipulation.

All modern AE schemes produce an authentication tag which is verified after the ciphertext
is decrypted. If a large amount of data is decrypted it is not always possible to buffer
all decrypted data until the authentication tag is verified. Returning unauthenticated 
data has the same issues like encrypting data without authentication.

Splitting the data into small chunks fixes the problem of deferred authentication checks
but introduces a new one. The chunks can be reordered - e.g. exchanging chunk 1 and 2 - 
because every chunk is encrypted separately. Therefore the order of the chunks must be
encoded somehow into the chunks itself to be able to detect rearranging any number of 
chunks.     

This project specifies a [format](https://github.com/minio/sio/blob/master/DARE.md) for 
en/decrypting an arbitrary data stream and gives some [recommendations](https://github.com/minio/sio/blob/master/DARE.md#appendices)
about how to use and implement data at rest encryption (DARE). Additionally this project
provides a reference implementation in Go.  

## Applications

DARE is designed with simplicity and efficiency in mind. It combines modern AE schemes
with a very simple reorder protection mechanism to build a tamper-resistant encryption
scheme. DARE can be used to encrypt files, backups and even large object storage systems.

Its main properties are:
 - Security and high performance by relying on modern AEAD ciphers
 - Small overhead - encryption increases the amount of data by ~0.05%
 - Support for long data streams - up to 256 TB under the same key  
 - Random access - arbitrary sequences / ranges can be decrypted independently

**Install:** `go get -u github.com/minio/sio`

DARE and `github.com/minio/sio` are finalized and can be used in production.

## Performance

Cipher            |   8 KB   |   64 KB   |   512 KB  |  1 MB
----------------- | -------- | --------- | --------- | --------
AES_256_GCM       |  90 MB/s | 1.96 GB/s | 2.64 GB/s | 2.83 GB/s
CHACHA20_POLY1305 |  97 MB/s | 1.23 GB/s | 1.54 GB/s | 1.57 GB/s

*On i7-6500U 2 x 2.5 GHz | Linux 4.10.0-32-generic | Go 1.8.3 | AES-NI & AVX2*
