# Data At Rest Encryption (DARE) - Version 1.0

**This is a draft**

## 1. Introduction

This document describes the Data At Rest Encryption (DARE) format for encrypting
data in a tamper-resistant way. DARE is designed to securely encrypt data stored
on (untrusted) storage providers.

## 2. Overview

DARE specifies how to split an arbitrary data stream into small chunks (packages)
and concatenate them into a tamper-proof chain. Tamper-proof means that an attacker
is not able to:
 - decrypt one or more packages.
 - modify the content of one or more packages.
 - reorder/rearrange one or more packages.

An attacker is defined as somebody who has full access to the encrypted data
but not to the encryption key. An attacker can also act as storage provider.

### 2.1 Cryptographic Notation

DARE will use the following notations:
 - The set **{a,b}** means select **one** of the provided values **a**, **b**.
 - The concatenation of the byte sequences **a** and **b** is **a || b**.
 - The function **len(seq)** returns the length of a byte sequence **seq** in bytes.
 - The index access **seq[i]** accesses one byte at index **i** of the sequence **seq**.
 - The range access **seq[i : j]** accesses a range of bytes starting at **i** (inclusive)
   and ending at **j** (exclusive).
 - The compare functions **a == b => f** and **a != b => f** succeed when **a** 
   is equal to **b** and **a** is not equal to **b** respectively and execute the command **f**.
 - The function **CTC(a, b)** returns **1** only if **a** and **b** are equal, 0 otherwise.
   CTC compares both values in **constant time**.
 - **ENC(key, nonce, plaintext, addData)** represents the byte sequence which is
   the output from an AEAD cipher authenticating the *addData*, encrypting and
   authenticating the *plaintext* with the secret encryption *key* and the *nonce*.
 - **DEC(key, nonce, ciphertext, addData)** represents the byte sequence which is
   the output from an AEAD cipher verifying the integrity of the *ciphertext* &
   *addData* and decrypting the *ciphertext* with the secret encryption *key* and
   the *nonce*. The decryption **always** fails if the integrity check fails. 

All numbers must be converted into byte sequences by using the little endian byte
order. An AEAD cipher will be either AES-256_GCM or CHACHA20_POLY1305. 

## 2.2 Keys

Both ciphers - AES-256_GCM and CHACHA20_POLY1305 - require a 32 byte key. The key
**must** be unique for one encrypted data stream. Reusing a key **compromises**
some security properties provided by DARE. See Appendix A for recommendations
about generating keys and preventing key reuse. 

## 2.3 Errors

DARE defines the following errors:
 - **err_unsupported_version**: Indicates that the header version is not supported.
 - **err_unsupported_cipher**: Indicates that the cipher suite is not supported.
 - **err_missing_header**: Indicates that the payload header is missing or incomplete.
 - **err_payload_too_short**: Indicates that the actual payload size is smaller than the
  payload size field of the header.
 - **err_package_out_of_order**: Indicates that the sequence number of the package does
   not match the expected sequence number.
 - **err_tag_mismatch**: Indicates that the tag of the package does not match the tag
   computed while decrypting the package.

## 3. Package Format

DARE splits an arbitrary data stream into a sequence of packages. Each package is
encrypted separately. A package consists of a header, a payload and an authentication
tag.

Header   | Payload        | Tag
---------|----------------|---------
16 bytes | 1 byte - 64 KB | 16 bytes

The header contains information about the package. It consists of:

Version | Cipher suite | Payload size     | Sequence number  | nonce
--------|--------------|------------------|------------------|---------
1 byte  | 1 byte       | 2 bytes / uint16 | 4 bytes / uint32 | 8 bytes

The first byte specifies the version of the format and is equal to 0x10 for DARE 
version 1.0. The second byte specifies the cipher used to encrypt the package.

Cipher            | Value
------------------|-------
AES-256_GCM       | 0x00
CHACHA20_POLY1305 | 0x01

The payload size is an uint16 number. The real payload size is defined as the payload
size field as uint32 + 1. This ensures that the payload can be exactly 64 KB long and
prevents empty packages without a payload.

The sequence number is an uint32 number identifying the package within a sequence of
packages. It is a monotonically increasing number. The sequence number **must** be 0 for
the first package and **must** be incremented for every subsequent package. The 
sequence number of the n-th package is n-1. This means a sequence of packages can consist
of 2 ^ 32 packages and each package can hold up to 64 KB data. The maximum size
of a data stream is limited by `64 KB * 2^32 = 256 TB`. This should be sufficient
for current use cases. However, if necessary, the maximum size of a data stream can increased
in the future by slightly tweaking the header (with a new version).

The nonce **should** be a random value for each data stream and **should** be kept constant
for all its packages. Even if a key is accidentally used
twice to encrypt two different data streams an attacker should not be able to decrypt one
of those data streams. However, an attacker is always able to exchange corresponding packages
between the streams whenever a key is reused. DARE is only tamper-proof when the encryption
key is unique. See Appendix A.

The payload contains the encrypted data. It must be at least 1 byte long and can contain a maximum of 64 KB.

The authentication tag is generated by the AEAD cipher while encrypting and authenticating the
package. The authentication tag **must** always be verified while decrypting the package.
Decrypted content **must never** be returned before the authentication tag is successfully 
verified.

## 4. Encryption

DARE encrypts every package separately. The header version, cipher suite and nonce **should**
be the same for all encrypted packages of one data stream. It is **recommended** to not change
this values within one sequence of packages. The nonce **should** be generated randomly once
at the beginning of the encryption process and repeated in every header. See Appendix B for 
recommendations about generating random numbers.

The sequence number is the sequence number of the previous package plus 1. The sequence number
**must** be a monotonically increasing number within one sequence of packages. The sequence number
of the first package is **always** 0.

The payload field is the length of the plaintext in bytes minus 1. The encryption process is
defined as following:

```
header[0]       = 0x10
header[1]       = {AES-256_GCM, CHACHA20_POLY1305}
header[2:4]     = little_endian( len(plaintext) - 1 )
header[4:8]     = little_endian( sequence_number )
header[8:16]    = nonce

payload || tag  = ENC(key, header[4:16], plaintext, header[0:4]) 

sequence_number = sequence_number + 1
```

## 5. Decryption

DARE decrypts every package separately. Every package **must** be successfully decrypted **before**
plaintext is returned. The decryption happens in three steps:

1. Verify that the header version is correct (`header[0] == 0x10`) and the cipher suite is supported.
2. Verify that the sequence number of the packages matches the expected sequence number. It is required
   to save the first expected sequence number at the beginning of the decryption process. After every
   successfully decrypted package this sequence number is incremented by 1. The sequence number of
   all packages **must** match the saved / expected number.
3. Verify that the authentication tag at the end of the package is equal to the authentication tag 
   computed while decrypting the package. This **must** happen in constant time.

The decryption is defined as following:

```
header[0]                          != 0x10                            => err_unsupported_version
header[1]                          != {AES-256_GCM,CHACHA20_POLY1305} => err_unsupported_cipher
little_endian_uint32(header[4:8])  != expected_sequence_number        => err_package_out_of_order

payload_size      := little_endian_uint32(header[2:4]) + 1
plaintext || tag  := DEC(key, header[4:16], ciphertext, header[0:4])

CTC(ciphertext[len(plaintext) : len(plaintext) + 16], tag) != 1       => err_tag_mismatch

expected_sequence_number = expected_sequence_number + 1
``` 

## Security

DARE provides confidentiality and integrity of the encrypted data as long as the encryption key
is never reused. This means that a **different** encryption key **must** be used for every data
stream. See Appendix A for recommendations.  

If the same encryption key is used to encrypt two different data streams, an attacker is able to
exchange packages with the same sequence number. This means that the attacker is able to replace 
any package of a sequence with another package as long as:
 - Both packages are encrypted with the same key.
 - The sequence numbers of both packages are equal.

If two data streams are encrypted with the same key the attacker will not be able to decrypt any
package of those streams without breaking the cipher as long as the nonces are different. To be
more precise the attacker may only be able to decrypt a package if:
 - There is another package encrypted with the same key.
 - The sequence number and nonce of those two packages (encrypted with the same key) are equal.

As long as the nonce of a sequence of packages differs from every other nonce (and the nonce is
repeated within one sequence - which is **recommended**) the attacker will not be able to decrypt
any package. It is not required that the nonce is indistinguishable from a truly random bit sequence.
It is sufficient when the nonces differ from each other in at least one bit.

## Appendices

### Appendix A - Key Derivation from a Master Key

DARE needs a unique encryption key per data stream. The best approach to ensure that the keys
are unique is to derive every encryption key from a master key. Therefore a key derivation function
(KDF) - e.g. HKDF, BLAKE2X or HChaCha20 -  can be used. The master key itself may be derived from
a password using functions like Argon2 or scrypt. Deriving those keys is the responsibility of the
users of DARE.

It is **not recommended** to derive encryption keys from a master key and an identifier (like the
file path). If a different data stream is stored under the same identifier - e.g. overwriting the 
data - the derived key would be the same for both streams.

Instead encryption keys should be derived from a master key and a random value. It is not required
that the random value is indistinguishable from a truly random bit sequence. The random value **must**
be unique but need not be secret - depending on the security properties of the KDF.

To keep this simple: The combination of master key and random value used to derive the encryption key
must be unique all the time.

### Appendix B - Generating random values

DARE does not require random values which are indistinguishable from a truly random bit sequence.
However, a random value **must** never be repeated. Therefore it is **recommended** to use a
cryptographically secure pseudorandom number generator (CSPRNG) to generate random values. Many
operating systems and cryptographic libraries already provide appropriate PRNG implementations.
These implementations should always be preferred over crafting a new one.
