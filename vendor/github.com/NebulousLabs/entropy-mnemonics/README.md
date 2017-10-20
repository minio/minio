# entropy-mnemonics

[![Build Status](https://travis-ci.org/NebulousLabs/entropy-mnemonics.svg?branch=master)](https://travis-ci.org/NebulousLabs/entropy-mnemonics)
[![Documentation](https://img.shields.io/badge/docs-godoc-blue.svg)](https://godoc.org/github.com/NebulousLabs/entropy-mnemonics)

mnemonics is a golang package that converts byte slices into human-friendly
phrases. The primary purpose is to assist with the generation of
cryptographically secure passwords. The threshold for a cryptographically
secure password is between 128 and 256 bits, which when converted to base64 is
22-43 random characters. Random characters are both difficult to remember and
subject to error when spoken or written down; slurring or sloppy handwriting
can make it difficult to recover a password.

These considerations may seem strange to those who use password managers; why
write down the password at all? The answer is: healthy paranoia. Retaining a
physical copy of a password protects the user from disk failure and malware.

mnemonics solves these problems by converting byte slices into human-intelligible
phrases. Take the following 128 bit example:

```
Hex:      a26a4821e36c7f7dccaa5484c080cefa
Base64:   ompIIeNsf33MqlSEwIDO+g==
Mnemonic: austere sniff aching hiding pact damp focus tacit timber pram left wonders
```

Though more verbose, the mnemonic phrase is less prone to human transcription errors.

The words are chosen from a dictionary of size 1626, such that a 12-word phrase
corresponds to almost exactly 128 bits of entropy. Note that only the first few
characters of each word need be unique; for the English dictionary, 3 characters
are sufficient. This means that passphrases can be altered to make them more
understandable or more easily memorized. For example, the phrase "austere sniff aching"
could be changed to "austere sniff achoo" and the phrase would still decode correctly.

Full UTF-8 support is available for dictionaries, including input normalization
for inputs with [canonical equivalence](https://en.wikipedia.org/wiki/Unicode_equivalence).

### Supported Dictionaries ###

+ English (prefix size: 3)
+ German (prefix size: 4)
+ Japanese (prefix size: 3)
