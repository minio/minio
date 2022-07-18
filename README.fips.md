# MinIO FIPS Builds

MinIO creates FIPS builds using a patched version of the Go compiler (that uses BoringCrypto, from BoringSSL, which is [FIPS 140-2 validated](https://csrc.nist.gov/csrc/media/projects/cryptographic-module-validation-program/documents/security-policies/140sp2964.pdf)) published by the Golang Team [here](https://github.com/golang/go/tree/dev.boringcrypto/misc/boring).

MinIO FIPS executables are available at <http://dl.min.io> - they are only published for `linux-amd64` architecture as binary files with the suffix `.fips`. We also publish corresponding container images to our official image repositories.

We are not making any statements or representations about the suitability of this code or build in relation to the FIPS 140-2 standard. Interested users will have to evaluate for themselves whether this is useful for their own purposes.
