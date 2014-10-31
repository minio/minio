/* $OpenBSD: crypto_api.c,v 1.1 2014/01/08 03:59:46 tedu Exp $ */
/*
 * Public domain. Author: Ted Unangst <tedu@openbsd.org>
 * API compatible reimplementation of functions from nacl
 */
#include <sys/types.h>

#include <string.h>
#include <sha2.h>

#include "crypto_api.h"

int
crypto_hash_sha512(unsigned char *out, const unsigned char *in,
    unsigned long long inlen)
{
	SHA2_CTX ctx;

	SHA512Init(&ctx);
	SHA512Update(&ctx, in, inlen);
	SHA512Final(out, &ctx);
	return 0;
}

int
crypto_verify_32(const unsigned char *x, const unsigned char *y)
{
	return timingsafe_bcmp(x, y, 32) ? -1 : 0;
}
