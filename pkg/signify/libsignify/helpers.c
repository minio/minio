/*	$OpenBSD: src/lib/libc/hash/helper.c,v 1.11 2014/04/03 17:55:27 beck Exp $ */

/*
 * Copyright (c) 2000 Poul-Henning Kamp <phk@FreeBSD.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * If we meet some day, and you think this stuff is worth it, you
 * can buy me a beer in return. Poul-Henning Kamp
 */

#include <sys/param.h>
#include <sys/stat.h>

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sha2.h>

/* ARGSUSED */
char *
SHA256End(SHA2_CTX *ctx, char *buf)
{
	int i;
	u_int8_t digest[SHA256_DIGEST_LENGTH];
	static const char hex[] = "0123456789abcdef";

	if (buf == NULL && (buf = malloc(SHA256_DIGEST_STRING_LENGTH)) == NULL)
		return (NULL);

	SHA256Final(digest, ctx);
	for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		buf[i + i] = hex[digest[i] >> 4];
		buf[i + i + 1] = hex[digest[i] & 0x0f];
	}
	buf[i + i] = '\0';
	memset(digest, 0, sizeof(digest));
	return (buf);
}

char *
SHA256FileChunk(const char *filename, char *buf, off_t off, off_t len)
{
	struct stat sb;
	u_char buffer[BUFSIZ];
	SHA2_CTX ctx;
	int fd, save_errno;
	ssize_t nr;

	SHA256Init(&ctx);

	if ((fd = open(filename, O_RDONLY)) < 0)
		return (NULL);
	if (len == 0) {
		if (fstat(fd, &sb) == -1) {
			close(fd);
			return (NULL);
		}
		len = sb.st_size;
	}
	if (off > 0 && lseek(fd, off, SEEK_SET) < 0) {
		close(fd);
		return (NULL);
	}

	while ((nr = read(fd, buffer, MIN(sizeof(buffer), len))) > 0) {
		SHA256Update(&ctx, buffer, (size_t)nr);
		if (len > 0 && (len -= nr) == 0)
			break;
	}

	save_errno = errno;
	close(fd);
	errno = save_errno;
	return (nr < 0 ? NULL : SHA256End(&ctx, buf));
}

char *
SHA256File(const char *filename, char *buf)
{
	return (SHA256FileChunk(filename, buf, (off_t)0, (off_t)0));
}

char *
SHA256Data(const u_char *data, size_t len, char *buf)
{
	SHA2_CTX ctx;

	SHA256Init(&ctx);
	SHA256Update(&ctx, data, len);
	return (SHA256End(&ctx, buf));
}

char *
SHA512End(SHA2_CTX *ctx, char *buf)
{
	int i;
	u_int8_t digest[SHA512_DIGEST_LENGTH];
	static const char hex[] = "0123456789abcdef";

	if (buf == NULL && (buf = malloc(SHA512_DIGEST_STRING_LENGTH)) == NULL)
		return (NULL);

	SHA512Final(digest, ctx);
	for (i = 0; i < SHA512_DIGEST_LENGTH; i++) {
		buf[i + i] = hex[digest[i] >> 4];
		buf[i + i + 1] = hex[digest[i] & 0x0f];
	}
	buf[i + i] = '\0';
	memset(digest, 0, sizeof(digest));
	return (buf);
}

char *
SHA512FileChunk(const char *filename, char *buf, off_t off, off_t len)
{
	struct stat sb;
	u_char buffer[BUFSIZ];
	SHA2_CTX ctx;
	int fd, save_errno;
	ssize_t nr;

	SHA512Init(&ctx);

	if ((fd = open(filename, O_RDONLY)) < 0)
		return (NULL);
	if (len == 0) {
		if (fstat(fd, &sb) == -1) {
			close(fd);
			return (NULL);
		}
		len = sb.st_size;
	}
	if (off > 0 && lseek(fd, off, SEEK_SET) < 0) {
		close(fd);
		return (NULL);
	}

	while ((nr = read(fd, buffer, MIN(sizeof(buffer), len))) > 0) {
		SHA512Update(&ctx, buffer, (size_t)nr);
		if (len > 0 && (len -= nr) == 0)
			break;
	}

	save_errno = errno;
	close(fd);
	errno = save_errno;
	return (nr < 0 ? NULL : SHA512End(&ctx, buf));
}

char *
SHA512File(const char *filename, char *buf)
{
	return (SHA512FileChunk(filename, buf, (off_t)0, (off_t)0));
}

char *
SHA512Data(const u_char *data, size_t len, char *buf)
{
	SHA2_CTX ctx;

	SHA512Init(&ctx);
	SHA512Update(&ctx, data, len);
	return (SHA512End(&ctx, buf));
}
