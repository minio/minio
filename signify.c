/* $OpenBSD: src/usr.bin/signify/signify.c,v 1.91 2014/07/13 18:59:40 tedu Exp $ */
/*
 * Copyright (c) 2013 Ted Unangst <tedu@openbsd.org>
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
#include <sys/stat.h>

#include <netinet/in.h>
#include <resolv.h>

#include <stdint.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <err.h>
#include <unistd.h>
#include <readpassphrase.h>
#include <util.h>
#include <sha2.h>

#include "crypto_api.h"
#ifndef VERIFY_ONLY
#include <stdint.h>
#include <stddef.h>
#include <ohash.h>
#endif

#define SIGBYTES crypto_sign_ed25519_BYTES
#define SECRETBYTES crypto_sign_ed25519_SECRETKEYBYTES
#define PUBLICBYTES crypto_sign_ed25519_PUBLICKEYBYTES

#define PKALG "Ed"
#define KDFALG "BK"
#define FPLEN 8

#define COMMENTHDR "untrusted comment: "
#define COMMENTHDRLEN 19
#define COMMENTMAXLEN 1024
#define VERIFYWITH "verify with "

struct enckey {
	uint8_t pkalg[2];
	uint8_t kdfalg[2];
	uint32_t kdfrounds;
	uint8_t salt[16];
	uint8_t checksum[8];
	uint8_t fingerprint[FPLEN];
	uint8_t seckey[SECRETBYTES];
};

struct pubkey {
	uint8_t pkalg[2];
	uint8_t fingerprint[FPLEN];
	uint8_t pubkey[PUBLICBYTES];
};

struct sig {
	uint8_t pkalg[2];
	uint8_t fingerprint[FPLEN];
	uint8_t sig[SIGBYTES];
};

extern char *__progname;

static void
usage(const char *error)
{
	if (error)
		fprintf(stderr, "%s\n", error);
	fprintf(stderr, "usage:"
#ifndef VERIFYONLY
	    "\t%1$s -C [-q] -p pubkey -x sigfile [file ...]\n"
	    "\t%1$s -G [-n] [-c comment] -p pubkey -s seckey\n"
	    "\t%1$s -I [-p pubkey] [-s seckey] [-x sigfile]\n"
	    "\t%1$s -S [-e] [-x sigfile] -s seckey -m message\n"
#endif
	    "\t%1$s -V [-eq] [-x sigfile] -p pubkey -m message\n",
	    __progname);
	exit(1);
}

static int
xopen(const char *fname, int oflags, mode_t mode)
{
	struct stat sb;
	int fd;

	if (strcmp(fname, "-") == 0) {
		if ((oflags & O_WRONLY))
			fd = dup(STDOUT_FILENO);
		else
			fd = dup(STDIN_FILENO);
		if (fd == -1)
			err(1, "dup failed");
	} else {
		fd = open(fname, oflags, mode);
		if (fd == -1)
			err(1, "can't open %s for %s", fname,
			    (oflags & O_WRONLY) ? "writing" : "reading");
	}
	if (fstat(fd, &sb) == -1 || S_ISDIR(sb.st_mode))
		errx(1, "not a valid file: %s", fname);
	return fd;
}

static void *
xmalloc(size_t len)
{
	void *p;

	if (!(p = malloc(len)))
		err(1, "malloc %zu", len);
	return p;
}

static size_t
parseb64file(const char *filename, char *b64, void *buf, size_t buflen,
    char *comment)
{
	char *commentend, *b64end;

	commentend = strchr(b64, '\n');
	if (!commentend || commentend - b64 <= COMMENTHDRLEN ||
	    memcmp(b64, COMMENTHDR, COMMENTHDRLEN) != 0)
		errx(1, "invalid comment in %s; must start with '%s'",
		    filename, COMMENTHDR);
	*commentend = '\0';
	if (comment) {
		if (strlcpy(comment, b64 + COMMENTHDRLEN,
		    COMMENTMAXLEN) >= COMMENTMAXLEN)
			errx(1, "comment too long");
	}
	if (!(b64end = strchr(commentend + 1, '\n')))
		errx(1, "missing new line after base64 in %s", filename);
	*b64end = '\0';
	if (b64_pton(commentend + 1, buf, buflen) != buflen)
		errx(1, "invalid base64 encoding in %s", filename);
	if (memcmp(buf, PKALG, 2) != 0)
		errx(1, "unsupported file %s", filename);
	return b64end - b64 + 1;
}

static void
readb64file(const char *filename, void *buf, size_t buflen, char *comment)
{
	char b64[2048];
	int rv, fd;

	fd = xopen(filename, O_RDONLY | O_NOFOLLOW, 0);
	if ((rv = read(fd, b64, sizeof(b64) - 1)) == -1)
		err(1, "read from %s", filename);
	b64[rv] = '\0';
	parseb64file(filename, b64, buf, buflen, comment);
	explicit_bzero(b64, sizeof(b64));
	close(fd);
}

static uint8_t *
readmsg(const char *filename, unsigned long long *msglenp)
{
	unsigned long long msglen = 0;
	uint8_t *msg = NULL;
	struct stat sb;
	ssize_t x, space;
	int fd;
	const unsigned long long maxmsgsize = 1UL << 30;

	fd = xopen(filename, O_RDONLY | O_NOFOLLOW, 0);
	if (fstat(fd, &sb) == 0 && S_ISREG(sb.st_mode)) {
		if (sb.st_size > maxmsgsize)
			errx(1, "msg too large in %s", filename);
		space = sb.st_size + 1;
	} else {
		space = 64 * 1024;
	}

	msg = xmalloc(space + 1);
	while (1) {
		if (space == 0) {
			if (msglen * 2 > maxmsgsize)
				errx(1, "msg too large in %s", filename);
			space = msglen;
			if (!(msg = realloc(msg, msglen + space + 1)))
				errx(1, "realloc");
		}
		if ((x = read(fd, msg + msglen, space)) == -1)
			err(1, "read from %s", filename);
		if (x == 0)
			break;
		space -= x;
		msglen += x;
	}

	msg[msglen] = '\0';
	close(fd);

	*msglenp = msglen;
	return msg;
}

static void
writeall(int fd, const void *buf, size_t buflen, const char *filename)
{
	ssize_t x;

	while (buflen != 0) {
		if ((x = write(fd, buf, buflen)) == -1)
			err(1, "write to %s", filename);
		buflen -= x;
		buf = (char *)buf + x;
	}
}

#ifndef VERIFYONLY
static void
writeb64file(const char *filename, const char *comment, const void *buf,
    size_t buflen, const void *msg, size_t msglen, int oflags, mode_t mode)
{
	char header[1024];
	char b64[1024];
	int fd, rv, nr;

	fd = xopen(filename, O_CREAT|oflags|O_NOFOLLOW|O_WRONLY, mode);
	if ((nr = snprintf(header, sizeof(header), "%s%s\n",
	    COMMENTHDR, comment)) == -1 || nr >= sizeof(header))
		errx(1, "comment too long");
	writeall(fd, header, strlen(header), filename);
	if ((rv = b64_ntop(buf, buflen, b64, sizeof(b64)-1)) == -1)
		errx(1, "base64 encode failed");
	b64[rv++] = '\n';
	writeall(fd, b64, rv, filename);
	explicit_bzero(b64, sizeof(b64));
	if (msg)
		writeall(fd, msg, msglen, filename);
	close(fd);
}

static void
kdf(uint8_t *salt, size_t saltlen, int rounds, int allowstdin, int confirm,
    uint8_t *key, size_t keylen)
{
	char pass[1024];
	int rppflags = RPP_ECHO_OFF;

	if (rounds == 0) {
		memset(key, 0, keylen);
		return;
	}

	if (allowstdin && !isatty(STDIN_FILENO))
		rppflags |= RPP_STDIN;
	if (!readpassphrase("passphrase: ", pass, sizeof(pass), rppflags))
		errx(1, "unable to read passphrase");
	if (strlen(pass) == 0)
		errx(1, "please provide a password");
	if (confirm && !(rppflags & RPP_STDIN)) {
		char pass2[1024];
		if (!readpassphrase("confirm passphrase: ", pass2,
		    sizeof(pass2), rppflags))
			errx(1, "unable to read passphrase");
		if (strcmp(pass, pass2) != 0)
			errx(1, "passwords don't match");
		explicit_bzero(pass2, sizeof(pass2));
	}
	if (bcrypt_pbkdf(pass, strlen(pass), salt, saltlen, key,
	    keylen, rounds) == -1)
		errx(1, "bcrypt pbkdf");
	explicit_bzero(pass, sizeof(pass));
}

static void
signmsg(uint8_t *seckey, uint8_t *msg, unsigned long long msglen,
    uint8_t *sig)
{
	unsigned long long siglen;
	uint8_t *sigbuf;

	sigbuf = xmalloc(msglen + SIGBYTES);
	crypto_sign_ed25519(sigbuf, &siglen, msg, msglen, seckey);
	memcpy(sig, sigbuf, SIGBYTES);
	free(sigbuf);
}

static void
generate(const char *pubkeyfile, const char *seckeyfile, int rounds,
    const char *comment)
{
	uint8_t digest[SHA512_DIGEST_LENGTH];
	struct pubkey pubkey;
	struct enckey enckey;
	uint8_t xorkey[sizeof(enckey.seckey)];
	uint8_t fingerprint[FPLEN];
	char commentbuf[COMMENTMAXLEN];
	SHA2_CTX ctx;
	int i, nr;

	crypto_sign_ed25519_keypair(pubkey.pubkey, enckey.seckey);
	arc4random_buf(fingerprint, sizeof(fingerprint));

	SHA512Init(&ctx);
	SHA512Update(&ctx, enckey.seckey, sizeof(enckey.seckey));
	SHA512Final(digest, &ctx);

	memcpy(enckey.pkalg, PKALG, 2);
	memcpy(enckey.kdfalg, KDFALG, 2);
	enckey.kdfrounds = htonl(rounds);
	memcpy(enckey.fingerprint, fingerprint, FPLEN);
	arc4random_buf(enckey.salt, sizeof(enckey.salt));
	kdf(enckey.salt, sizeof(enckey.salt), rounds, 1, 1, xorkey, sizeof(xorkey));
	memcpy(enckey.checksum, digest, sizeof(enckey.checksum));
	for (i = 0; i < sizeof(enckey.seckey); i++)
		enckey.seckey[i] ^= xorkey[i];
	explicit_bzero(digest, sizeof(digest));
	explicit_bzero(xorkey, sizeof(xorkey));

	if ((nr = snprintf(commentbuf, sizeof(commentbuf), "%s secret key",
	    comment)) == -1 || nr >= sizeof(commentbuf))
		errx(1, "comment too long");
	writeb64file(seckeyfile, commentbuf, &enckey,
	    sizeof(enckey), NULL, 0, O_EXCL, 0600);
	explicit_bzero(&enckey, sizeof(enckey));

	memcpy(pubkey.pkalg, PKALG, 2);
	memcpy(pubkey.fingerprint, fingerprint, FPLEN);
	if ((nr = snprintf(commentbuf, sizeof(commentbuf), "%s public key",
	    comment)) == -1 || nr >= sizeof(commentbuf))
		errx(1, "comment too long");
	writeb64file(pubkeyfile, commentbuf, &pubkey,
	    sizeof(pubkey), NULL, 0, O_EXCL, 0666);
}

static void
sign(const char *seckeyfile, const char *msgfile, const char *sigfile,
    int embedded)
{
	struct sig sig;
	uint8_t digest[SHA512_DIGEST_LENGTH];
	struct enckey enckey;
	uint8_t xorkey[sizeof(enckey.seckey)];
	uint8_t *msg;
	char comment[COMMENTMAXLEN], sigcomment[COMMENTMAXLEN];
	char *secname;
	unsigned long long msglen;
	int i, rounds, nr;
	SHA2_CTX ctx;

	readb64file(seckeyfile, &enckey, sizeof(enckey), comment);

	if (memcmp(enckey.kdfalg, KDFALG, 2) != 0)
		errx(1, "unsupported KDF");
	rounds = ntohl(enckey.kdfrounds);
	kdf(enckey.salt, sizeof(enckey.salt), rounds, strcmp(msgfile, "-") != 0,
	    0, xorkey, sizeof(xorkey));
	for (i = 0; i < sizeof(enckey.seckey); i++)
		enckey.seckey[i] ^= xorkey[i];
	explicit_bzero(xorkey, sizeof(xorkey));
	SHA512Init(&ctx);
	SHA512Update(&ctx, enckey.seckey, sizeof(enckey.seckey));
	SHA512Final(digest, &ctx);
	if (memcmp(enckey.checksum, digest, sizeof(enckey.checksum)) != 0)
	    errx(1, "incorrect passphrase");
	explicit_bzero(digest, sizeof(digest));

	msg = readmsg(msgfile, &msglen);

	signmsg(enckey.seckey, msg, msglen, sig.sig);
	memcpy(sig.fingerprint, enckey.fingerprint, FPLEN);
	explicit_bzero(&enckey, sizeof(enckey));

	memcpy(sig.pkalg, PKALG, 2);
	secname = strstr(seckeyfile, ".sec");
	if (secname && strlen(secname) == 4) {
		if ((nr = snprintf(sigcomment, sizeof(sigcomment), VERIFYWITH "%.*s.pub",
		    (int)strlen(seckeyfile) - 4, seckeyfile)) == -1 || nr >= sizeof(sigcomment))
			errx(1, "comment too long");
	} else {
		if ((nr = snprintf(sigcomment, sizeof(sigcomment), "signature from %s",
		    comment)) == -1 || nr >= sizeof(sigcomment))
			errx(1, "comment too long");
	}
	if (embedded)
		writeb64file(sigfile, sigcomment, &sig, sizeof(sig), msg,
		    msglen, O_TRUNC, 0666);
	else
		writeb64file(sigfile, sigcomment, &sig, sizeof(sig), NULL,
		    0, O_TRUNC, 0666);

	free(msg);
}

static void
inspect(const char *seckeyfile, const char *pubkeyfile, const char *sigfile)
{
	struct sig sig;
	struct enckey enckey;
	struct pubkey pubkey;
	char fp[(FPLEN + 2) / 3 * 4 + 1];

	if (seckeyfile) {
		readb64file(seckeyfile, &enckey, sizeof(enckey), NULL);
		b64_ntop(enckey.fingerprint, FPLEN, fp, sizeof(fp));
		printf("sec fp: %s\n", fp);
	}
	if (pubkeyfile) {
		readb64file(pubkeyfile, &pubkey, sizeof(pubkey), NULL);
		b64_ntop(pubkey.fingerprint, FPLEN, fp, sizeof(fp));
		printf("pub fp: %s\n", fp);
	}
	if (sigfile) {
		readb64file(sigfile, &sig, sizeof(sig), NULL);
		b64_ntop(sig.fingerprint, FPLEN, fp, sizeof(fp));
		printf("sig fp: %s\n", fp);
	}
}
#endif

static void
verifymsg(struct pubkey *pubkey, uint8_t *msg, unsigned long long msglen,
    struct sig *sig, int quiet)
{
	uint8_t *sigbuf, *dummybuf;
	unsigned long long siglen, dummylen;

	if (memcmp(pubkey->fingerprint, sig->fingerprint, FPLEN) != 0)
		errx(1, "verification failed: checked against wrong key");

	siglen = SIGBYTES + msglen;
	sigbuf = xmalloc(siglen);
	dummybuf = xmalloc(siglen);
	memcpy(sigbuf, sig->sig, SIGBYTES);
	memcpy(sigbuf + SIGBYTES, msg, msglen);
	if (crypto_sign_ed25519_open(dummybuf, &dummylen, sigbuf, siglen,
	    pubkey->pubkey) == -1)
		errx(1, "signature verification failed");
	if (!quiet)
		printf("Signature Verified\n");
	free(sigbuf);
	free(dummybuf);
}

static void
readpubkey(const char *pubkeyfile, struct pubkey *pubkey,
    const char *sigcomment)
{
	const char *safepath = "/etc/signify/";

	if (!pubkeyfile) {
		pubkeyfile = strstr(sigcomment, VERIFYWITH);
		if (pubkeyfile) {
			pubkeyfile += strlen(VERIFYWITH);
			if (strncmp(pubkeyfile, safepath, strlen(safepath)) != 0 ||
			    strstr(pubkeyfile, "/../") != NULL)
				errx(1, "untrusted path %s", pubkeyfile);
		} else
			usage("must specify pubkey");
	}
	readb64file(pubkeyfile, pubkey, sizeof(*pubkey), NULL);
}

static void
verifysimple(const char *pubkeyfile, const char *msgfile, const char *sigfile,
    int quiet)
{
	char sigcomment[COMMENTMAXLEN];
	struct sig sig;
	struct pubkey pubkey;
	unsigned long long msglen;
	uint8_t *msg;

	msg = readmsg(msgfile, &msglen);

	readb64file(sigfile, &sig, sizeof(sig), sigcomment);
	readpubkey(pubkeyfile, &pubkey, sigcomment);

	verifymsg(&pubkey, msg, msglen, &sig, quiet);

	free(msg);
}

static uint8_t *
verifyembedded(const char *pubkeyfile, const char *sigfile,
    int quiet, unsigned long long *msglenp)
{
	char sigcomment[COMMENTMAXLEN];
	struct sig sig;
	struct pubkey pubkey;
	unsigned long long msglen, siglen;
	uint8_t *msg;

	msg = readmsg(sigfile, &msglen);

	siglen = parseb64file(sigfile, msg, &sig, sizeof(sig), sigcomment);
	readpubkey(pubkeyfile, &pubkey, sigcomment);

	msglen -= siglen;
	memmove(msg, msg + siglen, msglen);
	msg[msglen] = 0;

	verifymsg(&pubkey, msg, msglen, &sig, quiet);

	*msglenp = msglen;
	return msg;
}

static void
verify(const char *pubkeyfile, const char *msgfile, const char *sigfile,
    int embedded, int quiet)
{
	unsigned long long msglen;
	uint8_t *msg;
	int fd;

	if (embedded) {
		msg = verifyembedded(pubkeyfile, sigfile, quiet, &msglen);
		fd = xopen(msgfile, O_CREAT|O_TRUNC|O_NOFOLLOW|O_WRONLY, 0666);
		writeall(fd, msg, msglen, msgfile);
		free(msg);
		close(fd);
	} else {
		verifysimple(pubkeyfile, msgfile, sigfile, quiet);
	}
}

#ifndef VERIFYONLY
#define HASHBUFSIZE 224
struct checksum {
	char file[1024];
	char hash[HASHBUFSIZE];
	char algo[32];
};

static void * 
ecalloc(size_t s1, size_t s2, void *data)
{
	void *p;

	if (!(p = calloc(s1, s2)))
		err(1, "calloc");
	return p;
}

static void
efree(void *p, void *data)
{
	free(p);
}

static void
recodehash(char *hash, size_t len)
{
	uint8_t data[HASHBUFSIZE / 2];
	int i, rv;

	if (strlen(hash) == len)
		return;
	if ((rv = b64_pton(hash, data, sizeof(data))) == -1)
		errx(1, "invalid base64 encoding");
	for (i = 0; i < rv; i++)
		snprintf(hash + i * 2, HASHBUFSIZE - i * 2, "%2.2x", data[i]);
}

static int
verifychecksum(struct checksum *c, int quiet)
{
	char buf[HASHBUFSIZE];

	if (strcmp(c->algo, "SHA256") == 0) {
		recodehash(c->hash, SHA256_DIGEST_STRING_LENGTH-1);
		if (!SHA256File(c->file, buf))
			return 0;
	} else if (strcmp(c->algo, "SHA512") == 0) {
		recodehash(c->hash, SHA512_DIGEST_STRING_LENGTH-1);
		if (!SHA512File(c->file, buf))
			return 0;
	} else {
		errx(1, "can't handle algorithm %s", c->algo);
	}
	if (strcmp(c->hash, buf) != 0) {
		return 0;
	}
	if (!quiet)
		printf("%s: OK\n", c->file);
	return 1;
}

static void
verifychecksums(char *msg, int argc, char **argv, int quiet)
{
	struct ohash_info info = { 0, NULL, ecalloc, efree, NULL };
	struct ohash myh;
	struct checksum c;
	char *e, *line, *endline;
	int hasfailed = 0;
	int i, rv;
	unsigned int slot;

	ohash_init(&myh, 6, &info);
	if (argc) {
		for (i = 0; i < argc; i++) {
			slot = ohash_qlookup(&myh, argv[i]);
			e = ohash_find(&myh, slot);
			if (e == NULL)
				ohash_insert(&myh, slot, argv[i]);
		}
	}

	line = msg;
	while (line && *line) {
		if ((endline = strchr(line, '\n')))
			*endline++ = '\0';
		rv = sscanf(line, "%31s (%1023s = %223s",
		    c.algo, c.file, c.hash);
		if (rv != 3 || c.file[0] == 0 || c.file[strlen(c.file)-1] != ')')
			errx(1, "unable to parse checksum line %s", line);
		c.file[strlen(c.file) - 1] = '\0';
		line = endline;
		if (argc) {
			slot = ohash_qlookup(&myh, c.file);
			e = ohash_find(&myh, slot);
			if (e != NULL) {
				if (verifychecksum(&c, quiet) != 0)
					ohash_remove(&myh, slot);
			}
		} else {
			if (verifychecksum(&c, quiet) == 0) {
				slot = ohash_qlookup(&myh, c.file);
				e = ohash_find(&myh, slot);
				if (e == NULL) {
					if (!(e = strdup(c.file)))
						err(1, "strdup");
					ohash_insert(&myh, slot, e);
				}
			}
		}
	}

	for (e = ohash_first(&myh, &slot); e != NULL; e = ohash_next(&myh, &slot)) {
		fprintf(stderr, "%s: FAIL\n", e);
		hasfailed = 1;
		if (argc == 0)
			free(e);
	}
	ohash_delete(&myh);
	if (hasfailed)
		exit(1);
}

static void
check(const char *pubkeyfile, const char *sigfile, int quiet, int argc,
    char **argv)
{
	unsigned long long msglen;
	uint8_t *msg;

	msg = verifyembedded(pubkeyfile, sigfile, quiet, &msglen);
	verifychecksums((char *)msg, argc, argv, quiet);

	free(msg);
}
#endif

int
main(int argc, char **argv)
{
	const char *pubkeyfile = NULL, *seckeyfile = NULL, *msgfile = NULL,
	    *sigfile = NULL;
	char sigfilebuf[1024];
	const char *comment = "signify";
	int ch, rounds;
	int embedded = 0;
	int quiet = 0;
	enum {
		NONE,
		CHECK,
		GENERATE,
		INSPECT,
		SIGN,
		VERIFY
	} verb = NONE;


	rounds = 42;

	while ((ch = getopt(argc, argv, "CGISVc:em:np:qs:x:")) != -1) {
		switch (ch) {
#ifndef VERIFYONLY
		case 'C':
			if (verb)
				usage(NULL);
			verb = CHECK;
			break;
		case 'G':
			if (verb)
				usage(NULL);
			verb = GENERATE;
			break;
		case 'I':
			if (verb)
				usage(NULL);
			verb = INSPECT;
			break;
		case 'S':
			if (verb)
				usage(NULL);
			verb = SIGN;
			break;
#endif
		case 'V':
			if (verb)
				usage(NULL);
			verb = VERIFY;
			break;
		case 'c':
			comment = optarg;
			break;
		case 'e':
			embedded = 1;
			break;
		case 'm':
			msgfile = optarg;
			break;
		case 'n':
			rounds = 0;
			break;
		case 'p':
			pubkeyfile = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 's':
			seckeyfile = optarg;
			break;
		case 'x':
			sigfile = optarg;
			break;
		default:
			usage(NULL);
			break;
		}
	}
	argc -= optind;
	argv += optind;

#ifndef VERIFYONLY
	if (verb == CHECK) {
		if (!sigfile)
			usage("must specify sigfile");
		check(pubkeyfile, sigfile, quiet, argc, argv);
		return 0;
	}
#endif

	if (argc != 0)
		usage(NULL);

	if (!sigfile && msgfile) {
		int nr;
		if (strcmp(msgfile, "-") == 0)
			usage("must specify sigfile with - message");
		if ((nr = snprintf(sigfilebuf, sizeof(sigfilebuf), "%s.sig",
		    msgfile)) == -1 || nr >= sizeof(sigfilebuf))
			errx(1, "path too long");
		sigfile = sigfilebuf;
	}

	switch (verb) {
#ifndef VERIFYONLY
	case GENERATE:
		if (!pubkeyfile || !seckeyfile)
			usage("must specify pubkey and seckey");
		generate(pubkeyfile, seckeyfile, rounds, comment);
		break;
	case INSPECT:
		inspect(seckeyfile, pubkeyfile, sigfile);
		break;
	case SIGN:
		if (!msgfile || !seckeyfile)
			usage("must specify message and seckey");
		sign(seckeyfile, msgfile, sigfile, embedded);
		break;
#endif
	case VERIFY:
		if (!msgfile)
			usage("must specify message");
		verify(pubkeyfile, msgfile, sigfile, embedded, quiet);
		break;
	default:
		usage(NULL);
		break;
	}

	return 0;
}
