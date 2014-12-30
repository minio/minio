# OpenBSD signify - portable version
# mancha <mancha1@zoho.com>

prefix=/usr
bindir=${prefix}/bin
mandir=${prefix}/man

DESTDIR=

CC=gcc
AR=ar
INSTALL=/usr/bin/install -c

CFLAGS=-O2 -D_FORTIFY_SOURCE=2 -fPIC -ftrapv -fPIE -fstack-protector-all \
	-Wno-attributes -Wno-unused-result -Ibsd-compat -I.

TARGET = signify

SIGNIFY_STATIC_LIB = libsignify_static.a
SIGNIFY_SHARED_LIB = libsignify_shared.so

SIGNIFY_OBJS = fe25519.o sc25519.o smult_curve25519_ref.o \
	mod_ed25519.o mod_ge25519.o crypto_api.o base64.o bcrypt_pbkdf.o \
	explicit_bzero.o arc4random.o timingsafe_bcmp.o sha2.o blowfish.o \
	readpassphrase.o strlcpy.o helpers.o ohash.o

all:	$(TARGET) $(TARGET_LIB)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

$(SIGNIFY_STATIC_LIB): $(SIGNIFY_OBJS)
	$(AR) crs $@ $(SIGNIFY_OBJS)

$(SIGNIFY_SHARED_LIB): $(SIGNIFY_STATIC_LIB)
	$(CC) -shared -o $@ $(SIGNIFY_STATIC_LIB)

$(TARGET): $(SIGNIFY_STATIC_LIB) $(SIGNIFY_STATIC_LIB)
	$(CC) $(CFLAGS) -o $(TARGET) signify.c $(SIGNIFY_STATIC_LIB)

clean:
	rm -f *.o *.so *.a signify

install:
	$(INSTALL) -c -D -m 0755 signify $(DESTDIR)/$(bindir)/signify
	$(INSTALL) -c -D -m 644 signify.1 $(DESTDIR)/$(mandir)/man1/signify.1

uninstall:
	@rm -f $(DESTDIR)/$(bindir)/signify
	@rm -f $(DESTDIR)/$(mandir)/man1/signify.1
