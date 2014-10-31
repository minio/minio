# OpenBSD signify - portable version
# mancha <mancha1@zoho.com>

prefix=/usr
bindir=${prefix}/bin
mandir=${prefix}/man

DESTDIR=

CC=gcc
AR=ar
LIB=static
INSTALL=/usr/bin/install -c

CFLAGS=-O2 -D_FORTIFY_SOURCE=2 -ftrapv -fPIE -fstack-protector-all \
	-Wno-attributes -Wno-unused-result -Ibsd-compat -I.

TARGET = signify

TARGET_LIB = libsignify_$(LIB).a

SIGNIFY_OBJS = signify.o fe25519.o sc25519.o smult_curve25519_ref.o \
	mod_ed25519.o mod_ge25519.o crypto_api.o base64.o bcrypt_pbkdf.o \
	explicit_bzero.o arc4random.o timingsafe_bcmp.o sha2.o blowfish.o \
	readpassphrase.o strlcpy.o helpers.o ohash.o

all:	$(TARGET) $(TARGET_LIB)

$(TARGET): $(SIGNIFY_OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SIGNIFY_OBJS)

$(TARGET_LIB): $(SIGNIFY_OBJS)
	$(AR) crs $(TARGET_LIB) $(SIGNIFY_OBJS)

clean:
	@rm -f *.o signify $(TARGET_LIB)

install:
	$(INSTALL) -c -D -m 0755 signify $(DESTDIR)/$(bindir)/signify
	$(INSTALL) -c -D -m 644 signify.1 $(DESTDIR)/$(mandir)/man1/signify.1

uninstall:
	@rm -f $(DESTDIR)/$(bindir)/signify
	@rm -f $(DESTDIR)/$(mandir)/man1/signify.1
