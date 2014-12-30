all: build
.PHONY: all

libsignify/libsignify_static.a:
	@$(MAKE) -C libsignify libsignify_static.a

build: libsignify/libsignify_static.a
	@godep go build
